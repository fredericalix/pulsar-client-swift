// Copyright 2024 Felix Ruppert
//
// Licensed under the Apache License, Version 2.0 (the License );
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Logging
import NIO

/// The core Pulsar Client used to connect to the server. All control of the library, like consumers and producers and its settings are controlled via the Client.
public final actor PulsarClient {
	let logger = Logger(label: "PulsarClient")
	private let group: EventLoopGroup
	var connectionPool: [String: Channel] = [:]
	var initialURL: String
	private var isReconnecting: Set<String> = []
	var isFirstConnect: Bool = true

	/// Creates a new Pulsar Client and tries to connect it.
	/// - Parameters:
	///   - host: The host to connect to. Doesn't need the `pulsar://` prefix.
	///   - port: The port to connect to. Normally `6650`.
	///   - group: If you want to pass your own EventLoopGroup, you can do it here. Otherwise the client will create it's own.
	public init(host: String, port: Int, group: EventLoopGroup? = nil) async {
		#if DEBUG
			self.group = group ?? MultiThreadedEventLoopGroup(numberOfThreads: 1)
		#else
			self.group = group ?? MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
		#endif
		initialURL = host
		await connect(host: host, port: port)
	}

	func connect(host: String, port: Int) async {
		// If already connected to this host, do nothing
		if connectionPool[host] != nil {
			return
		}
		let bootstrap = ClientBootstrap(group: group)
			.channelInitializer { channel in
				channel.pipeline.addHandlers([
					ByteToMessageHandler(PulsarFrameDecoder()),
					MessageToByteHandler(PulsarFrameEncoder()),
					PulsarClientHandler(eventLoop: self.group.next(), client: self)
				])
			}

		do {
			let channel = try await bootstrap.connect(host: host, port: port).get()
			let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

			// Store channel if successful
			connectionPool[host] = channel

			// Wait for the handler’s connectionEstablished
			try await handler.connectionEstablished.futureResult.get()
			logger.info("Successfully connected to \(host):\(port)c")
		} catch {
			connectionPool[host] = nil
			logger.error("Failed to connect to \(host):\(port) - \(error)")
			if isFirstConnect {
				isFirstConnect = false
				await handleChannelInactive(ipAddress: initialURL, handler: PulsarClientHandler(eventLoop: group.next(), client: self))
			}
		}
	}

	/// Closes all channels and fails all consumer streams. Then throws `clientClosed`.
	public func close() async throws {
		logger.warning("Closing client")

		// Fail all consumer streams (so their AsyncThrowingStream loops exit).
		for channel in connectionPool.values {
			if let handler = try? await channel.pipeline.handler(type: PulsarClientHandler.self).get() {
				for (_, cache) in handler.consumers {
					cache.consumer.fail(error: PulsarClientError.clientClosed)
				}
			}
		}

		// Then close the channels
		for (host, channel) in connectionPool {
			do {
				try await channel.close().get()
			} catch {
				logger.error("Failed to close channel for host \(host): \(error)")
			}
		}
		connectionPool.removeAll()

		// Finally, inform the caller we are closed
		throw PulsarClientError.clientClosed
	}

	func checkPersistentTopic(topic: String) -> Bool {
		if topic.starts(with: "persistent://") {
			return true
		} else if topic.starts(with: "non-persistent://") {
			return false
		}
		return true
	}

	func getConnection(connectionString: String) -> (String, Int) {
		var str = connectionString
		str = str.replacingOccurrences(of: "pulsar://", with: "")
		let parts = str.split(separator: ":")
		return (String(parts[0]), Int(parts[1]) ?? 6650)
	}
}

// MARK: - PulsarClient consumer logic

public extension PulsarClient {
	/// Creates a new Pulsar consumer.
	/// - Parameters:
	///   - topic: The topic to consume.
	///   - subscription: The name of the subscription.
	///   - subscriptionType: The type of the subscription.
	///   - subscriptionMode: Optional: The mode of the subscription.
	///   - consumerID: Optional: If you want to define your own consumerID.
	///   - connectionString: Not recommended:  Define another URL where the topic will be found. Can cause issues with connection.
	///   - existingConsumer: Not recommended: Reuse an existing consumer.
	/// - Returns: The newly created consumer.
	///
	/// - Warning: `connectionString` and `existingConsumer` are there for internal implementation and shouldn't be used by the library user.
	func consumer(
		topic: String,
		subscription: String,
		subscriptionType: SubscriptionType,
		subscriptionMode: SubscriptionMode = .durable,
		consumerID: UInt64? = nil,
		connectionString: String? = nil,
		existingConsumer: PulsarConsumer? = nil) async throws -> PulsarConsumer {
		var connectionString = connectionString ?? initialURL
		var topicFound = false

		// Possibly do multiple lookups if the broker says "redirect"
		while !topicFound {
			// Must have a channel for the current connectionString
			guard let channel = connectionPool[connectionString] else {
				throw PulsarClientError.topicLookupFailed
			}
			let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

			// Request a topic-lookup from the handler
			let lookup = try await handler.topicLookup(topic: topic, authorative: false)
			if let redirectHost = lookup.0, !redirectHost.isEmpty {
				// The broker told us to connect somewhere else
				connectionString = getConnection(connectionString: redirectHost).0
				await connect(host: connectionString, port: 6650)
			} else {
				// Means topicFound or broker said "use the existing connection"
				topicFound = true
			}
		}

		// By now, we have a connection for `connectionString`
		guard let channel = connectionPool[connectionString] else {
			throw PulsarClientError.topicLookupFailed
		}
		let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

		let consumer: PulsarConsumer = if let existingConsumer {
			// We DO NOT want a new consumer. We reattach it by performing the consumer flow again.
			try await handler.subscribe(
				topic: topic,
				subscription: subscription,
				consumerID: consumerID!,
				existingConsumer: existingConsumer,
				subscriptionType: subscriptionType,
				subscriptionMode: subscriptionMode
			)
		} else {
			try await handler.subscribe(
				topic: topic,
				subscription: subscription,
				subscriptionType: subscriptionType,
				subscriptionMode: subscriptionMode
			)
		}
		return consumer
	}
}

// MARK: - Reconnection logic

extension PulsarClient {
	func handleChannelInactive(ipAddress: String, handler: PulsarClientHandler) async {
		let remoteAddress = ipAddress
		connectionPool.removeValue(forKey: remoteAddress)

		if isReconnecting.contains(ipAddress) {
			logger.info("Already reconnecting to \(ipAddress). Skipping.")
			return
		}

		let oldConsumers = handler.consumers
		logger.warning("Channel inactive for \(ipAddress). Initiating reconnection...")
		isReconnecting.insert(ipAddress)

		let backoff = BackoffStrategy.exponential(
			initialDelay: .seconds(1),
			factor: 2.0,
			maxDelay: .seconds(30)
		)

		var attempt = 0
		let port = 6650
		while true {
			attempt += 1
			do {
				logger.info("Reconnection attempt #\(attempt) to \(remoteAddress):\(port)")

				await connect(host: remoteAddress, port: port)

				// Reattach consumers after reconnecting
				try await reattachConsumers(oldConsumers: oldConsumers, host: remoteAddress)
				isReconnecting.remove(ipAddress)
				logger.info("Reconnected to \(remoteAddress) after \(attempt) attempt(s).")
				break
			} catch {
				logger.error("Reconnection attempt #\(attempt) to \(remoteAddress) failed: \(error)")
				let delay = backoff.delay(forAttempt: attempt)
				logger.warning("Will retry in \(Double(delay.nanoseconds) / 1_000_000_000) second(s).")
				try? await Task.sleep(nanoseconds: UInt64(delay.nanoseconds))
			}
		}
	}

	private func reattachConsumers(
		oldConsumers: [UInt64: ConsumerCache],
		host: String
	) async throws {
		guard let _ = connectionPool[host] else {
			throw PulsarClientError.topicLookupFailed
		}
		logger.debug("Re-attaching \(oldConsumers.count) consumers...")
		for (_, consumerCache) in oldConsumers {
			let oldConsumer = consumerCache.consumer
			let topic = oldConsumer.topic
			let subscription = oldConsumer.subscriptionName
			let consumerID = oldConsumer.consumerID
			let subscriptionType = oldConsumer.subscriptionType
			let subscriptionMode = oldConsumer.subscriptionMode

			logger.info("Re-subscribing consumerID \(consumerCache.consumerID) for topic \(topic)")

			do {
				_ = try await consumer(
					topic: topic,
					subscription: subscription,
					subscriptionType: subscriptionType,
					subscriptionMode: subscriptionMode,
					consumerID: consumerID,
					connectionString: host,
					existingConsumer: oldConsumer
				)
			} catch {
				logger.error("Failed to re-subscribe consumer for topic \(topic): \(error)")
				throw PulsarClientError.consumerClosed
			}
		}
	}
}
