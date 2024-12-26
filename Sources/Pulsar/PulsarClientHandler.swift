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

import Foundation
import Logging
import NIO
import NIOCore
import NIOFoundationCompat

final class PulsarClientHandler: ChannelInboundHandler, @unchecked Sendable {
	let logger = Logger(label: "PulsarClientHandler")
	typealias InboundIn = PulsarMessage
	typealias OutboundOut = PulsarMessage

	let eventLoop: EventLoop
	let client: PulsarClient
	let connectionEstablished: EventLoopPromise<Void>

	init(eventLoop: EventLoop, client: PulsarClient) {
		let promise = eventLoop.makePromise(of: Void.self)
		connectionEstablished = promise
		self.eventLoop = eventLoop
		self.client = client
		connectionPromiseState = .inProgress(promise)
	}

	/// When - for some reason - the handler goes out of scope, fail the promise.
	/// This happens mostly when the TCP connection fails and the pipeline cleans up itself.
	deinit {
		connectionEstablished.fail(PulsarClientError.clientClosed)
	}

	/// Each consumer is tracked by an ID; storing the consumer object plus any relevant info
	var consumers: [UInt64: ConsumerCache] = [:]

	enum PromiseType: Hashable {
		case generic(String)
		case id(UInt64)
	}

	enum ConnectionState {
		case disconnected
		case inConnection
		case connected
	}

	private enum ConnectionPromiseState {
		case inProgress(EventLoopPromise<Void>)
		case completed
	}

	private var connectionPromiseState: ConnectionPromiseState

	let correlationMap = HandlerStateMap()

	// MARK: - Channel Lifecycle

	func channelActive(context: ChannelHandlerContext) {
		connect(context: context)
		correlationMap.context = context
	}

	func channelInactive(context: ChannelHandlerContext) {
		// inReconnection = true
		guard let ipAddress = context.channel.remoteAddress?.ipAddress else {
			context.fireChannelInactive()
			return
		}
		Task {
			await client.handleChannelInactive(ipAddress: ipAddress, handler: self)
		}

		// Continue normal pipeline behavior
		context.fireChannelInactive()
	}

	func channelRead(context: ChannelHandlerContext, data: NIOAny) {
		let message = unwrapInboundIn(data)

		do {
			switch message.command.type {
				case .connected:
					handleConnected(context: context, message: message.command.connected)
				case .ping:
					handlePing(context: context, message: message.command.ping)
				case .lookupResponse:
					handleLookupResponse(context: context, message: message.command.lookupTopicResponse)
				case .success:
					handleSuccess(context: context, message: message.command.success)
				case .message:
					handlePayloadMessage(context: context, message: message)
				case .closeConsumer:
					handleClosed(consumerID: message.command.closeConsumer.consumerID)
				case .error:
					// The server can return an Error command with a message inside
					let errorCmd = message.command.error
					try handleProtocolError(context: context, errorCmd)
				default:
					logger.debug("Unknown command \(message.command.type)")
					throw PulsarClientError.unsupportedMessageType
			}
		} catch {
			handleReadError(context: context, error: error)
		}
	}

	func handleReadError(context: ChannelHandlerContext, error: Error) {
		logger.error("Error during channelRead: \(error)")

		// Fail any big pending promise (like the connectionEstablished) if it’s not done
		switch connectionPromiseState {
			case let .inProgress(promise):
				promise.fail(error)
				connectionPromiseState = .completed
			case .completed:
				// Already completed; do nothing and log
				logger.debug("Connection promise was already completed; ignoring.")
		}
		logger.warning("Closing the channel, initiating reconnect")
		context.close(mode: .all, promise: nil)
		context.fireErrorCaught(error)
	}

	func makePromise(context: ChannelHandlerContext, type: PromiseType) -> EventLoopPromise<Void> {
		let promise: EventLoopPromise<Void> = context.eventLoop.makePromise()
		context.eventLoop.scheduleTask(in: .seconds(5)) { [weak self] in
			guard let self else {
				return
			}
			correlationMap.context!.eventLoop.scheduleTask(in: .seconds(5)) {
				// We only want to fail the promise if it's still there.
				if let promise = self.correlationMap.remove(promise: type) {
					self.logger.error("Request timed out.")
					self.logger.trace("Failing promise for \(type)")
					promise.fail(PulsarClientError.connectionTimeout)
					self.correlationMap.context!.close(mode: .all, promise: nil)
				} else {
					self.logger.trace("Promise for \(type) was already fullfilled.")
				}
				switch self.connectionPromiseState {
					case let .inProgress(eventLoopPromise):
						self.logger.error("Connection timed out.")
						eventLoopPromise.fail(PulsarClientError.connectionTimeout)
						self.correlationMap.context!.close(mode: .all, promise: nil)
					case .completed:
						break
				}
			}
		}
		return promise
	}

	// MARK: - Protocol Handlers

	func handleProtocolError(context: ChannelHandlerContext, _ errorCommand: Pulsar_Proto_CommandError) throws {
		let serverMsg = errorCommand.message
		logger.error("Server responded with error: \(serverMsg)")
		context.close(mode: .all, promise: nil)
		guard let ipAddress = context.channel.remoteAddress?.ipAddress else {
			context.fireChannelInactive()
			return
		}
		Task {
			await client.handleChannelInactive(ipAddress: ipAddress, handler: self)
		}

		// Continue normal pipeline behavior
		context.fireErrorCaught(PulsarClientError.unknownError)
	}

	func connect(context: ChannelHandlerContext) {
		logger.debug("Connecting...")
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .connect

		var connectCommand = Pulsar_Proto_CommandConnect()
		connectCommand.clientVersion = "Pulsar-Client-Swift-1.0.0"
		connectCommand.protocolVersion = 21
		baseCommand.connect = connectCommand

		let pulsarMessage = PulsarMessage(command: baseCommand)
		let promise = makePromise(context: context, type: .generic("connect"))
		correlationMap.add(promise: .generic("connect"), promiseValue: promise)
		correlationMap.beginConnection(promise: promise)

		context.writeAndFlush(wrapOutboundOut(pulsarMessage), promise: nil)
	}

	func handlePing(context: ChannelHandlerContext, message _: Pulsar_Proto_CommandPing) {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .pong

		let pongCommand = Pulsar_Proto_CommandPong()
		baseCommand.pong = pongCommand
		let pulsarMessage = PulsarMessage(command: baseCommand)
		context.writeAndFlush(wrapOutboundOut(pulsarMessage), promise: nil)
	}

	func handleConnected(context _: ChannelHandlerContext, message _: Pulsar_Proto_CommandConnected) {
		// Only succeed if we’re still in progress
		switch connectionPromiseState {
			case let .inProgress(promise):
				logger.info("Connected channel.")
				promise.succeed(())
				connectionPromiseState = .completed
				if let promise = correlationMap.remove(promise: .generic("connect")) {
					promise.succeed()
				}
			// isAlreadyReconnecting = false
			case .completed:
				// Already completed; ignore and log
				logger.trace("Connection promise was already completed.")
		}
	}

	func handleSuccess(context _: ChannelHandlerContext, message: Pulsar_Proto_CommandSuccess) {
		if let promise = correlationMap.remove(promise: .id(message.requestID)) {
			logger.debug("Success for requestID \(message.requestID)")
			promise.succeed()
		}
	}

	/// If the server returned a message payload, deliver it to the correct consumer.
	func handlePayloadMessage(context: ChannelHandlerContext, message: PulsarMessage) {
		let msgCmd = message.command.message
		guard let receivingConsumerCache = consumers[msgCmd.consumerID] else {
			logger.error("Received a message for unknown consumerID \(msgCmd.consumerID)")
			return
		}
		let receivingConsumer = receivingConsumerCache.consumer
		if let payload = message.payload {
			receivingConsumer.continuation.yield(Message(data: Data(buffer: payload)))
			receivingConsumerCache.messageCount += 1
		}
		if receivingConsumer.autoAcknowledge {
			acknowledge(context: context, message: message)
		}
		// Request more messages if we hit our threshold as per Pulsar Protocol
		if receivingConsumerCache.messageCount == 500 {
			flow(consumerID: receivingConsumerCache.consumerID)
		}
	}

	func acknowledge(context: ChannelHandlerContext, message: PulsarMessage) {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .ack
		var ackCmd = Pulsar_Proto_CommandAck()
		ackCmd.messageID = [message.command.message.messageID]
		ackCmd.consumerID = message.command.message.consumerID
		ackCmd.ackType = .individual
		baseCommand.ack = ackCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)
		context.writeAndFlush(wrapOutboundOut(pulsarMessage), promise: nil)
	}

	/// The broker told us the consumer is being closed. We can fail the stream and (optionally) try re-subscribing.
	func handleClosed(consumerID: UInt64) {
		guard let consumerCache = consumers[consumerID] else {
			logger.warning("Received closeConsumer for unknown consumerID \(consumerID)")
			return
		}
		let consumer = consumerCache.consumer
		logger.warning("Server closed consumerID \(consumerID) for topic \(consumer.topic)")

		// Optional: attempt a re-subscribe
		Task {
			do {
				logger.info("Attempting to re-subscribe consumer for \(consumer.topic)...")
				_ = try await client.consumer(topic: consumer.topic, subscription: consumer.subscriptionName, subscriptionType: .shared)
				logger.info("Successfully re-subscribed \(consumer.topic)")
			} catch {
				logger.error("Re-subscribe failed for \(consumer.topic): \(error)")
			}
		}
	}

	// MARK: - Closing Consumers

	func closeConsumer(consumerID: UInt64) async throws {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .closeConsumer
		var closeCmd = Pulsar_Proto_CommandCloseConsumer()
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		closeCmd.consumerID = consumerID
		closeCmd.requestID = requestID

		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		baseCommand.closeConsumer = closeCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)

		try await correlationMap.context!.eventLoop.submit {
			self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
		}.get()

		try await promise.futureResult.get()
	}

	// MARK: - Topic Lookup

	/// Looks up the topic. If the broker says “redirect,” we store that in `correlationMap` and succeed the promise.
	/// The caller (PulsarClient) will check `removeRedirectURL()` to see if we need to connect elsewhere.
	func topicLookup(topic: String, authorative: Bool) async throws -> (String?, Bool) {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .lookup
		var lookupCmd = Pulsar_Proto_CommandLookupTopic()
		lookupCmd.topic = topic
		lookupCmd.authoritative = authorative
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		lookupCmd.requestID = requestID
		baseCommand.lookupTopic = lookupCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)
		// Send the lookup command on the event loop
		try await correlationMap.context!.eventLoop.submit {
			self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
		}.get()

		// Create & store a promise so we know when the server responds
		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		// Wait for the server’s response or error
		try await promise.futureResult.get()

		// Did the server instruct us to redirect?
		let optionalRedirectResponse = correlationMap.removeRedirectURL()
		if let redirectResponse = optionalRedirectResponse, let redirectURL = redirectResponse.0 {
			return (redirectURL, redirectResponse.1)
		} else if let redirectResponse = optionalRedirectResponse {
			// Means server responded with “use the same connection”
			return ("", redirectResponse.1)
		}
		throw PulsarClientError.topicLookupFailed
	}

	func handleLookupResponse(context: ChannelHandlerContext, message: Pulsar_Proto_CommandLookupTopicResponse) {
		guard let promise = correlationMap.remove(promise: .id(message.requestID)) else {
			logger.error("Lookup response received for unknown request ID \(message.requestID)")
			handleReadError(context: context, error: PulsarClientError.topicLookupFailed)
			return
		}

		if message.response == .connect {
			// Means we can connect directly
			correlationMap.addRedirectURL(nil, isAuthorative: message.authoritative)
		} else if message.response == .redirect {
			// Means we must re-connect to the brokerServiceURL
			correlationMap.addRedirectURL(message.brokerServiceURL, isAuthorative: message.authoritative)
		} else if message.response == .failed {
			// Something bigger is broken; fail the entire promise and try to reconnect the channel
			guard let ipAddress = context.channel.remoteAddress?.ipAddress else {
				context.fireChannelInactive()
				return
			}
			Task {
				await client.handleChannelInactive(ipAddress: ipAddress, handler: self)
			}
			promise.fail(PulsarClientError.topicLookupFailed)
			// Continue normal pipeline behavior
			context.fireChannelInactive()

			return
		}

		promise.succeed()
	}

	// MARK: - Subscribing

	func subscribe(topic: String,
	               subscription: String,
	               consumerID: UInt64 = UInt64.random(in: 0 ..< UInt64.max),
	               existingConsumer: PulsarConsumer? = nil,
	               subscriptionType: SubscriptionType,
	               subscriptionMode: SubscriptionMode) async throws -> PulsarConsumer {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .subscribe
		var subscribeCmd = Pulsar_Proto_CommandSubscribe()
		subscribeCmd.topic = topic
		subscribeCmd.subscription = subscription
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		subscribeCmd.requestID = requestID
		subscribeCmd.subType = switch subscriptionType {
			case .exclusive:
				.exclusive
			case .failover:
				.failover
			case .keyShared:
				.keyShared
			case .shared:
				.shared
		}
		subscribeCmd.consumerID = consumerID

		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		baseCommand.subscribe = subscribeCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)

		// We add the consumer to the pool before connection, so in case the subscription attempt fails and we
		// need to reconnect, we already know the consumers we wanted.
		let consumer = existingConsumer ?? PulsarConsumer(
			handler: self,
			consumerID: consumerID,
			topic: topic,
			subscriptionName: subscription,
			subscriptionType: subscriptionType,
			subscriptionMode: subscriptionMode
		)
		consumers[consumerID] = ConsumerCache(consumerID: consumerID, consumer: consumer)

		// Write/flush on the event loop, can be called externally, so we must put it on the eventLoop explicitly.
		try await correlationMap.context!.eventLoop.submit {
			self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
		}.get()

		// Wait for the broker to respond with success (or error)
		try await promise.futureResult.get()

		// Create the consumer object and track it
		logger.info("Successfully subscribed to \(topic) with subscription: \(subscription)")

		// Issue initial flow permit
		try await correlationMap.context!.eventLoop.submit {
			self.flow(consumerID: consumerID, isInitial: true)
		}.get()

		return consumer
	}

	/// Permit new flow from broker to consumer.
	/// - Parameters:
	///   - consumerID: The id of the consumer to permit the message flow to.
	///   - isInitial: If it's initial request we request 1000, otherwise 500 more as per Pulsar protocol.
	func flow(consumerID: UInt64, isInitial: Bool = false) {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .flow
		var flowCmd = Pulsar_Proto_CommandFlow()
		flowCmd.messagePermits = isInitial ? 1000 : 500
		flowCmd.consumerID = consumerID
		baseCommand.flow = flowCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)
		correlationMap.context?.writeAndFlush(wrapOutboundOut(pulsarMessage), promise: nil)
	}
}
