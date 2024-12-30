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

import NIOCore

extension PulsarClientHandler {
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

	func subscribe(topic: String,
	               subscription: String,
	               consumerID: UInt64 = UInt64.random(in: 0 ..< UInt64.max),
	               schema: PulsarSchema,
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
		if let schemaCmd = getSchemaCmd(schema: schema) {
			subscribeCmd.schema = schemaCmd
		}
		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		baseCommand.subscribe = subscribeCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)

		// We add the consumer to the pool before connection, so in case the subscription attempt fails and we
		// need to reconnect, we already know the consumers we wanted.
		let consumer: PulsarConsumer
		if let existingConsumer {
			consumer = existingConsumer
			await consumer.stateManager.setHandler(self)
		} else {
			consumer = PulsarConsumer(
				handler: self,
				consumerID: consumerID,
				topic: topic,
				subscriptionName: subscription,
				subscriptionType: subscriptionType,
				subscriptionMode: subscriptionMode,
				schema: schema
			)
		}
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

	public func closeConsumer(consumerID: UInt64) async throws {
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

	/// The broker told us the consumer is being closed. We try re-subscribing.
	func handleClosedConsumer(consumerID: UInt64) {
		guard let consumerCache = consumers[consumerID] else {
			logger.warning("Received closeConsumer for unknown consumerID \(consumerID)")
			return
		}
		let consumer = consumerCache.consumer
		logger.warning("Server closed consumerID \(consumerID) for topic \(consumer.topic)")

		Task {
			do {
				logger.info("Attempting to re-subscribe consumer for \(consumer.topic)...")
				_ = try await client.consumer(topic: consumer.topic, subscription: consumer.subscriptionName, subscriptionType: .shared, consumerID: consumerID, existingConsumer: consumer)
				logger.info("Successfully re-subscribed \(consumer.topic)")
			} catch {
				logger.error("Re-subscribe failed for \(consumer.topic): \(error)")
			}
		}
	}
}
