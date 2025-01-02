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
import NIOCore
import NIOFoundationCompat

extension PulsarClientHandler {
	func handleProducerSuccess(context _: ChannelHandlerContext, message: Pulsar_Proto_CommandProducerSuccess) {
		let requestID = message.requestID
		// the producer success message could assign a new name to the
		// producer but doesn't tell us the producer id, so we need to find our producer based on the request id.
		let producerID = producers.map { ($0.value.producerID, $0.value.createRequestID) }.filter { $0.1 == requestID }.first!.0
		Task {
			await producers[producerID]!.producer.stateManager.setProducerName(message.producerName)
			await producers[producerID]!.producer.stateManager.setSequenceID(message.lastSequenceID)
		}

		if let promise = correlationMap.remove(promise: .id(message.requestID)) {
			logger.debug("Success for requestID \(message.requestID)")
			promise.succeed()
		}
	}

	func closeProducer(producerID: UInt64) async throws {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .closeProducer
		var closeCmd = Pulsar_Proto_CommandCloseProducer()
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		closeCmd.producerID = producerID
		closeCmd.requestID = requestID

		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		baseCommand.closeProducer = closeCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)

		try await correlationMap.context!.eventLoop
			.submit {
				self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
			}
			.get()

		try await promise.futureResult.get()
	}

	func handleSendReceipt(context _: ChannelHandlerContext, message: Pulsar_Proto_CommandSendReceipt) {
		let producerID = message.producerID
		let sequenceID = message.sequenceID

		if let promise = correlationMap.remove(promise: .send(producerID: producerID, sequenceID: sequenceID)) {
			logger.debug("Success for send with producerID: \(producerID) and sequenceID: \(sequenceID).")
			promise.succeed()
		}
	}

	func handleClosedProducer(producerID: UInt64) {
		guard let producerCache = producers[producerID] else {
			logger.warning("Received closeProducer for unknown producerID \(producerID)")
			return
		}
		let producer = producerCache.producer
		logger.warning("Server closed producerID \(producerID) for topic \(producer.topic)")

		// Optional: attempt a re-subscribe
		Task {
			do {
				logger.info("Attempting to reconnect producer for \(producer.topic)...")
				try await producer.handleClosing()
				logger.info("Successfully reconnected \(producer.topic)")
			} catch {
				logger.error("Reconnect failed for \(producer.topic): \(error)")
			}
		}
	}

	func send(message: Message<some PulsarPayload>, producerID: UInt64, producerName: String, isSyncSend: Bool) async throws {
		let data = message.payload
		let payload = data.encode()
		var baseCmd = Pulsar_Proto_BaseCommand()
		baseCmd.type = .send
		var sendCmd = Pulsar_Proto_CommandSend()
		sendCmd.producerID = producerID
		let sequenceID = await producers[producerID]!.producer.stateManager.sequenceID
		sendCmd.sequenceID = UInt64(sequenceID)
		sendCmd.numMessages = 1
		baseCmd.send = sendCmd
		var msgMetadata = Pulsar_Proto_MessageMetadata()
		msgMetadata.producerName = producerName
		msgMetadata.sequenceID = UInt64(sequenceID)
		msgMetadata.publishTime = UInt64(Date().timeIntervalSince1970 * 1000)
		msgMetadata.properties = []
		var promise: EventLoopPromise<Void>
		promise = makePromise(
			context: correlationMap.context!,
			type: .send(producerID: producerID, sequenceID: UInt64(sequenceID)),
			forceClose: isSyncSend
		)
		// We just instantiated a promise, so it is there.
		correlationMap.add(promise: .send(producerID: producerID, sequenceID: UInt64(sequenceID)), promiseValue: promise)
		let message = PulsarMessage(command: baseCmd, messageMetadata: msgMetadata, payload: payload)
		try await correlationMap.context!.eventLoop
			.submit {
				self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(message), promise: nil)
			}
			.get()
		if isSyncSend {
			// We are sure the promise is not null because it got added in the i-block with the same condition before.
			try await promise.futureResult.get()
		}
	}

	func createProducer<T: PulsarPayload>(
		topic: String,
		accessMode: ProducerAccessMode,
		schema: PulsarSchema,
		producerName: String? = nil,
		producerID: UInt64 = UInt64.random(in: 0 ..< UInt64.max),
		existingProducer: PulsarProducer<T>? = nil,
		onClosed: (@Sendable (any Error) throws -> Void)?
	) async throws -> PulsarProducer<T> {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .producer
		var producerCmd = Pulsar_Proto_CommandProducer()
		producerCmd.topic = topic
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		producerCmd.requestID = requestID
		producerCmd.producerAccessMode =
			switch accessMode {
				case .shared: .shared
				case .exclusive: .exclusive
				case .exclusiveWithFencing: .exclusiveWithFencing
				case .waitForExclusive: .waitForExclusive
			}
		producerCmd.producerID = producerID
		if let producerName {
			producerCmd.producerName = producerName
		}
		if let schemaCmd = getSchemaCmd(schema: schema) {
			producerCmd.schema = schemaCmd
		}

		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		baseCommand.producer = producerCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)

		// We add the producers to the pool before connection, so in case the create attempt fails and we
		// need to reconnect, we already know the producers we wanted.
		let producer: PulsarProducer<T>
		if let existingProducer {
			producer = existingProducer
			await producer.stateManager.setHandler(self)
		} else {
			producer = PulsarProducer(
				handler: self,
				producerAccessMode: accessMode,
				producerID: producerID,
				schema: schema,
				topic: topic,
				onClosed: onClosed
			)
		}
		producers[producerID] = ProducerCache(producerID: producerID, producer: producer, createRequestID: requestID)

		// Write/flush on the event loop, can be called externally, so we must put it on the eventLoop explicitly.
		try await correlationMap.context!.eventLoop
			.submit {
				self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
			}
			.get()

		// Wait for the broker to respond with success (or error)
		try await promise.futureResult.get()

		// Create the producer object and track it
		logger.info("Successfully created producer on \(topic)")
		#if DEBUG
			Task {
				let newProducerName = await producer.stateManager.getProducerName()
				logger.trace(
					"Producer got assigned name \(newProducerName ?? "none"), originally requested was \(producerName ?? "none.")"
				)
			}
		#endif
		return producer
	}
}
