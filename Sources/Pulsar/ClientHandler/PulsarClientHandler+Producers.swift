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
	func handleProducerSuccess(context _: ChannelHandlerContext, message: Pulsar_Proto_CommandProducerSuccess) {
		let requestID = message.requestID
		// the producer success message could assign a new name to the
		// producer but doesn't tell us the producer id, so we need to find our producer based on the request id.
		let producerID = producers.map { ($0.value.producerID, $0.value.createRequestID) }.filter { $0.1 == requestID }.first!.0
		Task {
			await producers[producerID]!.producer.producerName.set(message.producerName)
		}

		if let promise = correlationMap.remove(promise: .id(message.requestID)) {
			logger.debug("Success for requestID \(message.requestID)")
			promise.succeed()
		}
	}

	func createProducer(topic: String,
	                    accessMode: ProducerAccessMode,
	                    producerName: String? = nil,
	                    producerID: UInt64 = UInt64.random(in: 0 ..< UInt64.max),
	                    existingProducer: PulsarProducer? = nil) async throws -> PulsarProducer {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .producer
		var producerCmd = Pulsar_Proto_CommandProducer()
		producerCmd.topic = topic
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		producerCmd.requestID = requestID
		producerCmd.producerAccessMode = switch accessMode {
			case .shared: .shared
			case .exclusive: .exclusive
			case .exclusiveWithFencing: .exclusiveWithFencing
			case .waitForExclusive: .waitForExclusive
		}
		producerCmd.producerID = producerID
		if let producerName {
			producerCmd.producerName = producerName
		}

		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)

		baseCommand.producer = producerCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)

		// We add the producers to the pool before connection, so in case the create attempt fails and we
		// need to reconnect, we already know the producers we wanted.
		let producer = existingProducer ?? PulsarProducer(
			handler: self,
			producerID: producerID,
			topic: topic
		)
		producers[producerID] = ProducerCache(producerID: producerID, producer: producer, createRequestID: requestID)

		// Write/flush on the event loop, can be called externally, so we must put it on the eventLoop explicitly.
		try await correlationMap.context!.eventLoop.submit {
			self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
		}.get()

		// Wait for the broker to respond with success (or error)
		try await promise.futureResult.get()

		// Create the producer object and track it
		logger.info("Successfully created producer on \(topic)")
		#if DEBUG
			Task {
				let newProducerName = await producer.producerName.get()
				logger.trace("Producer got assigned name \(newProducerName ?? "none"), originally requested was \(producerName ?? "none.")")
			}
		#endif
		return producer
	}
}
