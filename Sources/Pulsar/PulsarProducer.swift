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

/// A Pulsar producer, used to publish messages to a topic.
public final class PulsarProducer: Sendable {
	let handler: PulsarClientHandler
	let producerID: UInt64
	let topic: String
	let producerCache = ProducerCacheActor()
	let accessMode: ProducerAccessMode

	init(handler: PulsarClientHandler, producerAccessMode: ProducerAccessMode, producerID: UInt64, topic: String, producerName: String? = nil) {
		self.handler = handler
		self.producerID = producerID
		self.topic = topic
		accessMode = producerAccessMode
		Task {
			await self.producerCache.setProducerName(producerName)
		}
	}
	
	/// Send messages synchronously.
	/// - Parameter message: The message to send.
	///
	/// Although this method is called `syncSend`, it is asynchronous. In the context of Pulsar, `syncSend` means
	/// we wait for an answer of the broker before returning this method. To prevent blocking the thread and "only" suspend execution
	/// till this answer is received, this method is asynchronous.
	public func syncSend(message: Message) async throws {
		await producerCache.increaseSequenceID()
		let producerName = await producerCache.getProducerName()!
		try await handler.send(message: message, producerID: producerID, producerName: producerName)
	}
}

actor ProducerCacheActor {
	var producerName: String?
	var sequenceID: UInt64 = 0

	func setProducerName(_ producerName: String?) {
		self.producerName = producerName
	}

	func getProducerName() -> String? {
		producerName
	}

	@discardableResult
	func increaseSequenceID() -> UInt64 {
		sequenceID += 1
		return sequenceID
	}
}
