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
	let producerID: UInt64
	let topic: String
	let stateManager = ProducerStateManager()
	let accessMode: ProducerAccessMode

	init(handler: PulsarClientHandler, producerAccessMode: ProducerAccessMode, producerID: UInt64, topic: String, producerName: String? = nil) {
		self.producerID = producerID
		self.topic = topic
		accessMode = producerAccessMode
		Task {
			await self.stateManager.setHandler(handler)
			await self.stateManager.setProducerName(producerName)
		}
	}

	/// Send messages synchronously.
	/// - Parameter message: The message to send.
	///
	/// Although this method is called `syncSend`, it is asynchronous. In the context of Pulsar, `syncSend` means
	/// we wait for an answer of the broker before returning this method. To prevent blocking the thread and "only" suspend execution
	/// till this answer is received, this method is asynchronous.
	///
	/// When we don't get an answer in the timeout, this method throws. For a version that does not care about timeouts, use ``PulsarProducer/asyncSend(message:)``.
	public func syncSend(message: Message) async throws {
		await stateManager.increaseSequenceID()
		let producerName = await stateManager.getProducerName()!
		try await stateManager.getHandler().send(message: message, producerID: producerID, producerName: producerName, isSyncSend: true)
	}

	/// Send messages asynchronously.
	/// - Parameter message: The message to send.
	///
	/// This method does not wait for a response from the server before returning, so should generally be faster. Also, this method does not throw when
	/// there is no response from the server after the timeout. Use ``PulsarProducer/syncSend(message:)`` if you want this behaviour.
	public func asyncSend(message: Message) async throws {
		await stateManager.increaseSequenceID()
		let producerName = await stateManager.getProducerName()!
		try await stateManager.getHandler().send(message: message, producerID: producerID, producerName: producerName, isSyncSend: false)
	}
}
