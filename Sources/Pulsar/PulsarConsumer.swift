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

/// A Pulsar Consumer, used to asynchronously consume messages from a topic.
public final class PulsarConsumer: AsyncSequence, Sendable {
	public let consumerID: UInt64
	let autoAcknowledge: Bool
	let topic: String
	let subscriptionName: String
	let subscriptionType: SubscriptionType
	let subscriptionMode: SubscriptionMode
	let stateManager = ConsumerStateHandler()

	private let stream: AsyncThrowingStream<Message, Error>
	let continuation: AsyncThrowingStream<Message, Error>.Continuation

	/// Used to consume messages.
	/// - Returns: The queue where the messages will land.
	public func makeAsyncIterator() -> AsyncThrowingStream<Message, Error>.AsyncIterator {
		stream.makeAsyncIterator()
	}

	init(autoAck: Bool = true,
	     handler: PulsarClientHandler,
	     consumerID: UInt64,
	     topic: String,
	     subscriptionName: String,
	     subscriptionType: SubscriptionType,
	     subscriptionMode: SubscriptionMode
	) {
		var cont: AsyncThrowingStream<Message, Error>.Continuation!
		stream = AsyncThrowingStream { c in
			cont = c
		}
		continuation = cont
		autoAcknowledge = autoAck
		self.consumerID = consumerID
		self.topic = topic
		self.subscriptionName = subscriptionName
		self.subscriptionType = subscriptionType
		self.subscriptionMode = subscriptionMode
		Task {
			await self.stateManager.setHandler(handler)
		}
	}

	/// Close the consumer
	public func close() async throws {
		try await stateManager.getHandler().closeConsumer(consumerID: consumerID)
	}

	func fail(error: Error) {
		continuation.finish(throwing: error)
	}

	func finish() {
		continuation.finish()
	}

	public typealias Element = Message
}
