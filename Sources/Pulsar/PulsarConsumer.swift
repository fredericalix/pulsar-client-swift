// Copyright 2025 Felix Ruppert
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

/// A Pulsar consumer used to asynchronously consume messages from a specific topic.
///
/// This class provides functionality for consuming messages from an Apache Pulsar topic.
/// It supports different subscription types and conforms to `AsyncSequence`, allowing
/// messages to be iterated over in an asynchronous context using Swift's `for await` syntax.
///
/// ## Features:
/// - Conforms to `AsyncSequence`, enabling structured and idiomatic message consumption.
/// - Handles message acknowledgment automatically (if `autoAcknowledge` is enabled).
/// - Supports schema-based payload deserialization.
/// - Provides explicit error handling mechanisms.
///
/// ## Usage Example:
/// ```swift
/// let consumer = PulsarConsumer<MyPayload>(
///     autoAck: true,
///     handler: myHandler,
///     consumerID: 67890,
///     topic: "persistent://public/default/my-topic",
///     subscriptionName: "my-subscription",
///     subscriptionType: .shared,
///     subscriptionMode: .durable,
///     schema: mySchema
/// )
///
/// for await message in consumer {
///     print("Received message: \(message.payload)")
/// }
///
/// try await consumer.close() // Close the consumer when done
/// ```
///
/// ## Lifecycle:
/// - The consumer is initialized with a handler, topic, subscription details, and schema.
/// - Messages are received and decoded using the specified schema.
/// - The consumer continuously yields messages via `AsyncThrowingStream<Message<T>, Error>`.
/// - The consumer can be explicitly closed using `close()`, ensuring proper resource cleanup.
///
/// ## Error Handling:
/// - If message deserialization fails, the consumer will call `fail(error:)`, terminating the stream.
/// - If an error occurs while handling messages, the stream finishes with the provided error.
/// - Closing the consumer ensures proper detachment from the Pulsar client.
///
/// - Note: This class is designed to be `Sendable`, meaning it can be safely used in concurrent contexts.
///
/// - Parameters:
///   - T: A type conforming to ``PulsarPayload``, representing the message payload.
///
/// - SeeAlso: ``PulsarProducer`` for message publishing.
///
public final class PulsarConsumer<T: PulsarPayload>: AsyncSequence, Sendable, AnyConsumer {
	public let consumerID: UInt64
	let autoAcknowledge: Bool
	let topic: String
	let subscriptionName: String
	let subscriptionType: SubscriptionType
	let subscriptionMode: SubscriptionMode
	let schema: PulsarSchema
	let stateManager = ConsumerStateHandler()

	private let stream: AsyncThrowingStream<Message<T>, Error>
	let continuation: AsyncThrowingStream<Message<T>, Error>.Continuation

	/// Used to consume messages.
	/// - Returns: The queue where the messages will land.
	public func makeAsyncIterator() -> AsyncThrowingStream<Message<T>, Error>.AsyncIterator {
		stream.makeAsyncIterator()
	}

	init(
		autoAck: Bool = true,
		handler: PulsarClientHandler,
		consumerID: UInt64,
		topic: String,
		subscriptionName: String,
		subscriptionType: SubscriptionType,
		subscriptionMode: SubscriptionMode,
		schema: PulsarSchema
	) {
		var cont: AsyncThrowingStream<Message<T>, Error>.Continuation!
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
		self.schema = schema
		Task {
			await self.stateManager.setHandler(handler)
		}
	}

	func handleMessasge(_ pulsarMessage: PulsarMessage) {
		if let payload = pulsarMessage.payload {
			do {
				let typedPayload = try T.decode(from: payload)
				continuation.yield(
					Message(payload: typedPayload)
				)
			} catch {
				fail(error: error)
			}
		}
	}

	func handleClosing() async throws {
		let handler = await stateManager.getHandler()
		_ = try await handler.client.consumer(
			topic: topic,
			subscription: subscriptionName,
			subscriptionType: subscriptionType,
			consumerID: consumerID,
			existingConsumer: self
		)
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
