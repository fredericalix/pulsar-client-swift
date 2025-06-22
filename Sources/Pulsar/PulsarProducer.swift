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

import Foundation

/// A Pulsar producer used to publish messages to a specific topic.
///
/// This class provides a mechanism for sending messages to an Apache Pulsar topic.
/// It supports both synchronous and asynchronous message publishing, allowing
/// developers to choose between guaranteed delivery with broker acknowledgment
/// (`syncSend`) and a fire-and-forget approach (`asyncSend`). The producer
/// is designed to handle different schema types and manage its lifecycle efficiently.
///
/// ## Features:
/// - Publishes messages to a specified Pulsar topic.
/// - Supports synchronous (`syncSend`) and asynchronous (`asyncSend`) message delivery.
/// - Manages producer state via `ProducerStateManager`.
/// - Configurable access mode and schema.
/// - Provides a closure (`onClosed`) to handle producer shutdown events.
///
/// ## Usage Example:
/// ```swift
/// let producer = PulsarProducer<MyPayload>(
///     handler: myHandler,
///     producerAccessMode: .exclusive,
///     producerID: 12345,
///     schema: mySchema,
///     topic: "persistent://public/default/my-topic"
/// )
///
/// try await producer.syncSend(message: myMessage) // Waits for broker response
/// try await producer.asyncSend(message: myMessage) // Fire-and-forget
/// try await producer.close() // Closes the producer
/// ```
///
/// ## Lifecycle:
/// - The producer is initialized with a handler, schema, topic, and other configurations.
/// - Messages can be sent using `syncSend` (awaits broker acknowledgment) or `asyncSend` (does not wait).
/// - The producer can be explicitly closed using `close()`, triggering the `onClosed` handler if provided.
///
/// ## Error Handling:
/// - `syncSend` throws an error if a broker acknowledgment is not received within a timeout.
/// - `asyncSend` does not throw errors for timeout issues but will throw for major failures.
/// - `close()` ensures a graceful shutdown of the producer.
///
/// - Note: This class is designed to be `Sendable`, meaning it can be used safely in concurrent contexts.
///
/// - Parameters:
///   - T: A type conforming to ``PulsarPayload``, representing the payload schema.
///
/// - SeeAlso: ``PulsarConsumer`` for message consumtion.
///
public final class PulsarProducer<T: PulsarPayload>: Sendable, AnyProducer {
	public let producerID: UInt64
	let topic: String
	let stateManager = ProducerStateManager()
	let accessMode: ProducerAccessMode
	let schema: PulsarSchema
	public let onClosed: (@Sendable (any Error) throws -> Void)?

	init(
		handler: PulsarClientHandler,
		producerAccessMode: ProducerAccessMode,
		producerID: UInt64,
		schema: PulsarSchema,
		topic: String,
		producerName: String? = nil,
		onClosed: (@Sendable (any Error) throws -> Void)?
	) {
		self.producerID = producerID
		self.topic = topic
		self.schema = schema
		self.onClosed = onClosed
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
	/// - throws: When we don't get an answer in the timeout, this method throws. For a version that does not care about timeouts, use ``PulsarProducer/asyncSend(message:)``.
	public func syncSend(message: Message<T>) async throws {
		await stateManager.increaseSequenceID()
		let producerName = await stateManager.getProducerName()!
		try await stateManager.getHandler()
			.send(message: message, producerID: producerID, producerName: producerName, isSyncSend: true)
	}

	/// Close the consumer
	public func close() async throws {
		try await stateManager.getHandler().closeProducer(producerID: producerID)
		try onClosed?(PulsarClientError.closedByUser)
	}

	/// Send messages asynchronously.
	/// - Parameter message: The message to send.
	///
	/// This method does not wait for a response from the server before returning, so should generally be faster. Also, this method does not throw when
	/// there is no response from the server after the timeout or anything else occurs, so it's fire and forget. Use ``PulsarProducer/syncSend(message:)`` if you do not want this behaviour.
	///
	/// - throws: Only throws when there is some major issue going on,
	public func asyncSend(message: Message<T>) async throws {
		await stateManager.increaseSequenceID()
		let producerName = await stateManager.getProducerName()!
		try await stateManager.getHandler()
			.send(message: message, producerID: producerID, producerName: producerName, isSyncSend: false)
	}

	func handleClosing() async throws {
		let handler = await stateManager.getHandler()
		_ = try await handler.client.producer(
			topic: topic,
			accessMode: accessMode,
			schema: schema,
			producerID: producerID,
			existingProducer: self,
			onClosed: onClosed
		)
	}
}
