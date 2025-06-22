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

import NIOCore

extension PulsarClient {
	/// Creates a new Pulsar producer.
	/// - Parameters:
	///   - topic: The topic to produce to.
	///   - accessMode: The access mode of the producer.
	///   - schema: The Pulsar Schema to use.
	///   - producerID: Optional: If you want to define your own producerID.
	///   - producerName: Optional: The name of the producer. Gets auto-assigned by the server when left empty.
	///   - connectionString: Not recommended:  Define another URL where the topic will be found. Can cause issues with connection.
	///   - existingProducer: Not recommended: Reuse an existing producer.
	///   - onClosed: This function will be called if the producer needs to be closed.
	/// - Returns: The newly created producer.
	/// - Throws: Throws an error when there is an issue that cannot be handled by internally.
	///
	/// - Warning: `connectionString` and `existingProducer` are there for internal implementation and shouldn't be used by the library user.
	public func producer<T>(
		topic: String,
		accessMode: ProducerAccessMode,
		schema: PulsarSchema = .bytes,
		producerID: UInt64? = nil,
		producerName: String? = nil,
		connectionString: String? = nil,
		existingProducer: PulsarProducer<T>? = nil,
		onClosed: (@Sendable (any Error) throws -> Void)?
	) async throws -> PulsarProducer<T> {
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
				try await connect(host: connectionString, port: port)
			} else {
				// Means topicFound or broker said "use the existing connection"
				topicFound = true
			}
		}

		guard let channel = connectionPool[connectionString] else {
			throw PulsarClientError.connectionError
		}
		let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

		let producer: PulsarProducer =
			if let existingProducer {
				try await handler.createProducer(
					topic: topic,
					accessMode: accessMode,
					schema: schema,
					producerName: producerName,
					producerID: producerID!,
					existingProducer: existingProducer,
					onClosed: onClosed
				)
			} else {
				try await handler.createProducer(
					topic: topic,
					accessMode: accessMode,
					schema: schema,
					producerName: producerName,
					onClosed: onClosed
				)
			}

		return producer
	}
}
