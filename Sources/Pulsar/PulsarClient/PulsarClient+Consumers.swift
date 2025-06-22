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
import Logging

extension PulsarClient {
	/// Creates a new Pulsar consumer.
	/// - Parameters:
	///   - topic: The topic to consume.
	///   - subscription: The name of the subscription.
	///   - subscriptionType: The type of the subscription.
	///   - schema: The Pulsar Schema to use.
	///   - subscriptionMode: Optional: The mode of the subscription.
	///   - consumerID: Optional: If you want to define your own consumerID.
	///   - connectionString: Not recommended:  Define another URL where the topic will be found. Can cause issues with connection.
	///   - existingConsumer: Not recommended: Reuse an existing consumer.
	/// - Returns: The newly created consumer.
	/// - Throws: Throws an error when there is an issue that cannot be handled by internally.
	///
	/// - Warning: `connectionString` and `existingConsumer` are there for internal implementation and shouldn't be used by the library user.
	public func consumer<T: PulsarPayload>(
		topic: String,
		subscription: String,
		subscriptionType: SubscriptionType,
		schema: PulsarSchema = .bytes,
		subscriptionMode: SubscriptionMode = .durable,
		consumerID: UInt64? = nil,
		connectionString: String? = nil,
		existingConsumer: PulsarConsumer<T>? = nil
	) async throws -> PulsarConsumer<T> {
		var connectionString = connectionString ?? initialURL
		var topicFound = false
		// Possibly do multiple lookups if the broker says "redirect"
		while !topicFound {
			// Must have a channel for the current connectionString
			guard let channel = connectionPool[connectionString] else {
				throw PulsarClientError.connectionError
			}
			let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

			// Request a topic-lookup from the handler
			let lookup = try await handler.topicLookup(topic: topic, authorative: false)
			if let redirectHost = lookup.0, !redirectHost.isEmpty {
				// The broker told us to connect somewhere else
				connectionString = getConnection(connectionString: redirectHost).0
				handler.host = connectionString
				try await connect(host: connectionString, port: port)
			} else {
				// Means topicFound or broker said "use the existing connection"
				topicFound = true
			}
		}

		// By now, we have a connection for `connectionString`
		guard let channel = connectionPool[connectionString] else {
			throw PulsarClientError.connectionError
		}
		let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

		let consumer: PulsarConsumer =
			if let existingConsumer {
				// We DO NOT want a new consumer. We reattach it by performing the consumer flow again.
				try await handler.subscribe(
					topic: topic,
					subscription: subscription,
					consumerID: consumerID!,
					schema: schema,
					existingConsumer: existingConsumer,
					subscriptionType: subscriptionType,
					subscriptionMode: subscriptionMode
				)
			} else {
				try await handler.subscribe(
					topic: topic,
					subscription: subscription,
					schema: schema,
					subscriptionType: subscriptionType,
					subscriptionMode: subscriptionMode
				)
			}
		return consumer
	}
}
