//
//  PulsarClient+Consumers.swift
//  pulsar-client-swift
//
//  Created by Felix Ruppert on 26.12.24.
//

public extension PulsarClient {
	/// Creates a new Pulsar consumer.
	/// - Parameters:
	///   - topic: The topic to consume.
	///   - subscription: The name of the subscription.
	///   - subscriptionType: The type of the subscription.
	///   - subscriptionMode: Optional: The mode of the subscription.
	///   - consumerID: Optional: If you want to define your own consumerID.
	///   - connectionString: Not recommended:  Define another URL where the topic will be found. Can cause issues with connection.
	///   - existingConsumer: Not recommended: Reuse an existing consumer.
	/// - Returns: The newly created consumer.
	///
	/// - Warning: `connectionString` and `existingConsumer` are there for internal implementation and shouldn't be used by the library user.
	func consumer(
		topic: String,
		subscription: String,
		subscriptionType: SubscriptionType,
		subscriptionMode: SubscriptionMode = .durable,
		consumerID: UInt64? = nil,
		connectionString: String? = nil,
		existingConsumer: PulsarConsumer? = nil) async throws -> PulsarConsumer {
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
				await connect(host: connectionString, port: 6650)
			} else {
				// Means topicFound or broker said "use the existing connection"
				topicFound = true
			}
		}

		// By now, we have a connection for `connectionString`
		guard let channel = connectionPool[connectionString] else {
			throw PulsarClientError.topicLookupFailed
		}
		let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

		let consumer: PulsarConsumer = if let existingConsumer {
			// We DO NOT want a new consumer. We reattach it by performing the consumer flow again.
			try await handler.subscribe(
				topic: topic,
				subscription: subscription,
				consumerID: consumerID!,
				existingConsumer: existingConsumer,
				subscriptionType: subscriptionType,
				subscriptionMode: subscriptionMode
			)
		} else {
			try await handler.subscribe(
				topic: topic,
				subscription: subscription,
				subscriptionType: subscriptionType,
				subscriptionMode: subscriptionMode
			)
		}
		return consumer
	}
}
