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

public extension PulsarClient {
	func producer(
		topic: String,
		producerID: UInt64? = nil,
		producerName: String? = nil,
		connectionString: String? = nil,
		existingProducer: PulsarProducer? = nil) async throws -> PulsarProducer {
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

		guard let channel = connectionPool[connectionString] else {
			throw PulsarClientError.topicLookupFailed
		}
		let handler = try await channel.pipeline.handler(type: PulsarClientHandler.self).get()

		let producer: PulsarProducer = if let existingProducer {
			try await handler.createProducer(
				topic: topic,
				producerName: producerName,
				producerID: producerID!,
				existingProducer: existingProducer
			)
		} else {
			try await handler.createProducer(topic: topic, producerName: producerName)
		}

		return producer
	}
}
