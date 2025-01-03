# Get Started

Welcome to the Swift Pulsar Client! This guide will help you get started with the library quickly by walking you through the basic setup and usage for both consuming and producing messages.

## Consumer Example
The following example demonstrates how to create a Pulsar consumer to receive messages from a specific topic.

```swift
import Logging
import NIO
import Pulsar

@main
struct PulsarExample {
	static func main() async throws {
		// Set up logging and event loop group
		let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
		LoggingSystem.bootstrap { label in
			var handler = StreamLogHandler.standardOutput(label: label)
			handler.logLevel = .trace
			return handler
		}

		let connector = PulsarExample()
		try await connector.connect(eventLoopGroup: eventLoopGroup)
	}

	func connect(eventLoopGroup: EventLoopGroup) async throws {
		var msgCount = 0

		// Create a Pulsar client
		let client = await PulsarClient(host: "localhost", port: 6650)

		// Set up a consumer
		let consumer = try await client.consumer(
			topic: "persistent://public/default/my-topic",
			subscription: "test",
			subscriptionType: .shared
		)

		// Consume messages
		Task {
			do {
				for try await message in consumer {
					msgCount += 1
					print("Received message: \(String(decoding: message.data, as: UTF8.self))")
					if msgCount == 2 {
						try await consumer.close()
						print("Closed consumer")
					}
				}
			} catch {
				print("Unexpected consumer closure: \(error)")
			}
		}

		// Keep the application running
		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		try await keepAlivePromise.futureResult.get()
	}
}
```

## Producer Example
This example shows how to create a producer to send messages to a specific Pulsar topic.

```swift
import Foundation
import Logging
import NIO
import Pulsar

@main
struct PulsarExample {
	static func main() async throws {
		// Set up logging and event loop group
		let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
		LoggingSystem.bootstrap { label in
			var handler = StreamLogHandler.standardOutput(label: label)
			handler.logLevel = .trace
			return handler
		}

		let connector = PulsarExample()
		try await connector.connect(eventLoopGroup: eventLoopGroup)
	}

	func connect(eventLoopGroup: EventLoopGroup) async throws {
		let client = await PulsarClient(host: "localhost", port: 6650, reconnectLimit: 10) { error in
			print("Client closed due to error: \(error)")
			exit(0)
		}

		// Set up a producer
		let producer = try await client.producer(
			topic: "persistent://public/default/my-topic1",
			accessMode: .shared,
			schema: .string
		) { _ in
			print("Producer closed")
		} as PulsarProducer<String>

		// Send messages in a loop
		Task {
			while true {
				do {
					let message = "Hello from Swift"
					try await producer.asyncSend(message: Message(payload: message))
					print("Sent message: \(message)")
					try await Task.sleep(for: .seconds(5))
				} catch {
					print("Failed to send message: \(error)")
				}
			}
		}

		// Keep the application running
		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		try await keepAlivePromise.futureResult.get()
	}
}
```

## Secure connection

The library supports mTLS encryption as well as mTLS authentication.

```swift
var clientCertPath: String? {
			// Get all the nescessary certs
			Bundle.module.path(forResource: "client-cert", ofType: "pem")
		}

		var clientKeyPath: String? {
			Bundle.module.path(forResource: "client-key", ofType: "pem")
		}

		var caCertPath: String? {
			Bundle.module.path(forResource: "ca-cert", ofType: "pem")
		}

		// Build the NIOSSLCertificates
		let clientCertificate = try NIOSSLCertificate(file: clientCertPath!, format: .pem)
		let clientPrivateKey = try NIOSSLPrivateKey(file: clientKeyPath!, format: .pem)
		let caCertificate = try NIOSSLCertificate(file: caCertPath!, format: .pem)

		// Make a NIO client TLS configuration.
		var tlsConfig = TLSConfiguration.makeClientConfiguration()
		tlsConfig.certificateVerification = .fullVerification
		tlsConfig.trustRoots = .certificates([caCertificate])
		tlsConfig.privateKey = .privateKey(clientPrivateKey)
		tlsConfig.certificateChain = [.certificate(clientCertificate)]
		tlsConfig.certificateVerification = .fullVerification

		// Wrap it into TLSConnection and define if the cluster only uses TLS encryption or also authentication
		let auth = TLSConnection(tlsConfiguration: tlsConfig, clientCA: clientCertificate, authenticationRequired: true)

		let client = try await PulsarClient(
			host: "localhost",
			port: 6651,
			tlsConfiguration: auth,
			group: eventLoopGroup,
			reconnectLimit: 10
		) { error in
			do {
				throw error
			} catch {
				print("Client closed")
				exit(0)
			}
		}
```

## Additional Features
- **Reconnection Handling**: Configure the reconnection limit with the `reconnectLimit` parameter when initializing the `PulsarClient`.
- **Schema Support**: Specify schemas like `.string` for type-safe message handling.
- **Logging**: Use Swift's `Logging` package to customize log levels and outputs.
