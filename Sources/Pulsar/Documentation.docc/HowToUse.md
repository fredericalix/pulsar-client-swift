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

		// Configure the Pulsar client
		let config = PulsarClientConfiguration(
			host: "localhost",
			port: 6650,
			group: eventLoopGroup,
			reconnectionLimit: 10
		)

		// Create a Pulsar client
		let client = try await PulsarClient(configuration: config) { error in
		   print("Error: \(error)")
	   }

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
		// Configure the Pulsar client
		let config = PulsarClientConfiguration(
			host: "localhost",
			port: 6650,
			group: eventLoopGroup,
			reconnectionLimit: 10
		)
		let client = try await PulsarClient(configuration: config) { error in
			print("Error: \(error)")
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
import Foundation
import NIO
import NIOSSL
import Pulsar

func createSecureClient(eventLoopGroup: EventLoopGroup) async throws -> PulsarClient {
	// Load certificates
	guard
		let clientCertPath = Bundle.module.path(forResource: "client-cert", ofType: "pem"),
		let clientKeyPath = Bundle.module.path(forResource: "client-key", ofType: "pem"),
		let caCertPath = Bundle.module.path(forResource: "ca-cert", ofType: "pem")
	else {
		fatalError("Certificates not found")
	}

	let clientCertificate = try NIOSSLCertificate(file: clientCertPath, format: .pem)
	let clientPrivateKey = try NIOSSLPrivateKey(file: clientKeyPath, format: .pem)
	let caCertificate = try NIOSSLCertificate(file: caCertPath, format: .pem)

	// Configure TLS
	var tlsConfig = TLSConfiguration.makeClientConfiguration()
	tlsConfig.certificateVerification = .fullVerification
	tlsConfig.trustRoots = .certificates([caCertificate])
	tlsConfig.privateKey = .privateKey(clientPrivateKey)
	tlsConfig.certificateChain = [.certificate(clientCertificate)]

	// Create TLS connection configuration
	let auth = TLSConnection(
		tlsConfiguration: tlsConfig,
		clientCA: clientCertificate,
		authenticationRequired: true
	)

	// Configure the Pulsar client with TLS
	let config = PulsarClientConfiguration(
		host: "localhost",
		port: 6651,
		tlsConfiguration: auth,
		group: eventLoopGroup,
		reconnectionLimit: 10
	)

	return try await PulsarClient(configuration: config) { error in
		print("Error: \(error)")
	}
}
```

## Additional Features
- **Reconnection Handling**: Configure the reconnection limit with the `reconnectLimit` parameter when initializing the `PulsarClient`.
- **Schema Support**: Specify schemas like `.string` for type-safe message handling.
- **Logging**: Use Swift's `Logging` package to customize log levels and outputs.
