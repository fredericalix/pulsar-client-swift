# Get started

This section provides an overview how to quickly get the library up and running.

## Consumer

```swift
import Logging
import NIO
import Pulsar

@main
struct PulsarExample {
	static func main() async throws {
		// You do not need to provide your own EventLoopGroup.
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
		// Create a Pulsar client and connect to the server at localhost:6650.
		let client = await PulsarClient(host: "localhost", port: 6650)

		// Create a consumer at the specified topic with the wanted subscription name.
		let consumer = try await client.consumer(topic: "persistent://public/default/my-topic", subscription: "test", subscriptionType: .shared)
		Task {
			do {
				// Consume messages and do a thing, everytime you receive a message.
				for try await message in consumer {
					msgCount += 1
					print("Received message in the exec: \(String(decoding: message.data, as: UTF8.self))")
					if msgCount == 2 {
						try await consumer.close()
						print("Closed consumer")
					}
				}
			} catch {
				// The consumer should never close automatically, only when you call consumer.close()
				print("Whooops we closed, this should never happen automatically.")
			}
		}
		// Keep the application running.
		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		try await keepAlivePromise.futureResult.get()
	}
}
```
