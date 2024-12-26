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

import Logging
import NIO
import Pulsar

@main
struct PulsarExample {
	static func main() async throws {
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

		let client = await PulsarClient(host: "localhost", port: 6650)
		let consumer = try await client.consumer(topic: "persistent://public/default/my-topic", subscription: "test", subscriptionType: .shared)
		Task {
			do {
				for try await message in consumer {
					// Fix an concurrency false-positive in Swift 5.10 - It's only demo code, so no issue
					#if compiler(>=6)
						msgCount += 1
						print("Received message in the exec: \(String(decoding: message.data, as: UTF8.self))")
						if msgCount == 2 {
							try await consumer.close()
							print("Closed consumer")
						}
					#endif
				}
			} catch {
				print("Whooops we closed, this should never happen")
			}
		}

		let producer = try await client.producer(topic: "persistent://public/default/my-topic1", accessMode: .shared)
		Task {
			while true {
				do {
					let testMsg = "Hello from Swift".data(using: .utf8)!
					try await producer.asyncSend(message: Message(data: testMsg))
					try await Task.sleep(for: .seconds(5))
					print("we try to send a message here")
				} catch {
					fatalError("We got a timeout on send")
				}
			}
		}

		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		try await keepAlivePromise.futureResult.get()
	}
}
