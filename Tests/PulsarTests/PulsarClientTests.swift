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
import Logging
import NIO
import Testing

@testable import Pulsar

@Suite("Client Tests", .serialized)
struct ClientTests {
	let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
	@Test("Connect to a running client")
	func connect() async throws {
		try await ServerController.startServer()
		try await Task.sleep(for: .seconds(10))
		var error: PulsarClientError?
		_ = await PulsarClient(host: "localhost", port: 6650, reconnectLimit: 1) { pulsarError in
			if let pError = pulsarError as? PulsarClientError {
				error = pError
			}
		}
		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		Task {
			try await Task.sleep(for: .seconds(3))
			keepAlivePromise.succeed(())
		}
		try await keepAlivePromise.futureResult.get()
		try await ServerController.stopServer()
		#expect(error == nil)
	}

	@Test("Connect to a non running client")
	func connectNonRunning() async throws {
		var error: PulsarClientError?
		_ = await PulsarClient(host: "localhost", port: 6650, reconnectLimit: 1) { pulsarError in
			if let pError = pulsarError as? PulsarClientError {
				error = pError
			}
		}
		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		Task {
			try await Task.sleep(for: .seconds(3))
			keepAlivePromise.succeed(())
		}
		try await keepAlivePromise.futureResult.get()
		#expect(error == .clientClosed)
	}

	@Test("Client reconnection")
	func reconnection() async throws {
		try await ServerController.startServer()
		try await Task.sleep(for: .seconds(10))
		var error: PulsarClientError?
		_ = await PulsarClient(host: "localhost", port: 6650, reconnectLimit: 10) { pulsarError in
			if let pError = pulsarError as? PulsarClientError {
				error = pError
			}
		}
		let keepAlivePromise = eventLoopGroup.next().makePromise(of: Void.self)
		Task {
			try await ServerController.stopServer()
			try await Task.sleep(for: .seconds(15))
			try await ServerController.startServer()
			try await Task.sleep(for: .seconds(30))
			keepAlivePromise.succeed(())
		}
		try await keepAlivePromise.futureResult.get()
		#expect(error == nil)
	}
}
