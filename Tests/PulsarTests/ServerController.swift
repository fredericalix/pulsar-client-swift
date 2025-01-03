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

struct ServerController {
	static func runCommand(_ command: String) async throws {
		let process = Process()
		let pipe = Pipe()
		let errorPipe = Pipe()
		#if canImport(Darwin)
			let shellPath = "/bin/zsh"
		#else
			let shellPath = "/bin/bash"
		#endif

		process.executableURL = URL(fileURLWithPath: shellPath)
		process.arguments = ["-c", command]
		process.standardOutput = pipe
		process.standardError = errorPipe

		try process.run()
		process.waitUntilExit()

		let outputData = pipe.fileHandleForReading.readDataToEndOfFile()
		let errorData = errorPipe.fileHandleForReading.readDataToEndOfFile()

		let output = String(data: outputData, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
		let error = String(data: errorData, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)

		if process.terminationStatus == 0 {
			print("Command executed successfully: \(command)")
			print("Output: \(output ?? "None")")
		} else {
			print("Command failed: \(command)")
			print("Error: \(error ?? "None")")
		}
	}

	static func startServer() async throws {
		let dockerCommand = """
			/usr/local/bin/docker run -d --name pulsar -it \\
			-p 6650:6650 \\
			-p 8080:8080 \\
			--mount source=pulsardata,target=/pulsar/data \\
			--mount source=pulsarconf,target=/pulsar/conf \\
			apachepulsar/pulsar:4.0.1 \\
			bin/pulsar standalone
			"""
		try await runCommand(dockerCommand)
	}

	static func stopServer() async throws {
		// Stop the Pulsar container
		try await runCommand("/usr/local/bin/docker stop pulsar")

		// Remove the Pulsar container
		try await runCommand("/usr/local/bin/docker rm pulsar")
	}
}
