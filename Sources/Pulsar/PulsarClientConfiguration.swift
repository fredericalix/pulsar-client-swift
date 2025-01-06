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

/// A Pulsar Client configuration object.
public struct PulsarClientConfiguration: Sendable {
	let host: String
	let port: Int
	let tlsConfiguration: TLSConnection?
	let group: EventLoopGroup?
	let reconnectionLimit: Int?

	/// Creates a new Pulsar Client configuration.
	/// - Parameters:
	///   - host: The host to connect to. Doesn't need the `pulsar://` prefix.
	///   - port: The port to connect to. Normally `6650`.
	///   - tlsConfiguration: If you connect to a `pulsar+ssl` URL, you need to provide a TLS configuration.
	///   - group: If you want to pass your own EventLoopGroup, you can do it here. Otherwise the client will create it's own.
	///   - reconnectionLimit: How often the client should try reconnecting, if a connection is lost. The reconnection happens with an exponential backoff. The default limit is 10. Pass `nil` if the client should try reconnecting indefinitely.
	public init(
		host: String,
		port: Int,
		tlsConfiguration: TLSConnection? = nil,
		group: EventLoopGroup? = nil,
		reconnectionLimit: Int? = 10
	) {
		self.host = host
		self.port = port
		self.tlsConfiguration = tlsConfiguration
		self.group = group
		self.reconnectionLimit = reconnectionLimit
	}
}
