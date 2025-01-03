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

import NIOSSL

/// A TLS connection to configure the client connection.
public struct TLSConnection: Sendable {
	let tlsConfiguration: TLSConfiguration
	let clientCA: NIOSSLCertificate
	let authenticationRequired: Bool

	public init(tlsConfiguration: TLSConfiguration, clientCA: NIOSSLCertificate, authenticationRequired: Bool) {
		self.tlsConfiguration = tlsConfiguration
		self.clientCA = clientCA
		self.authenticationRequired = authenticationRequired
	}
}
