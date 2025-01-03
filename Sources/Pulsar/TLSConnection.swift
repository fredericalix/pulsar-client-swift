//
//  TLSAuthentication.swift
//  pulsar-client-swift
//
//  Created by Felix Ruppert on 03.01.25.
//

import NIOSSL

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
