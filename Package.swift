// swift-tools-version: 5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
	name: "pulsar-client-swift",
	platforms: [.macOS(.v13)],
	products: [
		// Products define the executables and libraries a package produces, making them visible to other packages.
		.library(
			name: "Pulsar",
			targets: ["Pulsar"]
		)
	],
	dependencies: [
		.package(url: "https://github.com/apple/swift-nio.git", from: "2.77.0"),
		.package(url: "https://github.com/apple/swift-protobuf.git", from: "1.28.2"),
		.package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
		.package(url: "https://github.com/apple/swift-log.git", from: "1.6.1")
	],
	targets: [
		// Targets are the basic building blocks of a package, defining a module or a test suite.
		// Targets can depend on other targets in this package and products from dependencies.
		.target(
			name: "Pulsar",
			dependencies: [
				.product(name: "NIO", package: "swift-nio"),
				.product(name: "SwiftProtobuf", package: "swift-protobuf"),
				.product(name: "NIOSSL", package: "swift-nio-ssl"),
				.product(name: "NIOFoundationCompat", package: "swift-nio"),
				.product(name: "Logging", package: "swift-log")
			]

		),
		.executableTarget(
			name: "PulsarExample",
			dependencies: [
				"Pulsar"
			]
		),
		.testTarget(
			name: "pulsar-client-swiftTests",
			dependencies: ["Pulsar"]
		)
	],
	swiftLanguageVersions: [.version("6"), .v5]
)
