// swift-tools-version: 6.0

import PackageDescription

let package = Package(
	name: "pulsar-client-swift",
	platforms: [.macOS(.v13), .iOS(.v16), .tvOS(.v16), .watchOS(.v9)],
	products: [
		.library(
			name: "Pulsar",
			targets: ["Pulsar"]
		),
		.library(
			name: "SchemaTypes",
			targets: ["SchemaTypes"]
		)
	],
	dependencies: [
		.package(url: "https://github.com/apple/swift-nio.git", exact: "2.79.0"),
		.package(url: "https://github.com/apple/swift-protobuf.git", from: "1.28.2"),
		.package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.29.0"),
		.package(url: "https://github.com/apple/swift-log.git", from: "1.6.1")
	],
	targets: [
		.target(
			name: "Pulsar",
			dependencies: [
				.product(name: "NIO", package: "swift-nio"),
				.product(name: "SwiftProtobuf", package: "swift-protobuf"),
				.product(name: "NIOSSL", package: "swift-nio-ssl"),
				.product(name: "NIOFoundationCompat", package: "swift-nio"),
				.product(name: "Logging", package: "swift-log"),
				.target(name: "SchemaTypes")
			]

		),
		.executableTarget(
			name: "PulsarExample",
			dependencies: [
				"Pulsar"
			],
			resources: [
				.process("Resources")
			]
		),
		.testTarget(
			name: "PulsarTests",
			dependencies: ["Pulsar"]
		),
		.target(name: "SchemaTypes")
	],
	swiftLanguageModes: [.v6]
)

for target in package.targets where target.type != .plugin {
	var settings = target.swiftSettings ?? []
	settings.append(.enableUpcomingFeature("MemberImportVisibility"))
	target.swiftSettings = settings
}
