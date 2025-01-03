# pulsar-client-swift

[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fflexlixrup%2Fpulsar-client-swift%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/flexlixrup/pulsar-client-swift)
[![](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fflexlixrup%2Fpulsar-client-swift%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/flexlixrup/pulsar-client-swif)
[![License](https://img.shields.io/badge/License-Apache-gree.svg)](LICENSE)

> [!WARNING]
> This package is in early development and does **NOT** support many functionalities. Please do not use it in any critical environment.

## Overview

pulsar-client-swift provides a Swift native implementation of the [Apache Pulsar](https://pulsar.apache.org) Messaging Protocol as a client library. The package is written on top of [SwiftNIO](https://github.com/apple/swift-nio) to ensure a high-performance, non-blocking experience for the libary user. The client library does not necessarly expose any NIO components and can be worked with using standard Swift Concurrency Types.

## Requirements

Swift 6.0+

## Installation

### Swift Package Manager

To integrate `pulsar-client-swift` into your project using Swift Package Manager, follow these steps:

1. Open your project in Xcode.
2. Select `File` > `Swift Packages` > `Add Package Dependency...`.
3. Enter the package repository URL: `https://github.com/flexlixrup/pulsar-client-swift`.
4. Choose the latest release or specify a version range.
5. Add the package to your target.

Alternatively, you can add the following dependency to your `Package.swift` file:

```swift
dependencies: [
	.package(url: "https://github.com/flexlixrup/pulsar-client-swift", from: "0.0.1")
]
```

Then, include `Pulsar` as a dependency in your target:

```swift
.target(
	name: "YourTargetName",
	dependencies: [
		"Pulsar"
	]),
```

## Usage

The full documentation is provided via DocC on [Swift Package Manager](https://swiftpackageindex.com/flexlixrup/pulsar-client-swift).

## Contributing

> [!WARNING]
> This package uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to detect the semantic versioning. Commits not following this format will not be accepted.

If you would like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.

## License

This project is licensed under the Apache 2 License - see the [LICENSE](LICENSE) file for details.

## Contact

If you have any questions, feel free to open an issue.
