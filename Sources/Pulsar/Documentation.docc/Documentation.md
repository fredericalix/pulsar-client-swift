# ``Pulsar``

Apache Pulsar client library written natively in Swift.

## Overview

``Pulsar`` provides a Swift native implementation of the [Apache Pulsar](https://pulsar.apache.org) Messaging Protocol as a client library. The package is written on top of [SwiftNIO](https://github.com/apple/swift-nio) to ensure a high-performance, non-blocking experience for the libary user. The client library does not necessarly expose any NIO components and can be worked with using standard Swift Concurrency Types.
This documentation does not provide a comprehensive overview of the Pulsr functionalities itself. Please see [the official docs](https://pulsar.apache.org/docs/4.0.x/) for this information.

## Topics

### Essentials

- <doc:HowToUse>
- <doc:SupportedFeatures>

### Core Components

- ``PulsarClient``
- ``PulsarConsumer``
