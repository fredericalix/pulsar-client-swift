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

/// The corresponding Swift type for a Pulsar Schema with type `timestamp`.
public struct PulsarTimestamp: Sendable {
	public let pulsarDate: Int64

	public var swiftDate: Date {
		.init(timeIntervalSince1970: Double(pulsarDate))
	}
	public init(date: Date) {
		pulsarDate = date.pulsarTimestamp.pulsarDate
	}

	public init(timestamp: Int64) {
		self.pulsarDate = timestamp
	}
}
/// The corresponding Swift type for a Pulsar Schema with type `Date`.
public typealias PulsarDate = PulsarTimestamp

/// The corresponding Swift type for a Pulsar Schema with type `Time`.
public typealias PulsarTime = PulsarTimestamp
