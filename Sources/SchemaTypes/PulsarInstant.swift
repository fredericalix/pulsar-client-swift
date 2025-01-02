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

/// The corresponding Swift type for a Pulsar Schema with type `instant`.
public struct PulsarInstant: Sendable {
	public let seconds: Int64
	public let nanos: Int32

	public init(seconds: Int64, nanos: Int32) {
		self.seconds = seconds
		self.nanos = nanos
	}

	public init(date: Date) {
		let totalSeconds = Int64(date.timeIntervalSince1970)
		let nanos = Int32((date.timeIntervalSince1970 - Double(totalSeconds)) * 1_000_000_000)
		self.seconds = totalSeconds
		self.nanos = nanos
	}

	public var date: Date {
		Date(timeIntervalSince1970: Double(seconds) + Double(nanos) / 1_000_000_000)
	}
}
