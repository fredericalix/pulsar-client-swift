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

extension Date {
	/// The ``PulsarTimestamp`` representation of the date.
	public var pulsarTimestamp: PulsarTimestamp {
		PulsarTimestamp(timestamp: Int64(timeIntervalSince1970 * 1000))
	}

	/// Initialize a Date with ``pulsarTimestamp``.
	/// - Parameter pulsarTimestamp: The object to initialize with.
	public init(pulsarTimestamp: PulsarTimestamp) {
		self = Date(timeIntervalSince1970: Double(pulsarTimestamp.pulsarDate) / 1000)
	}
}
