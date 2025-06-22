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

// Supported Pulsar schemas by the library.
public enum PulsarSchema: String, Equatable, Sendable {
	case bytes
	case string
	case bool
	case int8
	case int16
	case int32
	case int64
	case float
	case double
	case date
	case time
	case timestamp
	case instant
	case localDate
	case localTime
	case localDateTime
}
