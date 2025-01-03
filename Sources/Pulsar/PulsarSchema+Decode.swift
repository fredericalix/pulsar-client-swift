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
import NIOCore

extension PulsarSchema {
	/// Decode the ByteBuffer into the concrete Swift type,
	/// based on the schema.
	func decodePayload(_ buffer: ByteBuffer) throws -> any PulsarPayload {
		switch self {
			case .bytes:
				return try Data.decode(from: buffer)
			case .string:
				return try String.decode(from: buffer)
			case .bool:
				return try Bool.decode(from: buffer)
			case .int8:
				return try Int8.decode(from: buffer)
			case .int16:
				return try Int16.decode(from: buffer)
			case .int32:
				return try Int32.decode(from: buffer)
			case .int64:
				return try Int64.decode(from: buffer)
			case .float:
				return try Float.decode(from: buffer)
			case .double:
				return try Double.decode(from: buffer)
			case .date,
				.time,
				.timestamp,
				.instant,
				.localDate,
				.localTime,
				.localDateTime:
				throw DecodingError.dataCorrupted(
					DecodingError.Context(
						codingPath: [],
						debugDescription: "Not implemented: decoding date/time types"
					)
				)
		}
	}
}
