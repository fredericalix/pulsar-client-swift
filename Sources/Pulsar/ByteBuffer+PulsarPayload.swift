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

extension ByteBuffer {
	init<T: PulsarPayload>(pulsarPayload: T) throws {
		let allocator = ByteBufferAllocator()
		var byteBuffer = allocator.buffer(capacity: MemoryLayout<T>.size)
		switch pulsarPayload {
			case let value as Int8:
				byteBuffer.writeInteger(value)
			case let value as Int16:
				byteBuffer.writeInteger(value)
			case let value as Int32:
				byteBuffer.writeInteger(value)
			case let value as Int64:
				byteBuffer.writeInteger(value)
			case let value as UInt8:
				byteBuffer.writeInteger(value)
			case let value as UInt16:
				byteBuffer.writeInteger(value)
			case let value as UInt32:
				byteBuffer.writeInteger(value)
			case let value as UInt64:
				byteBuffer.writeInteger(value)
			case let value as Float:
				byteBuffer.writeInteger(value.bitPattern)
			case let value as Double:
				byteBuffer.writeInteger(value.bitPattern)
			case let value as Bool:
				byteBuffer.writeInteger(value ? 1 : 0, as: UInt8.self)
			case let value as String:
				byteBuffer.writeString(value)
			case let value as Data:
				byteBuffer.writeBytes(value)
			default:
				throw EncodingError.invalidValue(
					pulsarPayload,
					EncodingError.Context(codingPath: [], debugDescription: "Unsupported PulsarPayload type")
				)
		}
		self = byteBuffer
	}
}
