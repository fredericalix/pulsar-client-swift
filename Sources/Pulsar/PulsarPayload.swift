// Copyright 2024 Felix Ruppert
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
import NIOFoundationCompat

public protocol PulsarPayload: Codable, Sendable {
	func encode() -> ByteBuffer
	static func decode(from buffer: ByteBuffer) throws -> Self
}

extension Data: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Data {
		return Data(buffer: buffer)
	}
}

extension String: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> String {
		var buffer = buffer
		guard let string = buffer.readString(length: buffer.readableBytes) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode String"))
		}
		return string
	}
}

extension Bool: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Bool {
		var buffer = buffer
		guard let value = buffer.readInteger(as: UInt8.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Bool"))
		}
		return value != 0
	}
}

extension Int8: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Int8 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int8.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int8"))
		}
		return value
	}
}

extension Int16: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Int16 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int16.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int16"))
		}
		return value
	}
}

extension Int32: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	public static func decode(from buffer: ByteBuffer) throws -> Int32 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int32.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int32"))
		}
		return value
	}
}

extension Int64: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Int64 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int64.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int64"))
		}
		return value
	}
}

extension Float: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Float {
		var buffer = buffer
		guard let bitPattern = buffer.readInteger(as: UInt32.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Float"))
		}
		return Float(bitPattern: bitPattern)
	}
}

extension Double: PulsarPayload {
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	public static func decode(from buffer: ByteBuffer) throws -> Double {
		var buffer = buffer
		guard let bitPattern = buffer.readInteger(as: UInt64.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Double"))
		}
		return Double(bitPattern: bitPattern)
	}
}
