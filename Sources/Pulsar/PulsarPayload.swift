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
import NIOFoundationCompat
import SchemaTypes

/// The Pulsar Payload protocol enables types to be used as a payload of a Pulsar Message.
public protocol PulsarPayload: Sendable {
	func encode() -> ByteBuffer
	static func decode(from buffer: ByteBuffer) throws -> Self
}

extension PulsarInstant: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		var buffer = ByteBufferAllocator().buffer(capacity: 12)
		buffer.writeInteger(seconds)
		buffer.writeInteger(nanos)
		return buffer
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> PulsarInstant {
		var mutableBuffer = buffer
		guard let seconds = mutableBuffer.readInteger(as: Int64.self) else {
			throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Failed to decode seconds."))
		}
		guard let nanos = mutableBuffer.readInteger(as: Int32.self) else {
			throw DecodingError.dataCorrupted(.init(codingPath: [], debugDescription: "Failed to decode nanos."))
		}
		return PulsarInstant(seconds: seconds, nanos: nanos)

	}

}

extension PulsarTimestamp: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		pulsarDate.encode()
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> PulsarTimestamp {
		let timestamp = try Int64.decode(from: buffer)
		return PulsarTimestamp(timestamp: timestamp)
	}

}

extension Data: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Data {
		Data(buffer: buffer)
	}
}

extension String: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> String {
		var buffer = buffer
		guard let string = buffer.readString(length: buffer.readableBytes) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode String"))
		}
		return string
	}
}

extension Bool: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Bool {
		var buffer = buffer
		guard let value = buffer.readInteger(as: UInt8.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Bool"))
		}
		return value != 0
	}
}

extension Int8: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Int8 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int8.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int8"))
		}
		return value
	}
}

extension Int16: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Int16 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int16.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int16"))
		}
		return value
	}
}

extension Int32: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Int32 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int32.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int32"))
		}
		return value
	}
}

extension Int64: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Int64 {
		var buffer = buffer
		guard let value = buffer.readInteger(as: Int64.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Int64"))
		}
		return value
	}
}

extension Float: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}
	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Float {
		var buffer = buffer
		guard let bitPattern = buffer.readInteger(as: UInt32.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Float"))
		}
		return Float(bitPattern: bitPattern)
	}
}

extension Double: PulsarPayload {
	/// Encode into a ByteBuffer.
	/// - Returns: The ByteBuffer.
	public func encode() -> ByteBuffer {
		try! ByteBuffer(pulsarPayload: self)
	}

	/// Decode from a ByteBuffer.
	/// - Parameter buffer: The buffer to decode.
	/// - Returns: The decoded type.
	/// - Throws: DecodingError.dataCorrupted when the decoding fails.
	public static func decode(from buffer: ByteBuffer) throws -> Double {
		var buffer = buffer
		guard let bitPattern = buffer.readInteger(as: UInt64.self) else {
			throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "Failed to decode Double"))
		}
		return Double(bitPattern: bitPattern)
	}
}
