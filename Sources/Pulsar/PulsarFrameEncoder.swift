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
import Logging
import NIOCore

final class PulsarFrameEncoder: MessageToByteEncoder {
	typealias OutboundIn = PulsarMessage
	typealias OutboundOut = ByteBuffer
	let logger = Logger(label: "PulsarFrameEncoder")

	func encode(data: PulsarMessage, out: inout ByteBuffer) throws {
		logger.trace("Encoding \(data.command)")

		// Serialize the command
		let commandData = try data.command.serializedData()
		let commandSize = UInt32(commandData.count)

		// Start building the frame
		var frameBuffer = ByteBufferAllocator().buffer(capacity: 1024)

		// Write commandSize and command
		frameBuffer.writeInteger(commandSize)
		frameBuffer.writeBytes(commandData)

		// Handle optional brokerEntryMetadata
		if let brokerEntryMetadata = data.brokerEntryMetadata {
			// Write magicNumberOfBrokerEntryMetadata (0x0e02)
			frameBuffer.writeInteger(UInt16(0x0E02))

			// Serialize brokerEntryMetadata
			let brokerEntryMetadataData = try brokerEntryMetadata.serializedData()
			let brokerEntryMetadataSize = UInt32(brokerEntryMetadataData.count)

			// Write brokerEntryMetadataSize and brokerEntryMetadata
			frameBuffer.writeInteger(brokerEntryMetadataSize)
			frameBuffer.writeBytes(brokerEntryMetadataData)
		}

		// If there's a payload, write magic number, checksum, metadata, and payload
		if let messageMetadata = data.messageMetadata, let payload = data.payload {
			// Write magicNumber (0x0e01)
			frameBuffer.writeInteger(UInt16(0x0E01))

			// Reserve space for checksum (we'll come back and fill this in later)
			let checksumIndex = frameBuffer.writerIndex
			frameBuffer.writeInteger(UInt32(0)) // Placeholder for checksum

			// Start checksum computation from here
			let checksumStartIndex = frameBuffer.writerIndex

			// Serialize messageMetadata
			let metadataData = try! messageMetadata.serializedData()
			let metadataSize = UInt32(metadataData.count)

			// Write metadataSize and metadata
			frameBuffer.writeInteger(metadataSize)
			frameBuffer.writeBytes(metadataData)

			// Write payload
			frameBuffer.writeBytes(payload.readableBytesView)

			// Compute checksum over everything after checksum field
			let checksumData = frameBuffer.getSlice(at: checksumStartIndex, length: frameBuffer.writerIndex - checksumStartIndex)!
			let computedChecksum = CRC32C.checksum(checksumData)

			// Write the checksum into the reserved space
			frameBuffer.setInteger(computedChecksum, at: checksumIndex)
		}

		// Calculate totalSize (size of everything after totalSize field)
		let totalSize = UInt32(frameBuffer.readableBytes)

		// Write totalSize at the beginning of the out buffer
		out.writeInteger(totalSize)

		// Append the frameBuffer to out
		out.writeBuffer(&frameBuffer)

		logger.trace("Encoded message of total size \(totalSize) bytes")
	}
}
