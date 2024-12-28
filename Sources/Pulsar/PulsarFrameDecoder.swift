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
import NIOFoundationCompat

final class PulsarFrameDecoder: ByteToMessageDecoder {
	typealias InboundIn = ByteBuffer
	typealias InboundOut = PulsarMessage
	let logger = Logger(label: "PulsarFrameDecoder")

	func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
		logger.trace("Received frame")
		let startIndex = buffer.readerIndex

		// Ensure at least 4 bytes are readable for totalSize
		guard buffer.readableBytes >= 4 else {
			buffer.moveReaderIndex(to: startIndex)
			return .needMoreData
		}

		guard let totalLength = buffer.readInteger(as: UInt32.self) else {
			throw PulsarClientError.invalidFrame
		}

		// Check if the entire frame is available
		guard buffer.readableBytes >= totalLength else {
			buffer.moveReaderIndex(to: startIndex)
			return .needMoreData
		}

		guard let commandSize = buffer.readInteger(as: UInt32.self) else {
			throw PulsarClientError.invalidFrame
		}

		guard let commandData = buffer.readSlice(length: Int(commandSize)) else {
			throw PulsarClientError.invalidFrame
		}

		let baseCommand = try Pulsar_Proto_BaseCommand(serializedBytes: Data(buffer: commandData))

		let restOfMessage = totalLength - commandSize - 4 // Subtract commandSize field size
		if restOfMessage > 0 {
			// Handle messages with payload
			var brokerEntryMetadata: Pulsar_Proto_BrokerEntryMetadata?
			// Peek at the next 2 bytes to check for brokerEntryMetadata magic number
			guard buffer.readableBytes >= 2 else {
				buffer.moveReaderIndex(to: startIndex)
				return .needMoreData
			}

			let possibleMagicNumber = buffer.getInteger(at: buffer.readerIndex, as: UInt16.self)

			if possibleMagicNumber == 0x0E02 {
				// Read magicNumberOfBrokerEntryMetadata
				guard let _ = buffer.readInteger(as: UInt16.self) else {
					throw PulsarClientError.invalidFrame
				}

				// Read brokerEntryMetadataSize
				guard let brokerEntryMetadataSize = buffer.readInteger(as: UInt32.self) else {
					throw PulsarClientError.invalidFrame
				}

				// Read brokerEntryMetadata
				guard let brokerEntryMetadataData = buffer.readSlice(length: Int(brokerEntryMetadataSize)) else {
					throw PulsarClientError.invalidFrame
				}

				// Parse brokerEntryMetadata
				brokerEntryMetadata = try Pulsar_Proto_BrokerEntryMetadata(serializedBytes: Data(buffer: brokerEntryMetadataData))
			}

			// Read magicNumber (0x0e01)
			guard let magicNumber = buffer.readInteger(as: UInt16.self), magicNumber == 0x0E01 else {
				throw PulsarClientError.invalidFrame
			}

			// Read checksum
			guard let checksum = buffer.readInteger(as: UInt32.self) else {
				throw PulsarClientError.invalidFrame
			}

			let checksumStartIndex = buffer.readerIndex

			// Calculate the length of data to be checksummed
			let frameEndIndex = startIndex + 4 + Int(totalLength)
			let checksumDataLength = frameEndIndex - checksumStartIndex

			// Ensure the checksum data is available
			guard buffer.readableBytes >= checksumDataLength else {
				buffer.moveReaderIndex(to: startIndex)
				return .needMoreData
			}

			// Get the data to be checksummed
			guard let checksumData = buffer.getSlice(at: checksumStartIndex, length: checksumDataLength) else {
				throw PulsarClientError.invalidFrame
			}

			// Compute and verify checksum
			let computedChecksum = CRC32C.checksum(checksumData)
			guard computedChecksum == checksum else {
				throw PulsarClientError.checksumMismatch
			}

			// Read metadataSize
			guard let metadataSize = buffer.readInteger(as: UInt32.self) else {
				throw PulsarClientError.invalidFrame
			}

			// Read metadata
			guard let metadataData = buffer.readSlice(length: Int(metadataSize)) else {
				throw PulsarClientError.invalidFrame
			}

			let messageMetadata = try Pulsar_Proto_MessageMetadata(serializedBytes: Data(buffer: metadataData))

			// Read payload
			let payloadLength = frameEndIndex - buffer.readerIndex
			guard payloadLength >= 0 else {
				throw PulsarClientError.invalidFrame
			}

			guard let payloadData = buffer.readSlice(length: payloadLength) else {
				throw PulsarClientError.invalidFrame
			}

			// Create PulsarMessage
			let message = PulsarMessage(
				command: baseCommand,
				brokerEntryMetadata: brokerEntryMetadata,
				messageMetadata: messageMetadata,
				payload: payloadData
			)

			logger.trace("Decoded message with payload: \(message)")
			context.fireChannelRead(wrapInboundOut(message))
			return .continue
		} else {
			// Handle messages without payload
			let message = PulsarMessage(command: baseCommand, brokerEntryMetadata: nil, messageMetadata: nil, payload: nil)
			logger.trace("Decoded message: \(message)")
			context.fireChannelRead(wrapInboundOut(message))
			return .continue
		}
	}
}
