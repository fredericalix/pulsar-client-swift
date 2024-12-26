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

struct CRC32C {
	private static func generateCRC32CTable() -> [UInt32] {
		let polynomial: UInt32 = 0x82F6_3B78
		var table = [UInt32](repeating: 0, count: 256)

		for i in 0 ..< 256 {
			var crc = UInt32(i)
			for _ in 0 ..< 8 {
				if crc & 1 != 0 {
					crc = (crc >> 1) ^ polynomial
				} else {
					crc >>= 1
				}
			}
			table[i] = crc
		}
		return table
	}

	static func checksum(_ data: Data) -> UInt32 {
		let table = generateCRC32CTable()
		var crc: UInt32 = 0xFFFF_FFFF

		for byte in data {
			let index = Int((crc ^ UInt32(byte)) & 0xFF)
			crc = (crc >> 8) ^ table[index]
		}

		return crc ^ 0xFFFF_FFFF
	}
}
