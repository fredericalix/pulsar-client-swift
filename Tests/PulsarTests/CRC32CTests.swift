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

import NIOCore
import NIOFoundationCompat
import Testing

@testable import Pulsar

@Suite("CRC32C Tests")
struct CRC32CTests {
	@Test("Test checksum")
	func testEncoding() throws {
		let data = ByteBuffer(data: "Hello World".data(using: .utf8)!)
		let checksum = CRC32C.checksum(data)
		#expect(checksum == 0x691D_AA2F)
	}

	@Test("Very long checksum")
	func testEncodingLong() throws {
		let data = ByteBuffer(
			data:
				"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et"
				.data(using: .utf8)!
		)
		let checksum = CRC32C.checksum(data)
		#expect(checksum == 0xA37A_B75F)
	}
}
