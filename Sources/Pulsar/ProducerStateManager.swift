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

actor ProducerStateManager {
	var producerName: String?
	var sequenceID: Int64 = -1
	var handler: PulsarClientHandler?

	func setProducerName(_ producerName: String?) {
		self.producerName = producerName
	}

	func setSequenceID(_ sequenceID: Int64) {
		self.sequenceID = sequenceID
	}

	func setHandler(_ handler: PulsarClientHandler?) {
		self.handler = handler
	}

	func getHandler() -> PulsarClientHandler {
		handler!
	}

	func getProducerName() -> String? {
		producerName
	}

	@discardableResult
	func increaseSequenceID() -> Int64 {
		sequenceID += 1
		return sequenceID
	}
}
