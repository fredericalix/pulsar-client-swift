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

struct PulsarMessage {
	let command: Pulsar_Proto_BaseCommand
	let brokerEntryMetadata: Pulsar_Proto_BrokerEntryMetadata?
	var messageMetadata: Pulsar_Proto_MessageMetadata?
	let payload: ByteBuffer?

	init(command: Pulsar_Proto_BaseCommand, brokerEntryMetadata: Pulsar_Proto_BrokerEntryMetadata? = nil, messageMetadata: Pulsar_Proto_MessageMetadata? = nil, payload: ByteBuffer? = nil) {
		self.command = command
		self.brokerEntryMetadata = brokerEntryMetadata
		self.messageMetadata = messageMetadata
		self.payload = payload
	}
}
