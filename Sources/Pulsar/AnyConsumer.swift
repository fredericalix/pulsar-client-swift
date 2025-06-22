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

protocol AnyConsumer: AsyncSequence, AnyObject {
	var consumerID: UInt64 { get }
	var autoAcknowledge: Bool { get }
	var topic: String { get }
	var subscriptionName: String { get }
	var subscriptionType: SubscriptionType { get }
	var subscriptionMode: SubscriptionMode { get }
	var schema: PulsarSchema { get }
	var stateManager: ConsumerStateHandler { get }
	func fail(error: Error)
	func handleMessasge(_ pulsarMessage: PulsarMessage)
	func handleClosing() async throws
}
