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

import NIOCore

/// Manages the state of any handler by tracking it.
class HandlerStateMap {
	var connectionPromise: EventLoopPromise<Void>?
	var correlationMap: [PulsarClientHandler.PromiseType: EventLoopPromise<Void>]
	var lookupState: (String?, Bool)?
	var context: ChannelHandlerContext?

	func getConnectionPromise() -> EventLoopPromise<Void>? {
		connectionPromise
	}

	init() {
		correlationMap = [:]
	}

	func setContext(_ context: ChannelHandlerContext) {
		self.context = context
	}

	func getContext() -> ChannelHandlerContext? {
		context
	}

	func addRedirectURL(_ url: String?, isAuthorative: Bool) {
		lookupState = (url, isAuthorative)
	}

	func removeRedirectURL() -> (String?, Bool)? {
		let returnURL = lookupState
		lookupState = nil
		return returnURL
	}

	func beginConnection(promise: EventLoopPromise<Void>) {
		connectionPromise = promise
	}

	func remove(promise: PulsarClientHandler.PromiseType) -> EventLoopPromise<Void>? {
		correlationMap.removeValue(forKey: promise)
	}

	func add(promise: PulsarClientHandler.PromiseType, promiseValue: EventLoopPromise<Void>) {
		correlationMap[promise] = promiseValue
	}
}
