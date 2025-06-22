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

// The errors thrown by the library.
public enum PulsarClientError: Error, Equatable {
	case clientClosed
	case networkError
	case topicLookupFailed
	case consumerClosed
	case unsupportedMessageType
	case promiseNotFound
	case serverError(String)
	case unknownError(String)
	case invalidFrame
	case connectionTimeout
	case internalError(String)
	case checksumMismatch
	case consumerFailed
	case producerFailed
	case connectionError
	case metadataError
	case persistenceError
	case authenticationError
	case authorizationError
	case consumerBusy
	case serviceNotReady
	case producerBlocked
	case checksumError
	case unsupportedVersion
	case topicNotFound
	case subscriptionNotFound
	case consumerNotFound
	case tooManyRequests
	case topicTerminated
	case producerBusy
	case closedByUser
	case invalidTopicName
	case incompatibleSchema
	case consumerAssignError
	case transactionCoordinatorNotFound
	case invalidTxnStatus
	case notAllowed
	case transactionConflict
	case transactionNotFound
	case producerFenced
	case noTLSProvided

	static func isUserHandledError(_ error: any Error) -> Bool {
		if let error = error as? PulsarClientError {
			switch error {
				case .authenticationError,
					.authorizationError,
					.producerBlocked,
					.checksumError,
					.unsupportedVersion,
					.topicNotFound,
					.subscriptionNotFound,
					.consumerNotFound,
					.topicTerminated,
					.invalidTopicName,
					.incompatibleSchema,
					.transactionCoordinatorNotFound,
					.invalidTxnStatus,
					.notAllowed,
					.transactionConflict,
					.transactionNotFound,
					.producerFenced:
					true
				default: false
			}
		} else {
			false
		}
	}
}
