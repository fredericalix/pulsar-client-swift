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
import NIOCore

enum BackoffStrategy {
	case exponential(initialDelay: TimeAmount, factor: Double, maxDelay: TimeAmount)

	func delay(forAttempt attempt: Int) -> TimeAmount {
		switch self {
			case let .exponential(initial, factor, max):
				let multiplier = pow(factor, Double(attempt))
				let computed = TimeAmount.nanoseconds(Int64(Double(initial.nanoseconds) * multiplier))
				return computed > max ? max : computed
		}
	}
}
