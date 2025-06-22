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
import NIOCore
import Logging

extension PulsarClient {
	func handleChannelInactive(ipAddress: String, handler: PulsarClientHandler) async {
		let remoteAddress = handler.host
		connectionPool.removeValue(forKey: remoteAddress)

		if isReconnecting.contains(remoteAddress) {
			logger.info("Already reconnecting to \(remoteAddress). Skipping.")
			return
		}

		let oldConsumers = handler.consumers
		let oldProducers = handler.producers
		logger.warning("Channel inactive for \(remoteAddress). Initiating reconnection...")
		isReconnecting.insert(remoteAddress)

		let backoff = BackoffStrategy.exponential(
			initialDelay: .seconds(1),
			factor: 2.0,
			maxDelay: .seconds(30)
		)

		var attempt = 0
		while true {
			attempt += 1
			do {
				logger.info("Reconnection attempt #\(attempt) of \(reconnectLimit ?? Int.max) to \(remoteAddress):\(port)")

				try await connect(host: remoteAddress, port: port)

				// Reattach consumers after reconnecting
				try await reattachConsumers(oldConsumers: oldConsumers, host: remoteAddress)
				try await reattachProducers(oldProducers: oldProducers, host: remoteAddress)
				isReconnecting.remove(remoteAddress)
				logger.info("Reconnected to \(remoteAddress) after \(attempt) attempt(s).")
				break
			} catch {
				if let reconnectLimit {
					if attempt >= reconnectLimit {
						do {
							try await close()
						} catch {
							if let error = error as? PulsarClientError {
								switch error {
									case .clientClosed:
										break
									default:
										logger.critical("Closing client failed. Continuing from here has undefined behavior.")
								}
							}
						}
						break
					}
				}
				logger.error("Reconnection attempt #\(attempt) to \(remoteAddress) failed: \(error)")
				let delay = backoff.delay(forAttempt: attempt)
				logger.warning("Will retry in \(Double(delay.nanoseconds) / 1_000_000_000) second(s).")
				try? await Task.sleep(nanoseconds: UInt64(delay.nanoseconds))
			}
		}
	}

	private nonisolated func reattachProducers(
		oldProducers: [UInt64: ProducerCache],
		host: String
	) async throws {
		guard await connectionPool[host] != nil else {
			throw PulsarClientError.topicLookupFailed
		}
		logger.debug("Re-attaching \(oldProducers.count) producers...")
		for (_, producerCache) in oldProducers {
			let oldProducer = producerCache.producer
			let topic = oldProducer.topic

			logger.info("Reconnection producerID \(producerCache.producerID) to topic \(topic)")

			do {
				try await oldProducer.handleClosing()

			} catch {
				logger.error("Failed to re-attach producer for topic \(topic): \(error)")

				// Let the producer close if there is an error which should be handled by the library owner
				if PulsarClientError.isUserHandledError(error) {
					try oldProducer.onClosed?(error)
				}
				throw PulsarClientError.producerFailed
			}
		}
	}

	private nonisolated func reattachConsumers(
		oldConsumers: [UInt64: ConsumerCache],
		host: String
	) async throws {
		guard await connectionPool[host] != nil else {
			throw PulsarClientError.topicLookupFailed
		}
		logger.debug("Re-attaching \(oldConsumers.count) consumers...")
		for (_, consumerCache) in oldConsumers {
			let oldConsumer = consumerCache.consumer
			let topic = oldConsumer.topic

			logger.info("Re-subscribing consumerID \(consumerCache.consumerID) for topic \(topic)")

			do {
				try await oldConsumer.handleClosing()

			} catch {
				logger.error("Failed to re-subscribe consumer for topic \(topic): \(error)")

				// Let the consumer stream throw if the error has to be handled by the user. We don't need an onClosed there because the stream can already fail on its own.
				if PulsarClientError.isUserHandledError(error) {
					oldConsumer.fail(error: error)
				}
				throw PulsarClientError.consumerFailed
			}
		}
	}
}
