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
import NIO
import NIOFoundationCompat

final class PulsarClientHandler: ChannelInboundHandler, @unchecked Sendable {
	let logger = Logger(label: "PulsarClientHandler")
	typealias InboundIn = PulsarMessage
	typealias OutboundOut = PulsarMessage

	let eventLoop: EventLoop
	let client: PulsarClient
	let connectionEstablished: EventLoopPromise<Void>

	init(eventLoop: EventLoop, client: PulsarClient) {
		let promise = eventLoop.makePromise(of: Void.self)
		connectionEstablished = promise
		self.eventLoop = eventLoop
		self.client = client
		connectionPromiseState = .inProgress(promise)
	}

	/// When - for some reason - the handler goes out of scope, fail the promise.
	/// This happens mostly when the TCP connection fails and the pipeline cleans up itself.
	deinit {
		connectionEstablished.fail(PulsarClientError.clientClosed)
	}

	/// Each consumer is tracked by an ID; storing the consumer object plus any relevant info
	var consumers: [UInt64: ConsumerCache] = [:]
	var producers: [UInt64: ProducerCache] = [:]

	enum PromiseType: Hashable {
		case generic(String)
		case id(UInt64)
		case send(producerID: UInt64, sequenceID: UInt64)
	}

	enum ConnectionState {
		case disconnected
		case inConnection
		case connected
	}

	private enum ConnectionPromiseState {
		case inProgress(EventLoopPromise<Void>)
		case completed
	}

	private var connectionPromiseState: ConnectionPromiseState

	let correlationMap = HandlerStateMap()

	// MARK: - Channel Lifecycle

	func channelActive(context: ChannelHandlerContext) {
		connect(context: context)
		correlationMap.context = context
	}

	func channelInactive(context: ChannelHandlerContext) {
		// inReconnection = true
		guard let ipAddress = context.channel.remoteAddress?.ipAddress else {
			context.fireChannelInactive()
			return
		}
		Task {
			await client.handleChannelInactive(ipAddress: ipAddress, handler: self)
		}

		// Continue normal pipeline behavior
		context.fireChannelInactive()
	}

	func channelRead(context: ChannelHandlerContext, data: NIOAny) {
		let message = unwrapInboundIn(data)

		do {
			switch message.command.type {
				case .connected:
					handleConnected(context: context, message: message.command.connected)
				case .ping:
					handlePing(context: context, message: message.command.ping)
				case .lookupResponse:
					handleLookupResponse(context: context, message: message.command.lookupTopicResponse)
				case .success:
					handleSuccess(context: context, message: message.command.success)
				case .message:
					handlePayloadMessage(context: context, message: message)
				case .closeConsumer:
					handleClosedConsumer(consumerID: message.command.closeConsumer.consumerID)
				case .producerSuccess:
					handleProducerSuccess(context: context, message: message.command.producerSuccess)
				case .sendReceipt:
					handleSendReceipt(context: context, message: message.command.sendReceipt)
				case .closeProducer:
					handleClosedProducer(producerID: message.command.closeProducer.producerID)
				case .error:
					// The server can return an Error command with a message inside
					let errorCmd = message.command.error
					try handleProtocolError(context: context, errorCmd)
				default:
					logger.debug("Unknown command \(message.command.type)")
					throw PulsarClientError.unsupportedMessageType
			}
		} catch {
			manageErrors(context: context, error: error)
		}
	}

	func manageErrors(context: ChannelHandlerContext, error: Error) {
		logger.error("Error during channelRead: \(error)")

		// Fail any big pending promise (like the connectionEstablished) if it’s not done
		switch connectionPromiseState {
			case let .inProgress(promise):
				promise.fail(error)
				connectionPromiseState = .completed
			case .completed:
				// Already completed; do nothing and log
				logger.debug("Connection promise was already completed; ignoring.")
		}
		logger.warning("Closing the channel, initiating reconnect")
		context.close(mode: .all, promise: nil)
		context.fireErrorCaught(error)
	}

	func makePromise(context: ChannelHandlerContext, type: PromiseType, forceClose: Bool = true) -> EventLoopPromise<Void> {
		let promise: EventLoopPromise<Void> = context.eventLoop.makePromise()
		context.eventLoop.scheduleTask(in: .seconds(5)) { [weak self] in
			guard let self else {
				return
			}
			correlationMap.context!.eventLoop.scheduleTask(in: .seconds(5)) {
				// We only want to fail the promise if it's still there.
				if let promise = self.correlationMap.remove(promise: type) {
					self.logger.error("Request timed out.")
					self.logger.trace("Failing promise for \(type)")
					promise.fail(PulsarClientError.connectionTimeout)
					if forceClose {
						self.correlationMap.context!.close(mode: .all, promise: nil)
					} else {
						self.logger.warning("Timeout on promise for \(type) but not closing connection")
					}
				} else {
					self.logger.trace("Promise for \(type) was already fullfilled.")
				}
				switch self.connectionPromiseState {
					case let .inProgress(eventLoopPromise):
						self.logger.error("Connection timed out.")
						eventLoopPromise.fail(PulsarClientError.connectionTimeout)
						self.correlationMap.context!.close(mode: .all, promise: nil)
					case .completed:
						break
				}
			}
		}
		return promise
	}

	// MARK: - Protocol Handlers

	func handleProtocolError(context: ChannelHandlerContext, _ errorCommand: Pulsar_Proto_CommandError) throws {
		let serverMsg = errorCommand.message
		logger.error("Server responded with error: \(serverMsg)")
		context.close(mode: .all, promise: nil)
		guard let ipAddress = context.channel.remoteAddress?.ipAddress else {
			context.fireChannelInactive()
			return
		}
		Task {
			await client.handleChannelInactive(ipAddress: ipAddress, handler: self)
		}
		// MANAGE errors seperately
		// Continue normal pipeline behavior
		context.fireErrorCaught(PulsarClientError.unknownError("Pulsar reported an error \(errorCommand.message)"))
	}

	func handlePing(context: ChannelHandlerContext, message _: Pulsar_Proto_CommandPing) {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .pong

		let pongCommand = Pulsar_Proto_CommandPong()
		baseCommand.pong = pongCommand
		let pulsarMessage = PulsarMessage(command: baseCommand)
		context.writeAndFlush(wrapOutboundOut(pulsarMessage), promise: nil)
	}

	func handleConnected(context _: ChannelHandlerContext, message _: Pulsar_Proto_CommandConnected) {
		// Only succeed if we’re still in progress
		switch connectionPromiseState {
			case let .inProgress(promise):
				logger.info("Connected channel.")
				promise.succeed(())
				connectionPromiseState = .completed
				if let promise = correlationMap.remove(promise: .generic("connect")) {
					promise.succeed()
				}
			// isAlreadyReconnecting = false
			case .completed:
				// Already completed; ignore and log
				logger.trace("Connection promise was already completed.")
		}
	}

	func handleSuccess(context _: ChannelHandlerContext, message: Pulsar_Proto_CommandSuccess) {
		if let promise = correlationMap.remove(promise: .id(message.requestID)) {
			logger.debug("Success for requestID \(message.requestID)")
			promise.succeed()
		}
	}

	/// If the server returned a message payload, deliver it to the correct consumer.
	func handlePayloadMessage(context: ChannelHandlerContext, message: PulsarMessage) {
		let msgCmd = message.command.message
		guard let receivingConsumerCache = consumers[msgCmd.consumerID] else {
			logger.error("Received a message for unknown consumerID \(msgCmd.consumerID)")
			return
		}
		let receivingConsumer = receivingConsumerCache.consumer
		if let payload = message.payload {
			receivingConsumer.continuation.yield(Message(payload: Data(buffer: payload)))
			receivingConsumerCache.messageCount += 1
		}
		if receivingConsumer.autoAcknowledge {
			acknowledge(context: context, message: message)
		}
		// Request more messages if we hit our threshold as per Pulsar Protocol
		if receivingConsumerCache.messageCount == 500 {
			flow(consumerID: receivingConsumerCache.consumerID)
		}
	}

	func topicLookupFailureType(context: ChannelHandlerContext, message: Pulsar_Proto_CommandLookupTopicResponse, promise: EventLoopPromise<Void>) {
		switch message.error {
			case .unknownError:
				logger.error("Unknown error occurred during topic lookup: \(message.message)")
				manageErrors(context: context, error: PulsarClientError.unknownError(message.message))

			case .metadataError:
				logger.warning("Metadata error occurred. Retrying lookup...")
				manageErrors(context: context, error: PulsarClientError.metadataError)

			case .persistenceError:
				logger.warning("Persistence error occurred. Retrying lookup...")
				manageErrors(context: context, error: PulsarClientError.persistenceError)

			case .authenticationError:
				logger.error("Authentication error occurred. Escalating to client.")
				promise.fail(PulsarClientError.authenticationError)

			case .authorizationError:
				logger.error("Authorization error occurred. Escalating to client.")
				promise.fail(PulsarClientError.authorizationError)

			case .consumerBusy:
				logger.warning("Consumer is busy. Retrying...")
				manageErrors(context: context, error: PulsarClientError.consumerBusy)

			case .serviceNotReady:
				logger.warning("Service not ready. Retrying...")
				manageErrors(context: context, error: PulsarClientError.serviceNotReady)

			case .producerBlockedQuotaExceededError,
			     .producerBlockedQuotaExceededException:
				logger.error("Producer blocked due to quota exceeded. Escalating to client.")
				promise.fail(PulsarClientError.producerBlocked)

			case .checksumError:
				logger.error("Checksum error occurred. Escalating to client.")
				promise.fail(PulsarClientError.checksumError)

			case .unsupportedVersionError:
				logger.error("Unsupported version error occurred. Escalating to client.")
				promise.fail(PulsarClientError.unsupportedVersion)

			case .topicNotFound:
				logger.error("Topic not found. Escalating to client.")
				promise.fail(PulsarClientError.topicNotFound)

			case .subscriptionNotFound:
				logger.error("Subscription not found. Escalating to client.")
				promise.fail(PulsarClientError.subscriptionNotFound)

			case .consumerNotFound:
				logger.error("Consumer not found. Escalating to client.")
				promise.fail(PulsarClientError.consumerNotFound)

			case .tooManyRequests:
				logger.warning("Too many requests. Retrying...")
				manageErrors(context: context, error: PulsarClientError.tooManyRequests)

			case .topicTerminatedError:
				logger.error("Topic has been terminated. Escalating to client.")
				promise.fail(PulsarClientError.topicTerminated)

			case .producerBusy:
				logger.warning("Producer is busy. Retrying...")
				manageErrors(context: context, error: PulsarClientError.producerBusy)

			case .invalidTopicName:
				logger.error("Invalid topic name. Escalating to client.")
				promise.fail(PulsarClientError.invalidTopicName)

			case .incompatibleSchema:
				logger.error("Incompatible schema error. Escalating to client.")
				promise.fail(PulsarClientError.incompatibleSchema)

			case .consumerAssignError:
				logger.warning("Consumer assignment error. Retrying...")
				manageErrors(context: context, error: PulsarClientError.consumerAssignError)

			case .transactionCoordinatorNotFound:
				logger.error("Transaction coordinator not found. Escalating to client.")
				promise.fail(PulsarClientError.transactionCoordinatorNotFound)

			case .invalidTxnStatus:
				logger.error("Invalid transaction status. Escalating to client.")
				promise.fail(PulsarClientError.invalidTxnStatus)

			case .notAllowedError:
				logger.error("Operation not allowed. Escalating to client.")
				promise.fail(PulsarClientError.notAllowed)

			case .transactionConflict:
				logger.error("Transaction conflict detected. Escalating to client.")
				promise.fail(PulsarClientError.transactionConflict)

			case .transactionNotFound:
				logger.error("Transaction not found. Escalating to client.")
				promise.fail(PulsarClientError.transactionNotFound)

			case .producerFenced:
				logger.error("Producer has been fenced. Escalating to client.")
				promise.fail(PulsarClientError.producerFenced)
		}
	}

	func handleLookupResponse(context: ChannelHandlerContext, message: Pulsar_Proto_CommandLookupTopicResponse) {
		guard let promise = correlationMap.remove(promise: .id(message.requestID)) else {
			logger.error("Lookup response received for unknown request ID \(message.requestID)")
			manageErrors(context: context, error: PulsarClientError.topicLookupFailed)
			return
		}

		if message.response == .connect {
			// Means we can connect directly
			correlationMap.addRedirectURL(nil, isAuthorative: message.authoritative)
		} else if message.response == .redirect {
			// Means we must re-connect to the brokerServiceURL
			correlationMap.addRedirectURL(message.brokerServiceURL, isAuthorative: message.authoritative)
		} else if message.response == .failed {
			topicLookupFailureType(context: context, message: message, promise: promise)
		}

		promise.succeed()
	}

	// MARK: - General methods

	func connect(context: ChannelHandlerContext) {
		logger.debug("Connecting...")
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .connect

		var connectCommand = Pulsar_Proto_CommandConnect()
		connectCommand.clientVersion = "Pulsar-Client-Swift-1.0.0"
		connectCommand.protocolVersion = 21
		baseCommand.connect = connectCommand

		let pulsarMessage = PulsarMessage(command: baseCommand)
		let promise = makePromise(context: context, type: .generic("connect"))
		correlationMap.add(promise: .generic("connect"), promiseValue: promise)
		correlationMap.beginConnection(promise: promise)

		context.writeAndFlush(wrapOutboundOut(pulsarMessage), promise: nil)
	}

	// MARK: - Topic Lookup

	/// Looks up the topic. If the broker says “redirect,” we store that in `correlationMap` and succeed the promise.
	/// The caller (PulsarClient) will check `removeRedirectURL()` to see if we need to connect elsewhere.
	func topicLookup(topic: String, authorative: Bool) async throws -> (String?, Bool) {
		var baseCommand = Pulsar_Proto_BaseCommand()
		baseCommand.type = .lookup
		var lookupCmd = Pulsar_Proto_CommandLookupTopic()
		lookupCmd.topic = topic
		lookupCmd.authoritative = authorative
		let requestID = UInt64.random(in: 0 ..< UInt64.max)
		lookupCmd.requestID = requestID
		baseCommand.lookupTopic = lookupCmd
		let pulsarMessage = PulsarMessage(command: baseCommand)
		// Send the lookup command on the event loop
		try await correlationMap.context!.eventLoop.submit {
			self.correlationMap.context!.writeAndFlush(self.wrapOutboundOut(pulsarMessage), promise: nil)
		}.get()

		// Create & store a promise so we know when the server responds
		let promise = makePromise(context: correlationMap.context!, type: .id(requestID))
		correlationMap.add(promise: .id(requestID), promiseValue: promise)
		do {
			// Wait for the server’s response or error
			try await promise.futureResult.get()

			// Did the server instruct us to redirect?
			let optionalRedirectResponse = correlationMap.removeRedirectURL()
			if let redirectResponse = optionalRedirectResponse, let redirectURL = redirectResponse.0 {
				return (redirectURL, redirectResponse.1)
			} else if let redirectResponse = optionalRedirectResponse {
				// Means server responded with “use the same connection”
				return ("", redirectResponse.1)
			} else {
				throw PulsarClientError.internalError("This should never throw for the user of the library. If it does, please open an issue at https://github.com/flexlixrup/pulsar-client-swift/issues")
			}
		} catch {
			throw PulsarClientError.connectionError
		}
	}
}
