# Supported features

The library is not (yet) complete. This section provides an overview of the supported features of the library.

## Client Features

| Feature                         | Supported | Notes                                         |
|---------------------------------|-----------|-----------------------------------------------|
| **Authentication**              |           |                                               |
| - Token-based Authentication    | No    |                                               |
| - TLS Authentication            | No    |                                               |
| - OAuth2 Authentication         | No    |                                               |
| **Encryption**                  |           |                                               |
| - End-to-End Encryption         | No    |                                               |
| **Connection Handling**         |           |                                               |
| - Automatic Reconnection        | Yes   |                                               |
| - Connection Timeout Config     | No    |                                               |
| **Logging and Metrics**         |           |                                               |
| - Built-in Logging              | Yes    |                                               |
| - Metrics Collection            | No    |                                               |

## Producer Features

| Feature                         | Supported | Notes                                         |
|---------------------------------|-----------|-----------------------------------------------|
| **Message Routing**             |           |                                               |
| - Key-based Routing             | No    |                                               |
| - Custom Partitioning           | No    |                                               |
| **Message Delivery**            |           |                                               |
| - Synchronous Send              | Yes    |                                               |
| - Asynchronous Send             | Yes    |                                               |
| - Batch Message Publishing      | No    |                                               |
| **Compression**                 |           |                                               |
| - LZ4 Compression               | No    |                                               |
| - ZLIB Compression              | No    |                                               |
| - ZSTD Compression              | No    |                                               |
| - SNAPPY Compression            | No    |                                               |
| **Message TTL**                 |           |                                               |
| - Set per Message               | No    |                                               |
| **Delayed Delivery**            |           |                                               |
| - Deliver After Delay           | No    |                                               |
| - Deliver at Specific Time      | No    |                                               |

## Consumer Features

| Feature                         | Supported | Notes                                         |
|---------------------------------|-----------|-----------------------------------------------|
| **Subscription Types**          |           |                                               |
| - Exclusive                     | Yes    |                                               |
| - Shared                        | Yes    |                                               |
| - Failover                      | Yes    |                                               |
| - Key_Shared                    | Yes    |                                               |
| **Message Acknowledgment**      |           |                                               |
| - Individual Acknowledgment     | Yes    |                                               |
| - Cumulative Acknowledgment     | No    |                                               |
| - Negative Acknowledgment       | No    |                                               |
| **Batch Message Consumption**   |           |                                               |
| - Batch Receive                 | No    |                                               |
| **Dead Letter Policy**          |           |                                               |
| - Dead Letter Topic Support     | No    |                                               |
| **Message Redelivery**          |           |                                               |
| - Delayed Redelivery            | No    |                                               |
| - Max Redelivery Attempts       | No    |                                               |

## Reader Features (Not implemented)

| Feature                         | Supported | Notes                                         |
|---------------------------------|-----------|-----------------------------------------------|
| **Message Reading**             |           |                                               |
| - Start from Specific MessageID | No    |                                               |
| - Start from Timestamp          | No    |                                               |
| **Non-Durable Subscriptions**   |       |                                               |
| - Ephemeral Readers             | No    |                                               |

## TableView Features (Not implemented)

| Feature                         | Supported | Notes                                         |
|---------------------------------|-----------|-----------------------------------------------|
| **Key-Value Access**            |           |                                               |
| - Real-time Key-Value Updates   | No    |                                               |
| - Snapshot of Latest Key-Values | No    |                                               |
