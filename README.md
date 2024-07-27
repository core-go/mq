# mq
![Message Queue](https://cdn-images-1.medium.com/max/800/1*UbKJu2BcAYim8_oJg8Ns6A.png)
## Message Queue
- A message queue is a communication method used in software systems to exchange information between different components or services asynchronously. It provides a way to send messages between producers (senders) and consumers (receivers) without requiring both parties to interact with the message queue at the same time. This decoupling allows for more scalable, reliable, and flexible system architectures.

### Key Concepts of Message Queues
#### Producers (Publishers/Senders/Writers)
- The components or services that send messages to the queue.
#### Consumers (Subscriber/Receivers/Readers)
- The components or services that receive and process messages from the queue.
#### Messages
- The data or payload that is sent by the producer and processed by the consumer. Messages can contain various types of information, such as text, binary data, or structured data like JSON or XML.
#### Queues
- Data structures that store messages until they are processed by consumers. Queues typically follow a FIFO (First In, First Out) principle, but other ordering mechanisms can also be implemented
#### Brokers
- Middleware components that manage the queues, handle the routing of messages, and ensure reliable delivery.
- Examples include RabbitMQ, Apache Kafka, Amazon SQS, Google Pub/Sub, NATS, Active MQ and IBM MQ.

## Message Queue Implementations
### RabbitMQ
- An open-source message broker that supports multiple messaging protocols. It provides features like message routing, persistence, and acknowledgment.
- RabbitMQ GO library is at [rabbitmq](https://github.com/core-go/rabbitmq), to wrap and simplify [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go). The sample is at [go-rabbit-mq-sample](https://github.com/project-samples/go-rabbit-mq-sample)
- RabbitMQ nodejs library is at [rabbitmq-ext](https://www.npmjs.com/package/rabbitmq-ext), to wrap and simplify [amqplib](https://www.npmjs.com/package/amqplib). The sample is at [rabbitmq-sample](https://github.com/typescript-tutorial/rabbitmq-sample)
### Apache Kafka
- A distributed streaming platform that handles high-throughput, low-latency message processing. It is often used for building real-time data pipelines and streaming applications.
- Kafka GO library is at [kafka](https://github.com/core-go/kafka), to wrap and simplify 3 Kafka GO libraries: [segmentio/kafka-go](https://github.com/segmentio/kafka-go), [IBM/sarama](https://github.com/IBM/sarama) and [confluent](https://github.com/confluentinc/confluent-kafka-go). The sample is at [go-kafka-sample](https://github.com/project-samples/go-kafka-sample)
- Kafka nodejs library is at [kafka](https://www.npmjs.com/package/kafka-plus), to wrap and simplify [kafkajs](https://www.npmjs.com/package/kafkajs). The sample is at [kafka-sample](https://github.com/typescript-tutorial/kafka-sample)
### Amazon SQS (Simple Queue Service)
- A fully managed message queue service offered by AWS. It provides a reliable, scalable, and cost-effective way to decouple and coordinate distributed software systems and microservices.
- SQS GO library is at [sqs](https://github.com/core-go/sqs), to wrap and simplify [aws-sdk-go/service/sqs](https://github.com/aws/aws-sdk-go/tree/main/service/sqs). The sample is at [go-amazon-sqs-sample](https://github.com/project-samples/go-amazon-sqs-sample)
### Google Cloud Pub/Sub
- A fully managed messaging service that allows for event-driven systems and real-time analytics on Google Cloud Platform.
- Pub/Sub GO library is at [core-go/pubsub](https://github.com/core-go/pubsub), to wrap and simplify [go/pubsub](https://pkg.go.dev/cloud.google.com/go/pubsub). The sample is at [go-pubsub-sample](https://github.com/project-samples/go-pubsub-sample)
- Pub/Sub nodejs library is at [google-pubsub](https://www.npmjs.com/package/google-pubsub), to wrap and simplify [@google-cloud/pubsub](https://www.npmjs.com/package/@google-cloud/pubsub). The sample is at [pubsub-sample](https://github.com/typescript-tutorial/pubsub-sample)
### IBM MQ
- IBM MQ at [ibmmq-plus](https://www.npmjs.com/package/ibmmq-plus), to wrap and simplify [ibmmq](https://github.com/ibm-messaging/mq-golang). The sample is at [go-ibm-mq-sample](https://github.com/project-samples/go-ibm-mq-sample)
- IBM MQ nodejs library is at [ibmmq-plus](https://www.npmjs.com/package/ibmmq-plus), to wrap and simplify [ibmmq](https://www.npmjs.com/package/ibmmq). The sample is at [ibmmq-sample](https://github.com/typescript-tutorial/ibmmq-sample)
### Active MQ
- Active MQ at [activemq](https://github.com/core-go/activemq), to wrap and simplify [go-stomp](https://github.com/go-stomp/stomp). The sample is at [go-active-mq-sample](https://github.com/project-samples/go-active-mq-sample)
- Active MQ nodejs library is at [activemq](https://www.npmjs.com/package/activemq), to wrap and simplify [activemq](https://www.npmjs.com/package/amqplib). The sample is at [activemq-sample](https://github.com/typescript-tutorial/activemq-sample)
### NATS
- NATS at [nats](https://github.com/core-go/nats), to wrap and simplify [nats.go](https://github.com/nats-io/nats.go). The sample is at [go-nats-sample](https://github.com/project-samples/go-nats-sample)
- NATS nodejs library is at [nats-plus](https://www.npmjs.com/package/nats-plus), to wrap and simplify [nats](https://www.npmjs.com/package/nats). The sample is at [nats-sample](https://github.com/typescript-tutorial/nats-sample)

### Advantages of Message Queues
#### Decoupling
- Producers and consumers do not need to be aware of each other.
- They can operate independently, allowing for more modular and maintainable systems.
#### Scalability
- Enables horizontal scaling by allowing multiple producers and consumers to interact with the queue concurrently.
#### Reliability
- Provides mechanisms for ensuring message delivery, such as persistence, acknowledgment, and retries.
#### Asynchronous Communication
- Allows systems to handle operations asynchronously, improving responsiveness and efficiency.
- Producers can send messages without waiting for consumers to process them immediately.
#### Load Balancing
- Messages can be distributed across multiple consumers, balancing the load and ensuring efficient processing.
#### Fault Tolerance
- Messages can be persisted in the queue, ensuring that they are not lost even if producers or consumers crash. This improves the resilience of the system.

### Use Cases of Message Queues
#### Microservices Communication
- Facilitates communication between microservices in a distributed system.
- For example, an order service can send messages to a payment service and a shipping service.

![Microservice Architecture](https://cdn-images-1.medium.com/max/800/1*vKeePO_UC73i7tfymSmYNA.png)
- A typical micro service

![A typical micro service](https://cdn-images-1.medium.com/max/800/1*d9kyekAbQYBxH-C6w38XZQ.png)

- A common flow to consume a message from a message queue.
  ![A common flow to consume a message from a message queue](https://cdn-images-1.medium.com/max/800/1*Y4QUN6QnfmJgaKigcNHbQA.png)
  - The sample is at [go-kafka-sample](https://github.com/project-samples/go-kafka-sample)
#### Task Queues
- Managing background tasks and job processing.
- For example, a web application can offload time-consuming tasks like image processing or email sending to a message queue.
#### Event-Driven Architectures
- Implementing event-driven systems where different components react to events.
- For example, a user registration event can trigger notifications, welcome emails, and analytics updates.
#### Data Pipelines
- Managing data flow in big data applications.
- For example, log data from various sources can be collected, processed, and analyzed using a message queue.
#### Decoupling Frontend and Backend
- Frontend applications can send messages to a queue, which are then processed by backend services.
- This improves responsiveness and allows for better handling of varying load conditions.

## Installation
Please make sure to initialize a Go module before installing core-go/mq:

```shell
go get -u github.com/core-go/mq
```

Import:
```go
import "github.com/core-go/mq"
```

Build for confluent:
```go
go build -buildmode=exe main.go
```
