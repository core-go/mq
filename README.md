# mq
![Message Queue](https://cdn-images-1.medium.com/max/800/1*a3_1glo06Fgh-pkqIATyng.png)
## Message Queue
A message queue is a communication method used in software systems to exchange information between different components or services asynchronously. It provides a way to send messages between producers (senders) and consumers (receivers) without requiring both parties to interact with the message queue at the same time. This decoupling allows for more scalable, reliable, and flexible system architectures.

You can refer to [Standardize-7-message-queues-in-GOLANG](https://www.linkedin.com/pulse/standardize-message-queues-golang-duc-nguyen-ekabc) and [Message Queue](https://www.linkedin.com/pulse/message-queue-go-nodejs-duc-nguyen-lb81c) at my [Linked In](https://vn.linkedin.com/in/duc-nguyen-437240239?trk=article-ssr-frontend-pulse_publisher-author-card) for more details.
## Standardize and Unify Message Queues in GOLANG
In distributed systems, message queues like Kafka, RabbitMQ, Active MQ, IBM MQ, NATS, Google Pub/Sub and Amazon SQS are crucial. They help to decouple services, ensure reliability, and enable asynchronous communication.

In Java, they have JMS (Java Message Service), which provides a standard API for messaging that can be used across different message-oriented middleware (MOM) systems, such as IBM MQ, ActiveMQ, and others.

However, in GOLANG, each of these message brokers has its own APIs and patterns for publishing and consuming messages, leading to code that’s tightly coupled to a specific technology, presenting a challenge: how do you maintain flexibility and simplicity when integrating these diverse systems?

### The Problems
#### Diverse APIs and Increased Complexity
Each message queue comes with its own set of complexities:
- Kafka: Requires handling partitions, consumer groups, and offset management.
- RabbitMQ: Involves exchanges, bindings, and manual message acknowledgments.
- Google Pub/Sub: Offers a simpler interface but still has its own quirks and configurations.

As a result, codebases that rely heavily on message queues often become entangled with the specifics of the chosen technology. If you decide to migrate from RabbitMQ to Kafka, for example, you’ll likely need to rewrite large portions of your codebase. Moreover, developers must spend time learning the intricacies of each new message queue, which can slow down development.
#### Handling pure-technical MQ parameters
Another challenge is dealing with pure-technical parameters like delay-seconds, count-threshold, and byte-threshold. These parameters are essential for configuring the message queue but don’t belong to the business logic layer. To keep the business logic clean and focused, we should wrap the message queue library to move these technical details to the infrastructure layer.
### The Solution: Standardizing Message Queues
To mitigate these issues, you can create a standardized interface for message publishing and consuming in GOLANG. This involves developing an abstraction layer that hides the complexities of individual message queues behind a unified API. By standardizing the way your application interacts with message queues, you can decouple your business logic from the specifics of the underlying message broker.
#### Key Features of a Standardized Interface:
- <b>Unified Publishing and Consuming</b>: A single set of functions for publishing and consuming messages, regardless of the underlying message queue.
- <b>Plug-and-Play Support</b>: Easily switch between different message queues by changing configurations, with minimal code changes.
- <b>Consistent Error Handling and Retries</b>: Implement standardized error handling, retries, and logging across all message queues.
- <b>Configuration Abstraction</b>: Standardize configuration options so that switching message queues doesn’t require reconfiguring the entire system.
- <b>Separate MQ technical parameters out of business logic</b>: We should move MQ technical parameters like delay-seconds, count-threshold, and byte-threshold to the infrastructure layer, to keep the business logic clean and focused.
- <b>Advanced Features</b>: In the wrapper library, we allow to use GO libraries at native level, to let developers access to advanced features of specific message queues through optional extensions, preserving flexibility without sacrificing simplicity.
#### The Pros and Cons of Standardization
<b>Pros</b>:
- <b>Faster Learning Curve</b>: New developers joining your team don’t need to learn the intricacies of multiple message queues. Instead, they can focus on the standardized interface, getting up to speed faster and contributing more effectively.
- <b>Simplified Codebase</b>: A standardized interface reduces the complexity of your codebase by decoupling it from specific message queue implementations.
- <b>Ease of Switching</b>: You can switch message queues with minimal effort, reducing the risk and cost of migrations.
- <b>Access to Advanced Features</b>: We allow to use GO libraries at native level, to allow developers to access to advanced features of a specific message queue like Kafka, IBM MQ.

<b>Cons</b>:
- <b>Potential Performance Overhead</b>: The abstraction layer might introduce slight performance penalties if not optimized for each message queue.
### Proposed Standardized Interface
#### Publishing A Message
```go
type Publisher interface {
  PublishData(ctx context.Context, data []byte) error
  Publish(ctx context.Context, data []byte, attributes map[string]string) error
  PublishMessage(ctx context.Context, message pubsub.Message) (string, error)
}
```
In most of message queues, I see they use Message struct as parameter, which has some disadvantages:
- In Message struct, there are some fields, which are used to consume message only. For example, in Google Pub/Sub, these fields 'PublishTime', 'DeliveryAttempt' are read-only, and used to consume message only.
- When most of the message queues use the full Message struct, they put more parameters, which are never used for publishing

<b>Solution</b>
- Move all MQ technical parameters like delay-seconds, count-threshold, and byte-threshold to the infrastructure layer, to keep the business logic clean.
- Remove all unused parameters, such as PublishTime, DeliveryAttempt when publishing the message
- Just keep the meaningful parameters. In the above interace, you see 2 clean methods, which can serve 95% the cases:
  ```go
  PublishData(ctx context.Context, data []byte) error
  Publish(ctx context.Context, data []byte, attributes map[string]string) error
  ```
- <b>To allow developers to access to advanced features</b>, we keep the native method:
  ```go
  PublishMessage(ctx context.Context, message pubsub.Message) (string, error)
  ```
#### Subscribe A Message
I observe these 9 libraries of 7 message queues below:
- https://github.com/segmentio/kafka-go (Kafka)
- https://github.com/IBM/sarama (Kafka)
- https://github.com/confluentinc/confluent-kafka-go (Kafka)
- https://github.com/rabbitmq/amqp091-go (Rabbit MQ)
- https://pkg.go.dev/cloud.google.com/go/pubsub (Google Pub/Sub)
- https://github.com/aws/aws-sdk-go/tree/main/service/sqs (Amazon SQS)
- https://github.com/ibm-messaging/mq-golang (IBM MQ)
- https://github.com/go-stomp/stomp (Active MQ)
- https://github.com/nats-io/nats.go (NATS)

After analyzed 9 libraries of 7 message queues, I see interface of Google Pub/Sub is simple, easy to use. So, I propose this interface:
```go
type Subscriber interface {
  SubscribeData(context.Context, func(context.Context, []byte))
  Subscribe(context.Context, func(context.Context, []byte, map[string]string))
  SubscribeMessage(context.Context, func(context.Context, *pubsub.Message))
}
```
- To keep the meaningful input parameters, I keep 2 clean methods, which can serve 95% the cases:
  ```go
  SubscribeData(context.Context, func(context.Context, []byte))
  Subscribe(context.Context, func(context.Context, []byte, map[string]string))
  ```
- To allow developers to access to advanced features, we keep the native method:
  ```go
  SubscribeMessage(context.Context, func(context.Context, *pubsub.Message))
  ```

<b>Summary</b>
With the above 2 interfaces, I can standardize the message queues, with clean business:
- You do not see the MQ configured parameters, because these parameters are put into the infrastructure layer.
- Most of the cases, we do not use the header. So, we keep 1 method to send/consume the body only.
- For some cases, we need to use the header. So, we keep 1 method to send/consume the body with header map[string]string. 'map[string]string' allow the interfaces not to depend any 3rd party library.
- Keep 1 method to handle the native library, to Access to Advanced Features.

If you do not like the above method names: SubscribeData, Subscribe, SubscribeMessage, in GOLANG, we have a solution for it. GOLANG allows higher-order functions, like Javascript, where you can pass one function to another, use it as a callback. You can create a new instance, and pass the method/function as the parameter. Inside the business layer, you can use the method name you want. 
### Available Examples:
I and my team, we standardize 9 GO libraries, of 7 message queues, and created these 9 samples. You can refer to these examples and see how easy to use:
#### RabbitMQ
- An open-source message broker that supports multiple messaging protocols. It provides features like message routing, persistence, and acknowledgment.
- RabbitMQ GO library is at [rabbitmq](https://github.com/core-go/rabbitmq), to wrap and simplify [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go). The sample is at [go-rabbit-mq-sample](https://github.com/project-samples/go-rabbit-mq-sample)
- RabbitMQ nodejs library is at [rabbitmq-ext](https://www.npmjs.com/package/rabbitmq-ext), to wrap and simplify [amqplib](https://www.npmjs.com/package/amqplib). The sample is at [rabbitmq-sample](https://github.com/typescript-tutorial/rabbitmq-sample)
#### Apache Kafka
- A distributed streaming platform that handles high-throughput, low-latency message processing. It is often used for building real-time data pipelines and streaming applications.
- Kafka GO library is at [kafka](https://github.com/core-go/kafka), to wrap and simplify 3 Kafka GO libraries: [segmentio/kafka-go](https://github.com/segmentio/kafka-go), [IBM/sarama](https://github.com/IBM/sarama) and [confluent](https://github.com/confluentinc/confluent-kafka-go). The sample is at [go-kafka-sample](https://github.com/project-samples/go-kafka-sample)
- Kafka nodejs library is at [kafka-plus](https://www.npmjs.com/package/kafka-plus), to wrap and simplify [kafkajs](https://www.npmjs.com/package/kafkajs). The sample is at [kafka-sample](https://github.com/typescript-tutorial/kafka-sample)
#### Amazon SQS (Simple Queue Service)
- A fully managed message queue service offered by AWS. It provides a reliable, scalable, and cost-effective way to decouple and coordinate distributed software systems and microservices.
- SQS GO library is at [sqs](https://github.com/core-go/sqs), to wrap and simplify [aws-sdk-go/service/sqs](https://github.com/aws/aws-sdk-go/tree/main/service/sqs). The sample is at [go-amazon-sqs-sample](https://github.com/project-samples/go-amazon-sqs-sample)
#### Google Cloud Pub/Sub
- A fully managed messaging service that allows for event-driven systems and real-time analytics on Google Cloud Platform.
- Pub/Sub GO library is at [core-go/pubsub](https://github.com/core-go/pubsub), to wrap and simplify [go/pubsub](https://pkg.go.dev/cloud.google.com/go/pubsub). The sample is at [go-pubsub-sample](https://github.com/project-samples/go-pubsub-sample)
- Pub/Sub nodejs library is at [google-pubsub](https://www.npmjs.com/package/google-pubsub), to wrap and simplify [@google-cloud/pubsub](https://www.npmjs.com/package/@google-cloud/pubsub). The sample is at [pubsub-sample](https://github.com/typescript-tutorial/pubsub-sample)
#### IBM MQ
- IBM MQ at [ibmmq](https://github.com/core-go/ibmmq), to wrap and simplify [ibmmq](https://github.com/ibm-messaging/mq-golang). The sample is at [go-ibm-mq-sample](https://github.com/project-samples/go-ibm-mq-sample)
- IBM MQ nodejs library is at [ibmmq-plus](https://www.npmjs.com/package/ibmmq-plus), to wrap and simplify [ibmmq](https://www.npmjs.com/package/ibmmq). The sample is at [ibmmq-sample](https://github.com/typescript-tutorial/ibmmq-sample)
#### Active MQ
- Active MQ at [activemq](https://github.com/core-go/activemq), to wrap and simplify [go-stomp](https://github.com/go-stomp/stomp). The sample is at [go-active-mq-sample](https://github.com/project-samples/go-active-mq-sample)
- Active MQ nodejs library is at [activemq](https://www.npmjs.com/package/activemq), to wrap and simplify [amqplib](https://www.npmjs.com/package/amqplib). The sample is at [activemq-sample](https://github.com/typescript-tutorial/activemq-sample)
#### NATS
- NATS at [nats](https://github.com/core-go/nats), to wrap and simplify [nats.go](https://github.com/nats-io/nats.go). The sample is at [go-nats-sample](https://github.com/project-samples/go-nats-sample)
- NATS nodejs library is at [nats-plus](https://www.npmjs.com/package/nats-plus), to wrap and simplify [nats](https://www.npmjs.com/package/nats). The sample is at [nats-sample](https://github.com/typescript-tutorial/nats-sample)

### Conclusion: Balancing Simplicity and Flexibility
Standardizing message publishing and consuming in Golang can significantly streamline your development process, especially in complex, distributed systems. It simplifies your code, makes it more maintainable, and makes it easier to switch between different message queues as your needs change. By adopting a standardized approach, you create a more resilient and adaptable system that can easily evolve as your project grows.

By also isolating technical parameters, you keep your business logic clean and focused, leading to better-structured and more maintainable code.

You might lose some advanced features, but the trade-off is worth it for the flexibility and simplicity you gain.

## Appendix
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

## Conclusion
Message queues are a crucial component in modern software architecture, providing a robust way to manage communication between different parts of a system. They enable decoupling, scalability, reliability, and fault tolerance, making them essential for building large-scale, distributed, and resilient applications. Understanding and implementing message queues can significantly enhance the efficiency and effectiveness of software systems.

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
