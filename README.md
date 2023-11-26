# mq
![Message Queue](https://camo.githubusercontent.com/31291934a502f50fda6ec65981f77e601efa450f7ef32b3e4bd9041355d68e3e/68747470733a2f2f63646e2d696d616765732d312e6d656469756d2e636f6d2f6d61782f3830302f312a55624b4a753242634159696d385f6f4a67384e7336412e706e67)

## Implementations
Support these message queues:
- Amazon Simple Queue Service (SQS) at [sqs](https://github.com/core-go/mq/tree/main/sqs)
- Google Cloud Pub/Sub at [pubsub](https://github.com/core-go/mq/tree/main/pubsub)
- Kafka: at [segmentio/kafka-go](https://github.com/core-go/mq/tree/main/kafka), [Shopify/sarama](https://github.com/core-go/mq/tree/main/sarama) and [confluent](https://github.com/core-go/mq/tree/main/confluent)
- NATS at [nats](https://github.com/core-go/mq/tree/main/nats)
- Active MQ at [activemq](https://github.com/core-go/mq/tree/main/activemq)
- RabbitMQ at [rabbitmq](https://github.com/core-go/mq/tree/main/rabbitmq)
- IBM MQ at [ibmmq](https://github.com/core-go/mq/tree/main/ibmmq)

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

### Flow to consume a message from a queue
![Flow to consume a message](https://camo.githubusercontent.com/782bbf69a516401c3918b7e920d8fc25521112d8b04e890f2455768551f6d64e/68747470733a2f2f63646e2d696d616765732d312e6d656469756d2e636f6d2f6d61782f3830302f312a593451554e36516e666d4a67614b6967634e486251412e706e67)
- Consume a message from queue, then write the message to database (SQL, Mongo, Casandra, Dynamodb, Firestore, Elasticsearch)
- Support these databases
    - SQL
    - Mongo
    - Casandra
    - Dynamodb
    - Firestore
    - Elasticsearch
#### The sample is [go-subscription]
- Another sample to consume message and handle by batch is [go-batch-subscription](https://github.com/project-samples/go-batch-subscription)
