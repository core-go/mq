# mq
## Implementations
Support these message queues:
- Amazon Simple Queue Service (SQS) at [sqs](https://github.com/core-go/mq/tree/main/sqs)
- Google Cloud Pub/Sub at [pubsub](https://github.com/core-go/mq/tree/main/pubsub)
- Kafka: at [segmentio/kafka-go](https://github.com/core-go/mq/tree/main/kafka) and [Shopify/sarama](https://github.com/core-go/mq/tree/main/sarama)
- NATS at [nats](https://github.com/core-go/mq/tree/main/nats)
- Active MQ at [amq](https://github.com/core-go/mq/tree/main/amq)
- RabbitMQ at [rabbitmq](https://github.com/core-go/mq/tree/main/rabbitmq)
- IBM MQ at [ibm-mq](https://github.com/core-go/mq/tree/main/ibm-mq)

## Installation
Please make sure to initialize a Go module before installing core-go/mq:

```shell
go get -u github.com/core-go/mq
```

Import:
```go
import "github.com/core-go/mq"
```