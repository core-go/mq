# mq
![Message Queue](https://camo.githubusercontent.com/25ff46695aa80731f9814cff5036e38f65597cf76fd0cb93a1425745184a807a/68747470733a2f2f63646e2d696d616765732d312e6d656469756d2e636f6d2f6d61782f3830302f312a355049763841616a34673031585050466169433059412e706e67)

## Implementations
Support these message queues:
- Amazon Simple Queue Service (SQS) at [sqs](https://github.com/core-go/mq/tree/main/sqs)
- Google Cloud Pub/Sub at [pubsub](https://github.com/core-go/mq/tree/main/pubsub)
- Kafka: at [segmentio/kafka-go](https://github.com/core-go/mq/tree/main/kafka) and [Shopify/sarama](https://github.com/core-go/mq/tree/main/sarama)
- NATS at [nats](https://github.com/core-go/mq/tree/main/nats)
- Active MQ at [amq](https://github.com/core-go/mq/tree/main/amq)
- RabbitMQ at [rabbitmq](https://github.com/core-go/mq/tree/main/rabbitmq)
- IBM MQ at [ibm-mq](https://github.com/core-go/mq/tree/main/ibm-mq)

##### The samples are [go-subscription](https://github.com/project-samples/go-subscription) and [go-batch-subscription](https://github.com/project-samples/go-batch-subscription)

## Installation
Please make sure to initialize a Go module before installing core-go/mq:

```shell
go get -u github.com/core-go/mq
```

Import:
```go
import "github.com/core-go/mq"
```
