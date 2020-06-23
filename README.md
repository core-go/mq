# mq
Define 2 standard interfaces: Producer and Consumer 
- [Producer](https://github.com/common-go/mq/blob/master/producer.go)
- [Consumer](https://github.com/common-go/mq/blob/master/consumer.go), which requires [Message](https://github.com/common-go/mq/blob/master/message.go) and [ConsumerCaller](https://github.com/common-go/mq/blob/master/consumer_caller.go)

## Implementations
Support these message queues:
- Amazon Simple Queue Service (SQS) at [common-go/sqs](https://github.com/common-go/sqs)
- Google Cloud Pub/Sub at [common-go/pubsub](https://github.com/common-go/pubsub)
- Kafka at [common-go/kafka](https://github.com/common-go/kafka)
- Active MQ at [common-go/amq](https://github.com/common-go/amq)
- RabbitMQ at [common-go/rabbitmq](https://github.com/common-go/rabbitmq)

## Versions
- v0.0.1: Producer only
- v0.0.5: Consumer only
- v1.0.0: Producer and Consumer
- v1.1.0: Producer, Consumer and [Batch Consumer](https://github.com/common-go/mq/blob/master/batch_handler.go)
- v1.1.1: Producer, Consumer and Batch Consumer with DefaultValidator

## Installation
Please make sure to initialize a Go module before installing common-go/mq:

```shell
go get -u github.com/common-go/mq
```

Import:

```go
import "github.com/common-go/mq"
```
