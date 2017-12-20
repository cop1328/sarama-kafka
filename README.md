# sarama-kafka

```go
package main

import (
	kafka "github.com/tarmylan/sarama-kafka"
)

type Trace struct {
	TraceAt int64       `json:"trace_at"`
	Data    interface{} `json:"data"`
}

func main() {
	brokers := []string{"172.16.10.222:9092", "172.16.10.223:9092", "172.16.10.224:9092"}
	kafka.Init(brokers)

	trace := &Trace{
		Data: "kafka test data",
	}

	kafka.Submit("trace-topic", trace)
}
```
