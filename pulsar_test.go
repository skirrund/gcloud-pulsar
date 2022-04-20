package pulsar

import (
	"strconv"
	"testing"
	"time"

	"github.com/skirrund/gcloud/logger"
	"github.com/skirrund/gcloud/mq"
	"github.com/skirrund/gcloud/mq/consumer"
)

type Test struct {
}

func (Test) OnMessage(message consumer.Message) error {
	logger.Info("onmsg:", message.Value)
	time.Sleep(1 * time.Second)
	return nil
}
func TestInitClient(t *testing.T) {
	client := NewClient("pulsar://pulsar1:6650", 0, 0, "test")
	for i := 0; i != 10000; i++ {
		go client.Send("test1", "test1-"+strconv.FormatInt(int64(i), 10))
	}
	client.Subscribe(mq.ConsumerOptions{
		Topic:            "test1",
		SubscriptionName: "test1",
		SubscriptionType: mq.Shared,
		MessageListener:  Test{},
	})
}
