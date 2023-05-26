package pulsar

import (
	"errors"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/skirrund/gcloud/logger"
	"github.com/skirrund/gcloud/mq"
	"github.com/skirrund/gcloud/utils"

	baseConsumer "github.com/skirrund/gcloud/mq/consumer"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Message struct {
	Msg pulsar.Message
}

type PulsarClient struct {
	client  pulsar.Client
	appName string
	mt      sync.Mutex
}

const (
	SERVER_URL_KEY           = "pulsar.service-url"
	CONNECTION_TIMEOUT_KEY   = "pulsar.connectionTimeout"
	OPERATION_TIMEOUT_KEY    = "pulsar.operationTimeout"
	defaultConnectionTimeout = 5 * time.Second
	defaultOperationTimeout  = 30 * time.Second
)

var consumers sync.Map

const (
	MAX_RETRY_TIMES = 50
)

var pulsarClient *PulsarClient
var once sync.Once

func NewClient(url string, connectionTimeoutSecond int64, operationTimeoutSecond int64, appName string) mq.IClient {
	if pulsarClient != nil {
		return pulsarClient
	}
	once.Do(func() {
		pulsarClient = &PulsarClient{
			client:  createClient(url, connectionTimeoutSecond, operationTimeoutSecond),
			appName: appName,
		}
	})
	return pulsarClient

}

func createClient(url string, connectionTimeoutSecond int64, operationTimeoutSecond int64) pulsar.Client {
	var cts time.Duration
	var ots time.Duration
	if connectionTimeoutSecond > 0 {
		cts = time.Duration(connectionTimeoutSecond) * time.Second
	} else {
		cts = defaultConnectionTimeout
	}
	if operationTimeoutSecond > 0 {
		ots = time.Duration(operationTimeoutSecond) * time.Second
	} else {
		ots = defaultOperationTimeout
	}
	logger.Infof("[pulsar]start init pulsar-client:" + url)
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     url,
		ConnectionTimeout:       cts,
		OperationTimeout:        ots,
		MaxConnectionsPerBroker: runtime.NumCPU(),
	})
	if err != nil {
		panic(err)
	}
	logger.Infof("[pulsar]finished init pulsar-client")
	return client
}

func getAppName(appName string) string {
	name := utils.Uuid()
	if len(appName) == 0 {
		return name
	} else {
		return appName + "-" + name
	}
}

func (pc *PulsarClient) Send(topic string, msg string) error {
	return pc.doSend(topic, msg, 0)
}

func (pc *PulsarClient) SendDelay(topic string, msg string, delay time.Duration) error {
	return pc.doSend(topic, msg, delay)
}
func (pc *PulsarClient) SendDelayAt(topic string, msg string, delayAt time.Time) error {
	return pc.doSendDelayAt(topic, msg, delayAt)
}
func (pc *PulsarClient) SendAsync(topic string, msg string) error {
	return pc.doSendAsync(topic, msg, 0)
}

func (pc *PulsarClient) SendDelayAsync(topic string, msg string, delay time.Duration) error {
	return pc.doSendAsync(topic, msg, delay)
}
func (pc *PulsarClient) SendDelayAtAsync(topic string, msg string, delayAt time.Time) error {
	return pc.doSendDelayAtAsync(topic, msg, delayAt)
}

func (pc *PulsarClient) Subscribe(opts mq.ConsumerOptions) {
	go pc.doSubscribe(opts)
}

func (pc *PulsarClient) Subscribes(opts ...mq.ConsumerOptions) {
	for _, opt := range opts {
		pc.Subscribe(opt)
	}
}

func (pc *PulsarClient) doSubscribe(opts mq.ConsumerOptions) error {
	subscriptionName := opts.SubscriptionName
	topic := opts.Topic
	logger.Infof("[pulsar]ConsumerOptions:%v", opts)
	options := pulsar.ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    subscriptionName,
		Type:                pulsar.SubscriptionType(opts.SubscriptionType),
		Name:                getAppName(pc.appName),
		NackRedeliveryDelay: 15 * time.Second,
		//Schema:              pulsar.NewStringSchema(nil),
	}
	if opts.RetryTimes == 0 {
		opts.RetryTimes = MAX_RETRY_TIMES
	}
	schema := pulsar.NewJSONSchema(`"string"`, nil)
	channelSize := opts.MaxMessageChannelSize
	if channelSize == 0 {
		channelSize = 200
	}
	channel := make(chan pulsar.ConsumerMessage, channelSize)
	options.MessageChannel = channel
	consumer, err := pc.client.Subscribe(options)
	if err != nil {
		logger.Error(errors.New("[pulsar] Subscribe error:" + err.Error()))
		panic("[pulsar] Subscribe error:" + err.Error())
		//return err
	}
	consumers.Store(topic+":"+subscriptionName, opts)

	logger.Infof("[pulsar]store consumerOptions:"+topic+":"+subscriptionName, ",", opts)

	for cm := range channel {
		go consume(cm, consumer, schema, opts)
	}
	return nil
}
func consume(cm pulsar.ConsumerMessage, consumer pulsar.Consumer, schema pulsar.Schema, opts mq.ConsumerOptions) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("[pulsar] consumer panic recover :", err, "\n", string(debug.Stack()))
		}
	}()
	msg := cm.Message
	var msgStr string
	logger.Infof("[pulsar] consumer info=>subName:%s,msgId:%v,reDeliveryCount:%d,publishTime:%v,producerName:%s", cm.Subscription(), msg.ID(), msg.RedeliveryCount(), msg.PublishTime(), msg.ProducerName())
	err := schema.Decode(msg.Payload(), &msgStr)
	if err != nil {
		logger.Info("[pulsar] consumer msg:", err.Error())
	} else {
		logger.Info("[pulsar] consumer msg:", msgStr)
	}
	err = opts.MessageListener.OnMessage(baseConsumer.Message{
		Value:           msgStr,
		RedeliveryCount: msg.RedeliveryCount(),
	})
	if err == nil {
		consumer.Ack(msg)
	} else {
		logger.Error("[pulsar] consumer error:" + err.Error())
		copts, ok := consumers.Load(opts.Topic + ":" + cm.Subscription())
		retryTimes := uint32(0)
		ACKMode := uint32(0)
		if ok {
			if opt, o := copts.(mq.ConsumerOptions); o {
				retryTimes = opt.RetryTimes
				if retryTimes > MAX_RETRY_TIMES {
					retryTimes = MAX_RETRY_TIMES
				}
				ACKMode = uint32(opt.ACKMode)
			} else {
				logger.Errorf("[pulsar] consumerOptions type error=> options:%v", copts)
			}
		} else {
			logger.Error("[pulsar] can not find ConsumerOptions=>subName:" + opts.Topic + ":" + cm.Subscription())
		}

		rt := msg.RedeliveryCount()
		if ACKMode == 1 && rt < retryTimes {
			logger.Infof("[pulsar]consummer error and retry=> subscriptionName:"+cm.Subscription()+",initRetryTimes:%d,retryTimes:%d,ack:%d", retryTimes, rt, ACKMode)
			consumer.Nack(msg)
		} else {
			logger.Infof("[pulsar]consummer error and can not retry=> subscriptionName:"+cm.Subscription()+",initRetryTimes:%d,retryTimes:%d,ack:%d", retryTimes, rt, ACKMode)
			consumer.Ack(msg)
		}

	}
}

func (pc *PulsarClient) Close() {
	pc.client.Close()
}
