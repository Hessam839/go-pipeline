package ext

import (
	"encoding/json"
	streams "github.com/Hessam839/go-pipeline"
	"github.com/Hessam839/go-pipeline/flow"
	"github.com/nsqio/go-nsq"
	"time"
)

type NsqSource struct {
	out     chan interface{}
	client  *nsq.Consumer
	topic   string
	channel string
}

func NewNsqSource(host string, topic string, channel string) *NsqSource {
	out := make(chan interface{})

	config := nsq.NewConfig()
	config.MsgTimeout = time.Second * 300
	config.Deflate = true
	config.HeartbeatInterval = time.Second * 15
	config.DialTimeout = time.Second * 120

	consumer, consumerErr := nsq.NewConsumer(topic, channel, config)
	if consumerErr != nil {
		panic(consumerErr)
	}
	consumer.AddHandler(&NsqHandler{out: out})
	err := consumer.ConnectToNSQD(host)
	if err != nil {
		panic(err)
	}
	return &NsqSource{
		client:  consumer,
		topic:   topic,
		channel: channel,
		out:     out,
	}
}

func (ns *NsqSource) Via(_stage streams.Flow) streams.Flow {
	flow.DoStream(ns, _stage)
	return _stage
}

func (ns *NsqSource) Out() <-chan interface{} {
	return ns.out
}

type NsqHandler struct {
	out chan interface{}
}

func (nh *NsqHandler) HandleMessage(m *nsq.Message) error {
	if m.Body == nil {
		panic("error nsq in message")
	}
	nh.out <- m.Body
	return nil
}

type NsqSink struct {
	in     chan interface{}
	client *nsq.Producer
	topic  string
}

//NewNsqSink
func NewNsqSink(host string, topic string) *NsqSink {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(host, config)
	if err != nil {
		panic(err)
	}
	nsqSink := &NsqSink{
		in:     make(chan interface{}),
		client: producer,
		topic:  topic,
	}
	go nsqSink.init()
	return nsqSink
}

func (ns *NsqSink) init() {
	for msg := range ns.in {
		data, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}
		publishErr := ns.client.Publish(ns.topic, data)
		if publishErr != nil {
			panic(publishErr)
		}
	}
}

func (ns *NsqSink) In() chan<- interface{} {
	return ns.in
}
