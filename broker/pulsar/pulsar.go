// Package pulsar provides a pulsar broker using pulsar cluster
package pulsar

import (
	"context"
	"errors"
	"sync"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec/json"
	"github.com/micro/go-micro/config/cmd"
	"github.com/micro/go-micro/util/log"
)

type kBroker struct {
	addr    string
	c       pulsar.Client
	pub     map[string]pulsar.Producer
	scMutex sync.Mutex
	opts    broker.Options
}

type subscriber struct {
	c    pulsar.Consumer
	opts broker.SubscribeOptions
}

type event struct {
	c    pulsar.Consumer
	m    pulsar.Message
	opts broker.PublishOptions
}

func init() {
	cmd.DefaultBrokers["pulsar"] = NewBroker
}

func (e event) Topic() string {
	return e.c.Topic()
}

func (e event) Message() *broker.Message {
	return &broker.Message{
		Header: e.m.Properties(),
		Body:   e.m.Payload(),
	}
}

func (e event) Ack() error {
	return e.c.Ack(e.m)
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.c.Topic()
}

func (s *subscriber) Unsubscribe() error {
	if err := s.c.Unsubscribe(); err != nil {
		return err
	}
	return s.c.Close()
}

func (k *kBroker) Address() string {
	if len(k.addr) > 0 {
		return k.addr
	}
	return "pulsar://localhost:6650"
}

func (k *kBroker) Connect() error {
	if k.c != nil {
		return nil
	}

	cliOpts := k.getBrokerConfig()
	if len(k.opts.Addrs) <= 0 {
		return errors.New("address init")
	}
	cliOpts.URL = k.opts.Addrs[0]
	c, err := pulsar.NewClient(*cliOpts)
	if err != nil {
		return err
	}
	k.c = c
	return nil
}

func (k *kBroker) Disconnect() error {
	k.scMutex.Lock()
	defer k.scMutex.Unlock()
	for _, pub := range k.pub {
		pub.Close()
	}
	return k.c.Close()
}

func (k *kBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&k.opts)
	}
	var cAddr string
	for _, addr := range k.opts.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddr = addr
		break
	}
	if len(cAddr) == 0 {
		cAddr = "pulsar://localhost:6650"
	}
	k.addr = cAddr
	return nil
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {

	pub, ok := k.pub[topic]
	if !ok {
		producerOpts := pulsar.ProducerOptions{
			Topic: topic,
		}
		options := broker.PublishOptions{}
		for _, o := range opts {
			o(&options)
		}
		if options.Context != nil {
			if v, ok := options.Context.Value(publishContextKey{}).(pulsar.ProducerOptions); ok {
				producerOpts.Name = v.Name
				producerOpts.Properties = v.Properties
			}
		}

		p, err := k.c.CreateProducer(producerOpts)
		if err != nil {
			return err
		}
		k.pub[topic] = p
		pub = p
	}
	err := pub.Send(k.opts.Context, pulsar.ProducerMessage{
		Payload:    msg.Body,
		Properties: msg.Header,
	})
	return err
}

func (k *kBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	opt := broker.SubscribeOptions{
		AutoAck: true,
		Queue:   uuid.New().String(),
	}
	for _, o := range opts {
		o(&opt)
	}
	msgChannel := make(chan pulsar.ConsumerMessage)
	consumerOpts := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: opt.Queue,
		MessageChannel:   msgChannel,
	}
	if opt.Context != nil {
		if v, ok := opt.Context.Value(subscribeContextKey{}).(pulsar.ConsumerOptions); ok {
			consumerOpts.Name = v.Name
			consumerOpts.Type = v.Type
		}
	}
	c, err := k.c.Subscribe(consumerOpts)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			for cm := range msgChannel {
				msg := cm.Message
				if err := handler(event{
					c: c,
					m: msg,
				}); err != nil {
					log.Log("consumer handle error: ", err)
				}
			}
		}
	}()
	return &subscriber{c: c, opts: opt}, nil
}

func (k *kBroker) String() string {
	return "pulsar"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		// default to json codec
		Codec:   json.Marshaler{},
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	var cAddrs []string
	for _, addr := range options.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{"pulsar://localhost:6650"}
	}

	return &kBroker{
		addr: cAddrs[0],
		opts: options,
		pub:  make(map[string]pulsar.Producer, 0),
	}
}

func (k *kBroker) getBrokerConfig() *pulsar.ClientOptions {
	if c, ok := k.opts.Context.Value(brokerConfigKey{}).(*pulsar.ClientOptions); ok {
		return c
	}
	return DefaultBrokerConfig
}
