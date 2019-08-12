package pulsar

import (
	"context"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"github.com/micro/go-micro/broker"
)

var (
	DefaultBrokerConfig = &pulsar.ClientOptions{}
)

type brokerConfigKey struct{}

func BrokerConfig(c *pulsar.ClientOptions) broker.Option {
	return setBrokerOption(brokerConfigKey{}, c)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type publishContextKey struct{}

// PublishContext set the context for broker.PublishOption
func PublishContext(ctx context.Context) broker.PublishOption {
	return setPublishOption(publishContextKey{}, ctx)
}
