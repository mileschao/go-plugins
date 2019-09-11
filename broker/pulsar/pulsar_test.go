package pulsar

import (
	"errors"
	"fmt"
	mbroker "github.com/micro/go-micro/broker"
	"testing"
	"time"
)

func TestPulsarConnect(t *testing.T) {
	broker := NewBroker(mbroker.Addrs("pulsar://localhost:6650"))
	if err := broker.Connect(); err != nil {
		t.Errorf("connect pulsar error: %s", err)
	}
}

func TestPulsarPubSub(t *testing.T) {
	broker := NewBroker(mbroker.Addrs("pulsar://localhost:6650"))
	if err := broker.Connect(); err != nil {
		t.Errorf("connect pulsar error: %s", err)
	}
	_, err := broker.Subscribe("test-topic", func(event mbroker.Event) error {
		if event.Topic() != "persistent://public/default/test-topic" {
			t.Errorf("sub topic error: %s", event.Topic())
			return errors.New("sub topic")
		}
		msg := event.Message()
		_, ok := msg.Header["key"]
		if !ok {
			t.Errorf("sub message error: %+v", msg)
			return fmt.Errorf("sub message: %+v", msg)
		}
		if string(msg.Body) != string(`{"body":"payload"}`) {
			t.Errorf("sub message error: %+v", msg)
			return fmt.Errorf("sub message: %+v", msg)
		}
		if err := event.Ack(); err != nil {
			t.Errorf("sub ack error: %+v", msg)
			return fmt.Errorf("sub message: %+v", msg)
		}
		t.Logf("sub msg: %+v", msg)
		return nil
	}, mbroker.Queue("test-subname"))
	if err != nil {
		t.Errorf("subscribe error: %s", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(time.Second * 3):
			for j := 0; j < 3; j++ {
				if err := broker.Publish("test-topic", &mbroker.Message{
					Header: map[string]string{"key": fmt.Sprintf("i: %d, j: %d", i, j)},
					Body:   []byte(`{"body":"payload"}`),
				}); err != nil {
					t.Errorf("publish error: %s", err)
				}
			}
		}
	}

	/* if err := sub.Unsubscribe(); err != nil {
		t.Errorf("unsubscribe error: %s", err)
	} */
}
