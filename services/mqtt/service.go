package mqtt

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/kapacitor/alert"
)

// QoSLevel indicates the quality of service for messages delivered to a
// broker.
type QoSLevel byte

const (
	// best effort delivery. "fire and forget"
	AtMostOnce QoSLevel = iota
	// guarantees delivery to at least one receiver. May deliver multiple times.
	AtLeastOnce
	// guarantees delivery only once. Safest and slowest.
	ExactlyOnce
)

// DefaultQuiesceTimeout is the duration the client will wait for outstanding
// messages to be published before forcing a disconnection
const DefaultQuiesceTimeout time.Duration = 250 * time.Millisecond

type Service struct {
	logger *log.Logger

	configValue atomic.Value
	client      pahomqtt.Client
	token       pahomqtt.Token
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Open() error {
	s.logger.Println("I! Starting MQTT service")
	s.runClient()
	return nil

}

func (s *Service) Close() error {
	s.logger.Println("I! Stopping MQTT service")
	s.stopClient()
	return nil
}

func (s *Service) Alert(qos QoSLevel, topic, message string) error {
	s.client.Publish(topic, byte(qos), false, message) // should retained be configureable?
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
		if err := s.reloadClient(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Handler(c HandlerConfig, l *log.Logger) alert.Handler {
	return &handler{
		s:      s,
		c:      c,
		logger: l,
	}
}

type HandlerConfig struct {
	Topic string
	QoS   QoSLevel
}

type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.QoS, h.c.Topic, event.State.Message); err != nil {
		h.logger.Println("E! failed to post message to MQTT broker", err)
	}
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	c := s.config()
	return HandlerConfig{
		Topic: c.DefaultTopic,
		QoS:   c.DefaultQoS,
	}
}

func (s *Service) reloadClient() error {
	if err := s.stopClient(); err != nil {
		return err
	}
	if err := s.runClient(); err != nil {
		return err
	}
	return nil
}

func (s *Service) runClient() error {
	c := s.config()
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(c.Broker())
	opts.SetClientID(c.ClientID)
	opts.SetUsername(c.Username)
	opts.SetPassword(c.Password)
	opts.SetCleanSession(false)

	s.client = pahomqtt.NewClient(opts)
	s.token = s.client.Connect()

	s.token.Wait()

	if err := s.token.Error(); err != nil {
		s.logger.Println("E! Error connecting to MQTT broker at", c.Broker(), "err:", err)
		return err
	}
	s.logger.Println("I! Connected to MQTT Broker at", c.Broker())
	return nil
}

func (s *Service) stopClient() error {
	if s.client != nil {
		s.client.Disconnect(uint(DefaultQuiesceTimeout / time.Millisecond))
	}
	s.logger.Println("I! MQTT Client Disconnected")
	return nil
}

type testOptions struct {
	Topic   string   `json:"topic"`
	Message string   `json:"message"`
	QoS     QoSLevel `json:"qos"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Topic:   c.DefaultTopic,
		QoS:     c.DefaultQoS,
		Message: "test MQTT message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.QoS, options.Topic, options.Message)
}
