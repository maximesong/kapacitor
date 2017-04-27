package mqtt

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/kapacitor/alert"
)

type QOSLevel byte

const (
	// best effort delivery. "fire and forget"
	ATMOSTONCE QOSLevel = iota
	// guarantees delivery to at least one receiver. May deliver multiple times.
	ATLEASTONCE
	// guarantees delivery only once. Safest and slowest.
	EXACTLYONCE
)

// DEFAULT_QUIESCE_TIMEOUT is the duration the client will wait for outstanding
// messages to be published before forcing a disconnection
const DEFAULT_QUIESCE_TIMEOUT time.Duration = 250 * time.Millisecond

type Service struct {
	Logger *log.Logger

	configValue atomic.Value
	client      pahomqtt.Client
	token       pahomqtt.Token
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		Logger: l,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

// TODO(timraymond): improve logging here and in Close
func (s *Service) Open() error {
	s.Logger.Println("I! Starting MQTT service")

	c := s.config()
	opts := pahomqtt.NewClientOptions()
	opts.AddBroker(c.Broker())
	opts.SetClientID(c.ClientID) // TODO(timraymond): should we provide a random one?
	opts.SetUsername(c.Username)
	opts.SetPassword(c.Password)
	opts.SetCleanSession(false) // wtf is this? Why does it default to false?

	s.client = pahomqtt.NewClient(opts)
	s.token = s.client.Connect()

	s.token.Wait()

	if err := s.token.Error(); err != nil {
		s.Logger.Println("E! Error connecting to MQTT broker at", c.Broker(), "err:", err) //TODO(timraymond): put a legit error in
		return err
	}
	s.Logger.Println("I! Connected to MQTT Broker at", c.Broker())
	return nil

}

func (s *Service) Close() error {
	s.client.Disconnect(uint(DEFAULT_QUIESCE_TIMEOUT / time.Millisecond))
	s.Logger.Println("I! MQTT Client Disconnected")
	return nil
}

func (s *Service) Alert(qos QOSLevel, topic, message string) error {
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
	QOS   QOSLevel
}

type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.QOS, h.c.Topic, event.State.Message); err != nil {
		h.logger.Println("E! failed to post message to MQTT broker", err)
	}
}

func (s *Service) DefaultHandlerConfig() HandlerConfig {
	c := s.config()
	return HandlerConfig{
		Topic: c.DefaultTopic,
		QOS:   c.DefaultQOS,
	}
}

type testOptions struct {
	Topic   string   `json:"topic"`
	Message string   `json:"message"`
	QOS     QOSLevel `json:"qos"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Topic:   c.DefaultTopic,
		QOS:     c.DefaultQOS,
		Message: "test MQTT message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.QOS, options.Topic, options.Message)
}
