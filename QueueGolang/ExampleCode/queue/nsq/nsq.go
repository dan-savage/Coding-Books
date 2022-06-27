package nsq

import (
	"encoding/json"
	"errors"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/apex/log"
	nsq "github.com/nsqio/go-nsq"
)

var (
	defaultBuffer                   = 1000
	errIncorrectScheme              = errors.New("incorrect scheme, should be nsqd or nsqlookupd")
	errIncorrectSchemeForPublishing = errors.New("incorrect scheme, publishing requires nsqd")
	nsqlookupdScheme                = "nsqlookupd"
	nsqdScheme                      = "nsqd"
)

func init() {
	queue.Register(nsqlookupdScheme, newNSQQueueMux(true))
	queue.Register(nsqdScheme, newNSQQueueMux(false))
}

type NSQMessage struct {
	CorrelationID string
	RawMessage    json.RawMessage
}

type NSQQueueMux struct {
	useNSQLookupd bool
}

func newNSQQueueMux(useNSQLookupd bool) *NSQQueueMux {
	return &NSQQueueMux{useNSQLookupd}
}

func (s *NSQQueueMux) Queue(uri string) (*queue.QueueHandler, error) {

	handler := queue.NewQueueHandler(uri, defaultBuffer)
	err := s.pollForIncomingMessages(handler)
	if err != nil {
		return nil, err
	}

	err = s.pollForOutgoingMessages(handler)
	if err != nil {
		return nil, err
	}
	return handler, nil
}

func (s *NSQQueueMux) pollForIncomingMessages(handler *queue.QueueHandler) error {
	u, err := url.Parse(handler.URI())
	if err != nil {
		return err
	}
	if u.Scheme != nsqdScheme && u.Scheme != nsqlookupdScheme {
		return errIncorrectScheme
	}
	topic := strings.TrimPrefix(path.Dir(u.Path), "/")
	channel := strings.TrimPrefix(path.Base(u.Path), "/")
	if u.Fragment == "ephemeral" {
		channel += "#ephemeral"
	}
	if topic == "" {
		topic = channel
	}
	if topic == channel {
		log.
			WithField("topic", topic).
			Info("queue started in publish only mode")
		return nil
	}
	consumer, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
	if err != nil {
		return err
	}
	go func() {
		// Wait for the handler to be ready before beginning consumption.
		<-handler.Ready
	LOOP:
		for {
			log.
				WithField("topic", topic).
				WithField("channel", channel).
				Info("nsq queue consumer starting")
			consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
				var m NSQMessage
				err := json.Unmarshal(message.Body, &m)
				if err != nil {
					return err
				}
				ctx := options.ContextWithPublishOptions(
					options.NewMessageContext(),
					options.Merge(
						options.WithCorrelationID(m.CorrelationID),
					),
				)

				return <-handler.Receive(ctx, m.RawMessage)
			}))
			if s.useNSQLookupd {
				err := consumer.ConnectToNSQLookupd(u.Host)
				if err != nil {
					log.
						WithField("topic", topic).
						WithField("channel", channel).
						WithError(err).
						Error("connecting to nsqlookupd")
					time.Sleep(10 * time.Second)
					// If an error occurs it should be temporary, wait 10s and
					// try again.
					continue LOOP
				}
			} else {
				err := consumer.ConnectToNSQD(u.Host)
				if err != nil {
					log.
						WithField("topic", topic).
						WithField("channel", channel).
						WithError(err).
						Error("connecting to nsqd")
					time.Sleep(10 * time.Second)
					// If an error occurs it should be temporary, wait 10s and
					// try again.
					continue LOOP
				}
			}
			// Wait for the handler or consumer to signal shutdown.
			select {
			case <-consumer.StopChan:
				log.
					WithField("topic", topic).
					WithField("channel", channel).
					Warn("nsq consumer stopping")
				break LOOP
			case <-handler.Done:
				log.
					WithField("topic", topic).
					WithField("channel", channel).
					Warn("nsq handler stopping")
				break LOOP
			}
		}
	}()
	return nil
}

func (s *NSQQueueMux) pollForOutgoingMessages(handler *queue.QueueHandler) error {
	u, err := url.Parse(handler.URI())
	if err != nil {
		return err
	}
	if u.Scheme != nsqdScheme {
		if u.Scheme == nsqlookupdScheme {
			log.Warn("queue is in non publishing mode")
			return nil
		}
		return errIncorrectScheme
	}
	topic := strings.TrimPrefix(path.Dir(u.Path), "/")
	producer, err := nsq.NewProducer(u.Host, nsq.NewConfig())
	if err != nil {
		return err
	}
	go func() {
	LOOP:
		for {
			select {
			case <-handler.Done:
				log.
					WithField("topic", topic).
					Info("nsq queue publisher shutting down")
				break LOOP
			case outgoing := <-handler.Outgoing:
				var msg NSQMessage
				opts, ok := options.PublishOptionsFromContext(outgoing.Context)
				if ok {
					if opts.CorrelationID != nil && *opts.CorrelationID != "" {
						msg.CorrelationID = *opts.CorrelationID
					}
				}
				msg.RawMessage = outgoing.Data
				byt, err := json.Marshal(msg)
				if err != nil {
					log.
						WithField("topic", topic).
						Error("error marshaling json (nsq)")
					outgoing.Err <- err
					outgoing.Close()
					continue
				}
				err = producer.Publish(topic, byt)
				if err != nil {
					log.
						WithField("topic", topic).
						Error("error publishing to nsq")
					outgoing.Err <- err
					outgoing.Close()
					continue
				}
				outgoing.Close()
			}
		}
	}()
	return nil
}
