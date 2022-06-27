package queue

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/apex/log"
)

var (
	errNoHandlers = errors.New("no handlers")
	queueRegistry = make(map[string]QueueMux)
)

// Register registers a QueueMux with the provided scheme. It must be
// called by underlying queue implementations in order use them with the
// Queue method.
func Register(scheme string, queueMux QueueMux) {
	queueRegistry[scheme] = queueMux
}

// Queue returns a QueueHandler for the provided URI or an error
// if the URI is invalid / unsupported.
func Queue(uri string) (*QueueHandler, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	mux, ok := queueRegistry[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("queue mux with scheme %s not found", u.Scheme)
	}
	return mux.Queue(uri)
}

// QueueMux is an interface to an underlying queue implementation.
type QueueMux interface {
	// Queue returns a QueueHandler for the provided URI or an error
	// if the URI is invalid / unsupported.
	Queue(uri string) (*QueueHandler, error)
}

// NewQueueHandler returns a new QueueHandler for the provided URI with the
// provided buffer size on the in and Outgoing channels.
func NewQueueHandler(uri string, buffer int) *QueueHandler {
	return &QueueHandler{
		uri:      uri,
		in:       make(chan QueueMessage, buffer),
		Outgoing: make(chan QueueMessage, buffer),
		Done:     make(chan bool),
		Ready:    make(chan bool, 1),
	}
}

// QueueMessage is a message that contains the raw
// data going to/from the underlying queue and an
// error channel for raising errors during the publishing
// or processing (consuming) of the message.
type QueueMessage struct {
	Data    []byte
	Err     chan error
	Context context.Context
}

// Close closes the error channel to indicate that the message
// has been processed/published with no error.
func (q QueueMessage) Close() {
	close(q.Err)
}

// QueueHandler implements methods for interacting with a message
// queue. It does not implement the any of the underlying message
// queue functionality. Implementations are expected to wrap
// this queue handler with their own logic such that producers &
// consumers can just utilise the QueueHandler methods to interact
// with the underlying queue implementation. QueueHandler supports
// buffering of incoming and outgoing messages via channels.
type QueueHandler struct {
	// The uri of the underlying message queue, where the uri scheme identifies the queue type (i.e. SQS etc.).
	uri string
	// Incoming message from the underlying queue are buffered using this channel. Consumers receive on this
	// channel to process messages.
	in chan QueueMessage
	// Outgoing messages to publish to the underlying queue are buffered on this channel. Produces send on this
	// channel to queue a message for publishing.
	Outgoing chan QueueMessage
	// Done is close to indicate shutdown of the queue.
	Done chan bool
	// Ready is used to notify implementations that queue is ready to consume.
	Ready chan bool
	// A slice of handlers that are called for each message received on the in channel.
	handlers []func(ctx context.Context, data []byte) error
	// Message Visibility Period
	Visibility time.Duration
}

// URI returns the queues URI.
func (q *QueueHandler) URI() string {
	return q.uri
}

// Receive is called by queue implementations to queue a message on the in channel for handling by the queue handlers.
// It returns a chan Error that is used by the handlers to propagate any errors during processing.
func (q *QueueHandler) Receive(ctx context.Context, data []byte) chan error {
	if len(q.handlers) == 0 {
		errCh := make(chan error, 1)
		errCh <- errNoHandlers
		return errCh
	}
	msg := QueueMessage{
		Data:    data,
		Err:     make(chan error, 1),
		Context: ctx,
	}
	q.in <- msg
	return msg.Err
}

// Close closes the Done channel indicating that the queue should shutdown.
func (q *QueueHandler) Close() {
	close(q.Done)
}

// AddHandler adds a handler func to the slice of handlers. It returns the QueueHandler for chaining.
func (q *QueueHandler) AddHandler(handler func(ctx context.Context, data []byte) error) *QueueHandler {
	q.handlers = append(q.handlers, handler)
	return q
}

// Publish sends a message to the Outgoing channel to queue the message for publishing by the
// underlying queue implementation.
func (q *QueueHandler) Publish(data []byte, opts ...options.PublishOptions) error {
	errCh := make(chan error, 1)
	m := QueueMessage{
		Data:    data,
		Err:     errCh,
		Context: options.ContextWithPublishOptions(options.NewMessageContext(), options.Merge(opts...)),
	}
	q.Outgoing <- m
	err := <-errCh
	if err != nil {
		metrics.MessagePublishError.WithLabelValues(q.URI()).Inc()
		return err
	}
	metrics.MessagePublishSuccess.WithLabelValues(q.URI()).Inc()
	return nil
}

// Start starts the QueueHandler receiving on the in channel to process messages. It needs to be called
// after the setup of handlers to begin consuming messages. It does not need to be called if the queue
// is only being used as a producer (i.e. for publishing).
func (q *QueueHandler) Start() {
	q.Ready <- true
	go func() {
		for {
			select {
			case msg := <-q.in:
				for i, handler := range q.handlers {
					tStart := time.Now()
					err := handler(msg.Context, msg.Data)
					if err != nil {
						metrics.MessageProcessedError.WithLabelValues(q.URI(), fmt.Sprint(i)).Inc()
						log.WithField("uri", q.uri).WithError(err).Error("handling message")
						msg.Err <- err
					} else {
						close(msg.Err)
						metrics.MessageProcessedSuccess.WithLabelValues(q.URI(), fmt.Sprint(i)).Inc()
					}
					metrics.MessageProcessTime.WithLabelValues(q.URI()).Observe(float64(time.Since(tStart).Milliseconds()))
				}
			case <-q.Done:
				log.WithField("uri", q.uri).Info("queue handler shutting down")
				break
			}
		}
	}()
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=DeduplicationMessage -exclude QueueHandler,MessageType
type DeduplicationMessage struct {
	InputURI string `json:"inputUri"`
	Account  account.Account
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=EmailMessage -exclude QueueHandler,MessageType
type EmailMessage struct {
	ID   string `json:"id"`
	Url  string `json:"url"`
	From string `json:"from"`
	To   string `json:"to"`
	ImportJobDetails
}

type ImportJobDetails struct {
	ID          string
	Account     string
	Measurement string
	UUID        string
	JobType     string
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=Measurement -exclude QueueHandler,MessageType
type Measurement = events.Measurement

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=LumenScriptJobMessage -exclude QueueHandler,MessageType
type LumenScriptJobMessage struct {
	ScriptURI        string
	ModelURI         string
	DestinationTable string
	InputURI         string
	ImportJob        api.ImportJob
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=BigQueryUploadMessage -exclude QueueHandler,MessageType
type BigQueryUploadMessage struct {
	InputURI           string
	DestinationDataset string
	DestinationTable   string
	ImportJob          api.ImportJob
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=DV360ImportMessage -exclude QueueHandler,MessageType
type DV360ImportMessage struct {
	InputURI  string
	ImportJob api.ImportJob
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=ImportJobRunMessage -exclude QueueHandler,MessageType
type ImportJobRunMessage struct {
	ImportJobID  string
	ImportJobRun importjob.ImportJobRun
}

//go:generate gen -src code.avct.cloud/attention-measurement-platform/internal/queue/template -replace MessageType=MediaGridActivation -exclude QueueHandler,MessageType
type MediaGridActivation struct {
	Activation activation.Activation
}
