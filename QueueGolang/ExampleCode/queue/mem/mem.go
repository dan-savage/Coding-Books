package mem

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/apex/log"
)

var (
	defaultBackoff = time.Second
	defaultBuffer  = 1000

	DefaultMemQueueMux = &MemQueueMux{pipe: make(map[string]chan MemQueueMessage)}
)

func init() {
	queue.Register("mem", DefaultMemQueueMux)
}

type MemQueueMessage struct {
	data []byte
	ctx  context.Context
}

type MemQueueMux struct {
	mtx  sync.Mutex
	pipe map[string]chan MemQueueMessage
}

func (s *MemQueueMux) Queue(uri string) (*queue.QueueHandler, error) {
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

func (s *MemQueueMux) pollForIncomingMessages(handler *queue.QueueHandler) error {
	u, err := url.Parse(handler.URI())
	if err != nil {
		return err
	}
	s.mtx.Lock()
	if _, exists := s.pipe[u.Host]; !exists {
		s.pipe[u.Host] = make(chan MemQueueMessage, defaultBuffer)
	}
	pipe := s.pipe[u.Host]
	s.mtx.Unlock()
	go func() {
		<-handler.Ready
		log.WithField("uri", handler.URI()).Info("queue consumer starting")
	LOOP:
		for {
			select {
			case <-handler.Done:
				log.WithField("uri", handler.URI()).Info("queue consumer shutting down")
				break LOOP
			case msg := <-pipe:
				err := <-handler.Receive(msg.ctx, msg.data)
				// The message should be deleted if there is no error, otherwise, if the handler
				// has set the message delete value it should use that behaviour.
				shouldDelete := err == nil
				if shouldDeleteValue, isDeleteSet := options.GetMessageDeleteValue(msg.ctx); isDeleteSet {
					shouldDelete = shouldDeleteValue
				}
				// Requeue the message unless it should be deleted.
				if !shouldDelete {
					go func() {
						log.Warn("requeueing failed message in 10 seconds")
						time.Sleep(10 * time.Second)
						pipe <- msg
					}()
					continue
				}
			}

		}
	}()
	return nil
}

func (s *MemQueueMux) pollForOutgoingMessages(handler *queue.QueueHandler) error {
	u, err := url.Parse(handler.URI())
	if err != nil {
		return err
	}
	go func() {
		log.WithField("uri", handler.URI()).Info("queue publisher starting")
		for {
			select {
			case <-handler.Done:
				log.WithField("uri", handler.URI()).Info("queue publisher shutting down")
				break
			case outgoing := <-handler.Outgoing:
				s.mtx.Lock()
				if ch, ok := s.pipe[u.Host]; ok {
					ch <- MemQueueMessage{
						data: outgoing.Data,
						ctx:  options.NewMessageContext(),
					}
				} else {
					s.pipe[u.Host] = make(chan MemQueueMessage, defaultBuffer)
					s.pipe[u.Host] <- MemQueueMessage{
						data: outgoing.Data,
						ctx:  options.NewMessageContext(),
					}
				}
				s.mtx.Unlock()
				outgoing.Close()
			}
		}
	}()
	return nil
}
