package sqs

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

var (
	defaultBackoff     = time.Second
	defaultBuffer      = 1000
	errIncorrectScheme = errors.New("incorrect scheme, should be sqs")
	sqsScheme          = "sqs"

	GetSQS = getSQS

	correlationIDAttributeKey = "correlation_id"
)

func init() {
	queue.Register(sqsScheme, newSQSQueueMux())
}

type SQSQueueMux struct {
}

func newSQSQueueMux() *SQSQueueMux {
	return &SQSQueueMux{}
}

func (s *SQSQueueMux) Queue(uri string) (*queue.QueueHandler, error) {
	handler := queue.NewQueueHandler(uri, defaultBuffer)
	err := pollForIncomingMessages(handler)
	if err != nil {
		return nil, err
	}
	err = pollForOutgoingMessages(handler)
	if err != nil {
		return nil, err
	}
	visibility, err := getVisibility(handler)
	if err != nil {
		return nil, err
	}
	handler.Visibility = visibility
	return handler, nil
}

func getVisibility(handler *queue.QueueHandler) (time.Duration, error) {
	var duration time.Duration
	u, err := url.Parse(handler.URI())
	if err != nil {
		return duration, err
	}
	if u.Scheme != sqsScheme {
		return duration, errIncorrectScheme
	}
	svc := GetSQS()
	res, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(u.Hostname()),
	})

	if err != nil {
		log.WithError(err).Error("failed to get queue url")
	}
	a := "All"
	attributes, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{QueueUrl: res.QueueUrl, AttributeNames: []*string{&a}})
	if err != nil {
		log.WithError(err).Error("")
	}
	visibility, err := strconv.Atoi(*attributes.Attributes["VisibilityTimeout"]) // This is the time in seconds
	if err != nil {
		return duration, err
	}
	duration = time.Second * time.Duration(visibility)
	return duration, nil
}

func pollForIncomingMessages(handler *queue.QueueHandler) error {
	u, err := url.Parse(handler.URI())
	if err != nil {
		return err
	}
	if u.Scheme != sqsScheme {
		return errIncorrectScheme
	}
	svc := GetSQS()
	res, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(u.Hostname()),
	})
	if err != nil {
		return err
	}
	go func() {
		// Wait for the handler to be ready before beginning consumption.
		<-handler.Ready
		log.WithField("uri", handler.URI()).Info("queue consumer starting")
		for {
			select {
			case <-handler.Done:
				log.WithField("uri", handler.URI()).Info("queue consumer shutting down")
				break
			default:
			}
			msgs, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
				WaitTimeSeconds: aws.Int64(10),
				QueueUrl:        res.QueueUrl,
				MessageAttributeNames: []*string{
					aws.String(correlationIDAttributeKey),
				},
			})

			if err != nil {
				log.WithField("uri", handler.URI()).WithError(err).Warn("error receiving message from queue")
				time.Sleep(defaultBackoff)
				continue
			}
			for _, msg := range msgs.Messages {
				ctx := options.ContextWithPublishOptions(context.Background(), options.Merge(
					options.WithCorrelationID(safelyGetCorrelationID(msg)),
				))
				err := <-handler.Receive(ctx, []byte(aws.StringValue(msg.Body)))
				if err != nil {
					continue
				}
				// The message should be deleted if there is no error, otherwise, if the handler
				// has set the message delete value it should use that behaviour.
				shouldDelete := err == nil
				if shouldDeleteValue, isDeleteSet := options.GetMessageDeleteValue(ctx); isDeleteSet {
					shouldDelete = shouldDeleteValue
				}
				// If shouldDelete is true delete the message from sqs.
				if shouldDelete {
					_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      res.QueueUrl,
						ReceiptHandle: msg.ReceiptHandle,
					})
					if err != nil {
						log.WithField("uri", handler.URI()).WithError(err).Error("deleting processed message")
						continue
					}
				}
			}
		}
	}()
	return nil
}

func pollForOutgoingMessages(handler *queue.QueueHandler) error {
	u, err := url.Parse(handler.URI())
	if err != nil {
		return err
	}
	if u.Scheme != sqsScheme {
		return errIncorrectScheme
	}
	svc := GetSQS()
	res, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(u.Hostname()),
	})
	if err != nil {
		return err
	}
	go func() {
	LOOP:
		for {
			select {
			case <-handler.Done:
				log.WithField("uri", handler.URI()).Info("queue publisher shutting down")
				break LOOP
			case outgoing := <-handler.Outgoing:
				messageAttributes := make(map[string]*sqs.MessageAttributeValue)
				opts, ok := options.PublishOptionsFromContext(outgoing.Context)
				if ok {
					messageAttributes[correlationIDAttributeKey] = &sqs.MessageAttributeValue{
						StringValue: opts.CorrelationID,
						DataType:    aws.String("String"),
					}
				}
				_, err = svc.SendMessage(&sqs.SendMessageInput{
					MessageBody:       aws.String(string(outgoing.Data)),
					MessageAttributes: messageAttributes,
					QueueUrl:          res.QueueUrl,
				})
				if err != nil {
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

func getSQS() sqsiface.SQSAPI {
	sess := session.New(aws.NewConfig())
	return sqs.New(sess)
}

func safelyGetCorrelationID(msg *sqs.Message) string {
	if attr, ok := msg.MessageAttributes[correlationIDAttributeKey]; ok {
		return aws.StringValue(attr.StringValue)
	}
	return ""
}
