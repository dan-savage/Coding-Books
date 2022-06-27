package sqs

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type TestSQS struct {
	sqsiface.SQSAPI
	queueUrl                 string
	queueUrlCalledWith       *sqs.GetQueueUrlInput
	receiveMessageCalledWith *sqs.ReceiveMessageInput
	receiptHandle            string
	deleteMessageCalledWith  *sqs.DeleteMessageInput
	queueVisbilityCalledWith *sqs.GetQueueAttributesInput
}

func (t *TestSQS) GetQueueAttributes(i *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
	t.queueVisbilityCalledWith = i

	v := "30"
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]*string{"VisibilityTimeout": &v},
	}, nil

}

func (t *TestSQS) GetQueueUrl(i *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	t.queueUrlCalledWith = i
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(t.queueUrl),
	}, nil
}

func (t *TestSQS) ReceiveMessage(i *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	t.receiveMessageCalledWith = i
	return &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				ReceiptHandle: aws.String(t.receiptHandle),
			},
		},
	}, nil
}

func (t *TestSQS) DeleteMessage(i *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	t.deleteMessageCalledWith = i
	return &sqs.DeleteMessageOutput{}, nil
}

func TestSQSQueueMux_Queue(t *testing.T) {
	type args struct {
		uri string
	}
	tests := []struct {
		name    string
		args    args
		sqs     TestSQS
		expect  func(sqs *TestSQS)
		wantErr bool
	}{
		{
			"invalid scheme should error",
			args{"foo://"},
			TestSQS{},
			nil,
			true,
		},
		{
			"correct scheme should create queue",
			args{"sqs://someuri"},
			TestSQS{queueUrl: "someurl", receiptHandle: "somehandle"},
			func(sqs *TestSQS) {
				if !reflect.DeepEqual(*sqs.queueUrlCalledWith.QueueName, "someuri") {
					t.Errorf("expected someuri got %s", *sqs.queueUrlCalledWith.QueueName)
				}
				if !reflect.DeepEqual(*sqs.receiveMessageCalledWith.QueueUrl, "someurl") {
					t.Errorf("expected someurl got %v", *sqs.receiveMessageCalledWith.QueueUrl)
				}
				if !reflect.DeepEqual(*sqs.deleteMessageCalledWith.ReceiptHandle, "somehandle") {
					t.Errorf("expected somehandle got %v", *sqs.deleteMessageCalledWith.ReceiptHandle)
				}
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GetSQS = func() sqsiface.SQSAPI {
				return &tt.sqs
			}
			s := &SQSQueueMux{}
			handler, err := s.Queue(tt.args.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("SQSQueueMux.Queue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if handler != nil {
				// Add noop handler and start consuming messages.
				handler.
					AddHandler(func(ctx context.Context, d []byte) error {
						return nil
					}).
					Start()
				// Short sleep to wait for queue polling to occur.
				time.Sleep(100 * time.Millisecond)
				if tt.expect != nil {
					tt.expect(&tt.sqs)
				}
				// Close the handler and shutdown queues.
				handler.Close()
			}
		})
	}
}
