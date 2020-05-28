package sqs

import (
	"context"
	"github.com/aaronland/go-aws-session"
	"github.com/aws/aws-sdk-go/aws"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_sqs "github.com/aws/aws-sdk-go/service/sqs"
	"strings"
)

func SendMessageWithDSN(ctx context.Context, dsn string, queue_url string, body string) (*aws_sqs.SendMessageOutput, error) {

	sess, err := session.NewSessionWithDSN(dsn)

	if err != nil {
		return nil, err
	}

	return SendMessageWithSession(ctx, sess, queue_url, body)
}

func SendMessageWithSession(ctx context.Context, sess *aws_session.Session, queue_url string, body string) (*aws_sqs.SendMessageOutput, error) {

	svc := aws_sqs.New(sess)
	return SendMessageWithService(ctx, svc, queue_url, body)
}

func SendMessageWithService(ctx context.Context, svc *aws_sqs.SQS, queue_url string, body string) (*aws_sqs.SendMessageOutput, error) {

	if !strings.HasPrefix(queue_url, "https://sqs") {

		rsp, err := svc.GetQueueUrl(&aws_sqs.GetQueueUrlInput{
			QueueName: aws.String(queue_url),
		})

		if err != nil {
			return nil, err
		}

		queue_url = *rsp.QueueUrl
	}

	msg := &aws_sqs.SendMessageInput{
		QueueUrl:    aws.String(queue_url),
		MessageBody: aws.String(body),
	}

	return svc.SendMessage(msg)
}
