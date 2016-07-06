package amqp

import (
	"time"

	"github.com/streadway/amqp"
)

type DeliveryInterface interface {
	GetAcknowledger() amqp.Acknowledger
	GetHeaders() amqp.Table
	GetContentType() string
	GetContentEncoding() string
	GetDeliveryMode() uint8
	GetPriority() uint8
	GetCorrelationId() string
	GetReplyTo() string
	GetExpiration() string
	GetMessageId() string
	GetTimestamp() time.Time
	GetType() string
	GetUserId() string
	GetAppId() string
	GetConsumerTag() string
	GetMessageCount() uint32
	GetDeliveryTag() uint64
	GetRedelivered() bool
	GetExchange() string   // basic.publish exhange
	GetRoutingKey() string // basic.publish routing key
	GetBody() []byte

	Ack(multiple bool) error
	Nack(multiple, requeue bool) error
	Reject(requeue bool) error
}

type Delivery struct {
	amqp.Delivery
}

func (d Delivery) GetAcknowledger() amqp.Acknowledger { return d.Acknowledger }
func (d Delivery) GetHeaders() amqp.Table             { return d.Headers }
func (d Delivery) GetContentType() string             { return d.ContentType }
func (d Delivery) GetContentEncoding() string         { return d.ContentEncoding }
func (d Delivery) GetDeliveryMode() uint8             { return d.DeliveryMode }
func (d Delivery) GetPriority() uint8                 { return d.Priority }
func (d Delivery) GetCorrelationId() string           { return d.CorrelationId }
func (d Delivery) GetReplyTo() string                 { return d.ReplyTo }
func (d Delivery) GetExpiration() string              { return d.Expiration }
func (d Delivery) GetMessageId() string               { return d.MessageId }
func (d Delivery) GetTimestamp() time.Time            { return d.Timestamp }
func (d Delivery) GetType() string                    { return d.Type }
func (d Delivery) GetUserId() string                  { return d.UserId }
func (d Delivery) GetAppId() string                   { return d.AppId }
func (d Delivery) GetConsumerTag() string             { return d.ConsumerTag }
func (d Delivery) GetMessageCount() uint32            { return d.MessageCount }
func (d Delivery) GetDeliveryTag() uint64             { return d.DeliveryTag }
func (d Delivery) GetRedelivered() bool               { return d.Redelivered }
func (d Delivery) GetExchange() string                { return d.Exchange }
func (d Delivery) GetRoutingKey() string              { return d.RoutingKey }
func (d Delivery) GetBody() []byte                    { return d.Body }

func (d Delivery) Ack(multiple bool) error           { return d.Delivery.Ack(multiple) }
func (d Delivery) Nack(multiple, requeue bool) error { return d.Delivery.Nack(multiple, requeue) }
func (d Delivery) Reject(requeue bool) error         { return d.Delivery.Reject(requeue) }

type mockDelivery struct {
	Acknowledger    amqp.Acknowledger
	Headers         amqp.Table
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
	ConsumerTag     string
	MessageCount    uint32
	DeliveryTag     uint64
	Redelivered     bool
	Exchange        string
	RoutingKey      string
	Body            []byte

	acked       bool
	ackMultiple bool

	nacked       bool
	nackMultiple bool
	nackRequeue  bool

	rejected       bool
	rejectRequeued bool
}

func (d *mockDelivery) GetAcknowledger() amqp.Acknowledger { return d.Acknowledger }
func (d *mockDelivery) GetHeaders() amqp.Table             { return d.Headers }
func (d *mockDelivery) GetContentType() string             { return d.ContentType }
func (d *mockDelivery) GetContentEncoding() string         { return d.ContentEncoding }
func (d *mockDelivery) GetDeliveryMode() uint8             { return d.DeliveryMode }
func (d *mockDelivery) GetPriority() uint8                 { return d.Priority }
func (d *mockDelivery) GetCorrelationId() string           { return d.CorrelationId }
func (d *mockDelivery) GetReplyTo() string                 { return d.ReplyTo }
func (d *mockDelivery) GetExpiration() string              { return d.Expiration }
func (d *mockDelivery) GetMessageId() string               { return d.MessageId }
func (d *mockDelivery) GetTimestamp() time.Time            { return d.Timestamp }
func (d *mockDelivery) GetType() string                    { return d.Type }
func (d *mockDelivery) GetUserId() string                  { return d.UserId }
func (d *mockDelivery) GetAppId() string                   { return d.AppId }
func (d *mockDelivery) GetConsumerTag() string             { return d.ConsumerTag }
func (d *mockDelivery) GetMessageCount() uint32            { return d.MessageCount }
func (d *mockDelivery) GetDeliveryTag() uint64             { return d.DeliveryTag }
func (d *mockDelivery) GetRedelivered() bool               { return d.Redelivered }
func (d *mockDelivery) GetExchange() string                { return d.Exchange }
func (d *mockDelivery) GetRoutingKey() string              { return d.RoutingKey }
func (d *mockDelivery) GetBody() []byte                    { return d.Body }

func (d *mockDelivery) Ack(multiple bool) error {
	d.acked = true
	d.ackMultiple = multiple

	return nil
}

func (d *mockDelivery) Nack(multiple, requeue bool) error {
	d.nacked = true
	d.nackMultiple = multiple
	d.nackRequeue = requeue

	return nil
}

func (d *mockDelivery) Reject(requeue bool) error {
	d.rejected = true
	d.rejectRequeued = requeue

	return d.Reject(requeue)
}
