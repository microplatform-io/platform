package platform

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/streadway/amqp"
)

var logger = GetLogger("platform")

func NewAmqpConnection(endpoint string) (*amqp.Connection, error) {
	logger.Println("[amqp] > connecting")

	return amqp.Dial(endpoint)
}

type AmqpProducer struct {
	conn     *amqp.Connection
	exchange string
}

func (ap *AmqpProducer) Close() error {
	return ap.conn.Close()
}

func (ap *AmqpProducer) Publish(topic string, body []byte) error {
	logger.Printf("[amqp] > publishing for %s", topic)

	ch, err := ap.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.Publish(
		ap.exchange, // exchange
		topic,       // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
}

func NewAmqpProducer(conn *amqp.Connection, exchange string) (*AmqpProducer, error) {
	return &AmqpProducer{conn, exchange}, nil
}

type AmqpConsumer struct {
	conn     *amqp.Connection
	queue    string
	topic    string
	exchange string
	handler  RoutedMessageHandler
}

func (ac *AmqpConsumer) AddHandler(handler RoutedMessageHandler) {
	ac.handler = handler
}

func (ac *AmqpConsumer) Close() error {
	return ac.conn.Close()
}

func (ac *AmqpConsumer) ListenAndServe() error {
	ch, err := ac.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(ac.queue, false, true, false, false, nil)
	if err != nil {
		return err
	}

	if err := ch.QueueBind(queue.Name, ac.topic, ac.exchange, false, nil); err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queue.Name, // queue
		ac.topic,   // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	for {
		for msg := range msgs {
			go func(msg amqp.Delivery) {
				if ac.topic == "" || (ac.topic == msg.RoutingKey) {
					routedMessage := &RoutedMessage{}
					if err := proto.Unmarshal(msg.Body, routedMessage); err != nil {
						logger.Println("> failed to decode routed message")
						msg.Ack(true)
					} else {
						if err := ac.handler.HandleMessage(routedMessage); err != nil {
							// If this message has already been redelivered, just ack it
							if msg.Redelivered {
								msg.Ack(true)
							} else {
								msg.Reject(true)
							}
						} else {
							msg.Ack(true)
						}
					}
				} else {
					msg.Reject(true)
				}
			}(msg)
		}
	}

	return nil
}

func NewAmqpTopicConsumer(conn *amqp.Connection, queue, topic, exchange string) (*AmqpConsumer, error) {
	return &AmqpConsumer{
		conn:     conn,
		queue:    queue,
		topic:    topic,
		exchange: exchange,
	}, nil
}

type AmqpConsumerFactory struct {
	conn  *amqp.Connection
	queue string
}

func (acf *AmqpConsumerFactory) Create(topic string, handler RoutedMessageHandler) Consumer {
	consumer, err := NewAmqpTopicConsumer(acf.conn, acf.queue, topic, "amq.topic")
	if err != nil {
		logger.Fatalf("> failed to create a consumer from the factory: %s", err)
	}

	consumer.AddHandler(handler)

	return consumer
}
