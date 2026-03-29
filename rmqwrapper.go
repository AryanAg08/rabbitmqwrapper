package rmqwrapper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	RMQConn  *amqp.Connection
	connLock sync.RWMutex
)

// Config holds the RabbitMQ connection configuration
type Config struct {
	URL              string
	ReconnectDelay   time.Duration
	MaxReconnectTime time.Duration
}

// Connect establishes a connection to RabbitMQ with automatic reconnection
func Connect(ctx context.Context, config Config) (*amqp.Connection, error) {
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = 5 * time.Second
	}
	if config.MaxReconnectTime == 0 {
		config.MaxReconnectTime = 5 * time.Minute
	}

	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	connLock.Lock()
	RMQConn = conn
	connLock.Unlock()

	// Monitor connection and reconnect if needed
	go monitorConnection(ctx, config)

	return conn, nil
}

// monitorConnection watches for connection issues and attempts to reconnect
func monitorConnection(ctx context.Context, config Config) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-RMQConn.NotifyClose(make(chan *amqp.Error)):
			if err != nil {
				fmt.Printf("RabbitMQ connection closed: %v. Attempting to reconnect...\n", err)
				reconnect(ctx, config)
			}
		}
	}
}

// reconnect attempts to reconnect to RabbitMQ with exponential backoff
func reconnect(ctx context.Context, config Config) {
	delay := config.ReconnectDelay
	maxDelay := config.MaxReconnectTime

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
			conn, err := amqp.Dial(config.URL)
			if err != nil {
				fmt.Printf("Failed to reconnect to RabbitMQ: %v. Retrying in %v...\n", err, delay)
				delay *= 2
				if delay > maxDelay {
					delay = maxDelay
				}
				continue
			}

			connLock.Lock()
			RMQConn = conn
			connLock.Unlock()

			fmt.Println("Successfully reconnected to RabbitMQ")
			go monitorConnection(ctx, config)
			return
		}
	}
}

// GetConnection returns the current RabbitMQ connection
func GetConnection() *amqp.Connection {
	connLock.RLock()
	defer connLock.RUnlock()
	return RMQConn
}

// DeclareQueue declares a queue with standard settings
func DeclareQueue(ch *amqp.Channel, queueName string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
}

// DeclareExchange declares an exchange
func DeclareExchange(ch *amqp.Channel, exchangeName, exchangeType string) error {
	return ch.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
}

// BindQueue binds a queue to an exchange with a routing key
func BindQueue(ch *amqp.Channel, queueName, routingKey, exchangeName string) error {
	return ch.QueueBind(
		queueName,    // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,
		nil,
	)
}

// Publish publishes a message to a queue or exchange
func Publish(ch *amqp.Channel, exchange, routingKey string, body []byte, headers amqp.Table) error {
	return ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
		},
	)
}

// Close closes the RabbitMQ connection
func Close() error {
	connLock.Lock()
	defer connLock.Unlock()

	if RMQConn != nil && !RMQConn.IsClosed() {
		return RMQConn.Close()
	}
	return nil
}
