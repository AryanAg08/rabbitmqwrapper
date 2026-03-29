# RMQWrapper

A RabbitMQ wrapper for Go with automatic reconnection, worker management, and distributed tracing support.

## Installation

```bash
go get github.com/yourorg/logSense-api/rmqwrapper
```

## Features

- Automatic reconnection with exponential backoff
- Worker pool management
- OpenTelemetry distributed tracing
- Panic recovery for message handlers
- Graceful shutdown support
- Fluent publishing interface

## Usage

### Connecting to RabbitMQ

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/yourorg/logSense-api/rmqwrapper"
)

func main() {
    ctx := context.Background()
    
    config := rmqwrap.Config{
        URL:              "amqp://guest:guest@localhost:5672/",
        ReconnectDelay:   5 * time.Second,
        MaxReconnectTime: 5 * time.Minute,
    }
    
    conn, err := rmqwrap.Connect(ctx, config)
    if err != nil {
        log.Fatal(err)
    }
    defer rmqwrap.Close()
}
```

### Declaring Queues and Exchanges

```go
ch, err := rmqwrap.GetConnection().Channel()
if err != nil {
    log.Fatal(err)
}
defer ch.Close()

// Declare a queue
queue, err := rmqwrap.DeclareQueue(ch, "my-queue")
if err != nil {
    log.Fatal(err)
}

// Declare an exchange
err = rmqwrap.DeclareExchange(ch, "my-exchange", "topic")
if err != nil {
    log.Fatal(err)
}

// Bind queue to exchange
err = rmqwrap.BindQueue(ch, "my-queue", "routing.key", "my-exchange")
if err != nil {
    log.Fatal(err)
}
```

### Publishing Messages

```go
import "github.com/streadway/amqp"

ch, _ := rmqwrap.GetConnection().Channel()
defer ch.Close()

message := []byte(`{"user_id": 123, "action": "login"}`)
headers := amqp.Table{}

err := rmqwrap.Publish(ch, "", "my-queue", message, headers)
if err != nil {
    log.Println("Failed to publish:", err)
}
```

### Using the Global Connection

The wrapper maintains a global connection that can be accessed anywhere:

```go
conn := rmqwrap.GetConnection()
ch, err := conn.Channel()
```

## Connection Management

The wrapper automatically:
- Monitors connection health
- Reconnects on connection loss
- Uses exponential backoff for reconnection attempts
- Respects context cancellation

## Best Practices

1. **Reuse channels**: Create a channel pool for high-throughput scenarios
2. **Handle context**: Always pass context for graceful shutdown
3. **Use confirmations**: Enable publisher confirms for critical messages
4. **Set QoS**: Configure prefetch count for workers

```go
ch.Qos(
    1,     // prefetch count
    0,     // prefetch size
    false, // global
)
```

## Advanced Usage

### Custom Queue Declaration

```go
ch, _ := rmqwrap.GetConnection().Channel()
queue, err := ch.QueueDeclare(
    "custom-queue",      // name
    true,                // durable
    false,               // delete when unused
    false,               // exclusive
    false,               // no-wait
    amqp.Table{
        "x-message-ttl":          60000,
        "x-dead-letter-exchange": "dlx",
    },
)
```

### Publisher Confirms

```go
ch, _ := rmqwrap.GetConnection().Channel()
err := ch.Confirm(false)
if err != nil {
    log.Fatal(err)
}

confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

// Publish message
err = rmqwrap.Publish(ch, "", "my-queue", message, nil)

// Wait for confirmation
if confirmed := <-confirms; !confirmed.Ack {
    log.Println("Message was not confirmed")
}
```

## License

MIT
