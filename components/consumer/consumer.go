package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
)

type GazerConsumer[R any] struct {
	client  *redis.Client
	name    string
	key     string
	handler func(*R) error
}

type GazerConsumerOption[R any] struct {
	Client  *redis.Client
	Name    string
	Key     string
	Handler func(*R) error
}

func New[R any](options *GazerConsumerOption[R]) *GazerConsumer[R] {
	if options.Client == nil {
		panic("Redis Client is nil")
	}
	if options.Key == "" {
		panic("Key is empty")
	}
	if options.Name == "" {
		options.Name = "gazer"
	}
	return &GazerConsumer[R]{
		client:  options.Client,
		key:     options.Key,
		name:    options.Name,
		handler: options.Handler,
	}
}

func (c GazerConsumer[R]) getFullKey(key string) string {
	return c.name + ":raws:" + key
}

func (c *GazerConsumer[R]) Consume() error {
	result := c.client.LPop(context.Background(), c.getFullKey(c.key))
	if result.Err() != nil {
		if result.Err().Error() == "redis: nil" {
			<-time.After(time.Second)
			return nil
		}
		return result.Err()
	}
	data := result.Val()
	if data == "" {
		return nil
	}
	t := new(R)
	err := json.Unmarshal([]byte(data), t)
	if err != nil {
		return err
	}
	err = c.handler(t)
	if err != nil {
		return err
	}
	return nil
}

// Run run the fetcher
func (c *GazerConsumer[R]) Run() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				err := c.Consume()
				if err != nil {
					fmt.Println(err)
					continue
				}
			}
		}
	}()
	<-stop
	fmt.Println("Gazer Consumer has been stopped.")
}
