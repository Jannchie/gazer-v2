package tasker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/jannchie/gazer-v2/common/models"
)

// GazerTasker is the client of gazer
type GazerTasker[T any] struct {
	name   string
	client *redis.Client
}

type GazerTaskerOptions struct {
	Client *redis.Client
	Name   string
}

// New create a new gazer client
func New[T any](options *GazerTaskerOptions) *GazerTasker[T] {
	if options.Name == "" {
		options.Name = "gazer"
	}
	return &GazerTasker[T]{
		client: options.Client,
		name:   options.Name,
	}
}

// getFullKey get the full key of the task
func (t GazerTasker[T]) getFullKey(key string) string {
	return t.name + ":tasks:" + key
}

// RPushTask push task to the tail of the queue
func (t *GazerTasker[T]) RPushTask(ctx context.Context, task *models.Task[T]) error {
	data, err := json.Marshal(task.Params)
	if err != nil {
		return err
	}
	result := t.client.RPush(ctx, t.getFullKey(task.Key), data)
	return result.Err()
}

func (t *GazerTasker[T]) RPushTasks(ctx context.Context, tasks []*models.Task[T]) error {
	pipe := t.client.Pipeline()
	for _, task := range tasks {
		data, err := json.Marshal(task.Params)
		if err != nil {
			fmt.Println(err)
			continue
		}
		pipe.RPush(ctx, t.getFullKey(task.Key), data)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// LPushTask push task to the head of the queue
func (t *GazerTasker[T]) LPushTask(ctx context.Context, task *models.Task[T]) error {
	result := t.client.LPush(ctx, t.getFullKey(task.Key), task.Params)
	return result.Err()
}
