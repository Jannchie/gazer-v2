package gazer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
)

// Tasker is the client of gazer
type Tasker[T any] struct {
	name   string
	client *redis.Client
}

type TaskerOptions struct {
	Client *redis.Client
	Name   string
}

// New create a new gazer client
func NewTasker[T any](options *TaskerOptions) *Tasker[T] {
	if options.Name == "" {
		options.Name = "gazer"
	}
	return &Tasker[T]{
		client: options.Client,
		name:   options.Name,
	}
}

// getFullKey get the full key of the task
func (t Tasker[T]) getFullKey(key string) string {
	return t.name + ":tasks:" + key
}

// RPushTask push task to the tail of the queue
func (t *Tasker[T]) RPushTask(ctx context.Context, task *Task[T]) error {
	data, err := json.Marshal(task.Params)
	if err != nil {
		return err
	}
	result := t.client.RPush(ctx, t.getFullKey(task.Key), data)
	return result.Err()
}

func (t *Tasker[T]) RPushTasks(ctx context.Context, tasks []*Task[T]) error {
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
func (t *Tasker[T]) LPushTask(ctx context.Context, task *Task[T]) error {
	result := t.client.LPush(ctx, t.getFullKey(task.Key), task.Params)
	return result.Err()
}
