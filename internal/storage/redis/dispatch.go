package redis

import (
	"context"
	"encoding/json"

	goredis "github.com/redis/go-redis/v9"

	"taskforge.dev/internal/model"
)

type DispatchStore struct {
	client *goredis.Client
}

func NewDispatchStore(client *goredis.Client) *DispatchStore {
	return &DispatchStore{client: client}
}

func (s *DispatchStore) Enqueue(ctx context.Context, queueName string, task model.Task) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	return s.client.ZAdd(ctx, key(queueName), goredis.Z{
		Score:  float64(task.Priority),
		Member: payload,
	}).Err()
}

func (s *DispatchStore) Dequeue(ctx context.Context, queueName string, workerID string) (model.Task, error) {
	values, err := s.client.ZPopMin(ctx, key(queueName), 1).Result()
	if err != nil {
		return model.Task{}, err
	}
	if len(values) == 0 {
		return model.Task{}, model.ErrTaskNotFound
	}

	member, ok := values[0].Member.(string)
	if !ok {
		return model.Task{}, model.ErrTaskNotFound
	}

	var task model.Task
	if err := json.Unmarshal([]byte(member), &task); err != nil {
		return model.Task{}, err
	}
	return task, nil
}

func (s *DispatchStore) Remove(ctx context.Context, queueName string, taskID string) error {
	values, err := s.client.ZRange(ctx, key(queueName), 0, -1).Result()
	if err != nil {
		return err
	}
	for _, value := range values {
		var task model.Task
		if err := json.Unmarshal([]byte(value), &task); err != nil {
			continue
		}
		if task.ID == taskID {
			return s.client.ZRem(ctx, key(queueName), value).Err()
		}
	}
	return nil
}

func key(queueName string) string {
	return "taskforge:dispatch:" + queueName
}
