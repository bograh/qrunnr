package memory_test

import (
	"testing"

	"qrunnr.dev/internal/storage/memory"
	"qrunnr.dev/internal/storage/storetest"
)

func TestStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (storetest.QueueStore, storetest.TaskStore) {
		queues := memory.NewQueueStore()
		return queues, memory.NewTaskStore(queues)
	})
}
