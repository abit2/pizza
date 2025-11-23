package db

import "fmt"

var queueTemplate = "pizza:%s:%s"
var queueLeaseTemplate = "pizza:lease:%s:%020d:%s"

func keyPauseQueue(queue []byte) []byte {
	key := fmt.Sprintf(queueTemplate, string(queue), "paused")
	return []byte(key)
}

func keyPendingQueue(queue []byte) []byte {
	key := fmt.Sprintf(queueTemplate, string(queue), "pending")
	return []byte(key)
}

func keyLeaseQueue(now int64, queue []byte, taskID string) []byte {
	key := fmt.Sprintf(queueLeaseTemplate, string(queue), now, taskID)
	return []byte(key)
}

func keyActiveQueue(queue []byte) []byte {
	key := fmt.Sprintf(queueTemplate, string(queue), "active")
	return []byte(key)
}

func keySeq(queueWithState []byte) []byte {
	key := fmt.Sprintf("seq:%s", string(queueWithState))
	return []byte(key)
}

func keyTask(taskID string) []byte {
	key := fmt.Sprintf("task:%s", taskID)
	return []byte(key)
}
