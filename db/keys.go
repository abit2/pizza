package db

import "fmt"

var queueTemplate = "pizza:%s:%s"
var queueZSetTemplate = "pizza:%s:%s:%020d:%s"

// var queueLeaseTemplate = "pizza:%s:lease:%020d:%s"
var keyLeaseReferenceTemplate = "pizza:reference:%s:%s"
var keySeqTemplate = "seq:%s"
var keyTaskTemplate = "task:%s"

const leaseState = "lease"

func keyReference(queue []byte, taskID string) []byte {
	key := fmt.Sprintf(keyLeaseReferenceTemplate, string(queue), taskID)
	return []byte(key)
}

func keyQueue(queue, state []byte) []byte {
	key := fmt.Sprintf(queueTemplate, string(queue), string(state))
	return []byte(key)
}

func keyZSet(now int64, queue, state []byte, taskID string) []byte {
	key := fmt.Sprintf(queueZSetTemplate, string(queue), string(state), now, taskID)
	return []byte(key)
}

func keyPauseQueue(queue []byte) []byte {
	key := fmt.Sprintf(queueTemplate, string(queue), "paused")
	return []byte(key)
}

func keyLeaseQueue(now int64, queue []byte, taskID string) []byte {
	key := fmt.Sprintf(queueZSetTemplate, string(queue), "lease", now, taskID)
	return []byte(key)
}

func keySeq(queueWithState []byte) []byte {
	key := fmt.Sprintf(keySeqTemplate, string(queueWithState))
	return []byte(key)
}

func keyTask(taskID string) []byte {
	key := fmt.Sprintf(keyTaskTemplate, taskID)
	return []byte(key)
}
