package pizza

import "time"

const (
	heartBeatInterval         = 1 * time.Second
	heartBeatExtendBeforeExpr = 5 * time.Second

	heartBeatChannelBufferSize = 100

	defaultLeaseDuration = 30 * time.Second
)
