package utils

// NoopLogger implements the logInterface used by log.Logger without producing output.
type NoopLogger struct{}

func (n *NoopLogger) Debug(msg string, keysAndValues ...any) {}
func (n *NoopLogger) Info(msg string, keysAndValues ...any)  {}
func (n *NoopLogger) Warn(msg string, keysAndValues ...any)  {}
func (n *NoopLogger) Error(msg string, keysAndValues ...any) {}
