package log

type Logger struct {
	logger logInterface
}

type logInterface interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

func NewLogger(l logInterface) *Logger {
	return &Logger{
		logger: l,
	}
}

func (l *Logger) Info(msg string, keysAndValues ...any) {
	l.logger.Info(msg, keysAndValues...)
}

func (l *Logger) Warn(msg string, keysAndValues ...any) {
	l.logger.Warn(msg, keysAndValues...)
}

func (l *Logger) Error(msg string, keysAndValues ...any) {
	l.logger.Error(msg, keysAndValues...)
}

func (l *Logger) Debug(msg string, keysAndValues ...any) {
	l.logger.Debug(msg, keysAndValues...)
}
