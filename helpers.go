package pizza

import (
	"log/slog"
	"os"

	"github.com/abit2/pizza/log"
)

func slogLogger() *log.Logger {
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelDebug)
	return log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
	})))
}
