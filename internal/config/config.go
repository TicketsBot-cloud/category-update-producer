package config

import (
	"github.com/caarlos0/env/v11"
	"go.uber.org/zap/zapcore"
	"time"
)

type Config struct {
	SentryDsn *string       `env:"SENTRY_DSN"`
	JsonLogs  bool          `env:"JSON_LOGS"`
	LogLevel  zapcore.Level `env:"LOG_LEVEL" envDefault:"info"`

	ExecutionTimeout  time.Duration `env:"EXECUTION_TIMEOUT" envDefault:"5m"`
	RunFrequency      time.Duration `env:"RUN_FREQUENCY" envDefault:"10m"`
	MoveCategoryAfter time.Duration `env:"MOVE_CATEGORY_AFTER" envDefault:"10m"`

	Database struct {
		Uri string `env:"URI,required"`
	} `envPrefix:"DATABASE_"`

	Kafka struct {
		Brokers []string `env:"BROKERS,required" envSeparator:","`
		Topic   string   `env:"TOPIC,required"`
	} `envPrefix:"KAFKA_"`
}

func LoadFromEnv() (Config, error) {
	return env.ParseAs[Config]()
}
