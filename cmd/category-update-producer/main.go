package main

import (
	"context"
	"fmt"
	"github.com/TicketsBot/category-update-producer/internal/config"
	"github.com/TicketsBot/category-update-producer/internal/daemon"
	"github.com/TicketsBot/common/observability"
	"github.com/TicketsBot/common/rpc"
	"github.com/TicketsBot/database"
	"github.com/getsentry/sentry-go"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	config, err := config.LoadFromEnv()
	if err != nil {
		panic(err)
	}

	// Build logger
	if config.SentryDsn != nil {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn: *config.SentryDsn,
		}); err != nil {
			panic(fmt.Errorf("sentry.Init: %w", err))
		}
	}

	var logger *zap.Logger
	if config.JsonLogs {
		loggerConfig := zap.NewProductionConfig()
		loggerConfig.Level.SetLevel(config.LogLevel)

		logger, err = loggerConfig.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
			zap.WrapCore(observability.ZapSentryAdapter(observability.EnvironmentProduction)),
		)
	} else {
		loggerConfig := zap.NewDevelopmentConfig()
		loggerConfig.Level.SetLevel(config.LogLevel)
		loggerConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

		logger, err = loggerConfig.Build(zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	}

	if err != nil {
		panic(fmt.Errorf("failed to initialise zap logger: %w", err))
	}

	logger.Info("Connecting to database...")
	db, err := connectDatabase(config)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
		return
	}

	logger.Info("Database connected.")

	logger.Info("Starting RPC client")
	rpcClient, err := rpc.NewClient(
		logger.With(zap.String("service", "rpc-client")),
		rpc.Config{
			Brokers: config.Kafka.Brokers,
		},
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to start RPC client", zap.Error(err))
		return
	}

	logger.Info("RPC client started")

	d := daemon.NewDaemon(
		logger.With(zap.String("service", "daemon")),
		config,
		db,
		rpcClient,
	)

	go d.Start()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	logger.Info("Shutting down")
	d.Shutdown()
}

func connectDatabase(config config.Config) (*database.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.Connect(ctx, config.Database.Uri)
	if err != nil {
		return nil, err
	}

	return database.NewDatabase(pool), nil
}
