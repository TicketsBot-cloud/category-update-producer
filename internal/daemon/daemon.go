package daemon

import (
	"context"
	"github.com/TicketsBot/category-update-producer/internal/config"
	"github.com/TicketsBot/common/model"
	"github.com/TicketsBot/common/rpc"
	rpcmodel "github.com/TicketsBot/common/rpc/model"
	"github.com/TicketsBot/database"
	"go.uber.org/zap"
	"time"
)

type Daemon struct {
	logger *zap.Logger
	config config.Config
	db     *database.Database
	rpc    *rpc.Client

	shutdownCh chan struct{}
}

func NewDaemon(logger *zap.Logger, config config.Config, db *database.Database, rpc *rpc.Client) *Daemon {
	return &Daemon{
		logger:     logger,
		config:     config,
		db:         db,
		rpc:        rpc,
		shutdownCh: make(chan struct{}),
	}
}

func (d *Daemon) Start() {
	d.logger.Info("Starting daemon")

	ticker := time.NewTicker(d.config.RunFrequency)
	for {
		select {
		case <-d.shutdownCh:
			d.logger.Info("Shutting down daemon")
			return
		case <-ticker.C:
			d.RunOnce()
		}
	}
}

func (d *Daemon) RunOnce() {
	d.logger.Debug("Running...")

	ctx, cancel := context.WithTimeout(context.Background(), d.config.ExecutionTimeout)
	defer cancel()

	start := time.Now()

	tickets, err := d.db.CategoryUpdateQueue.GetReadyForUpdate(ctx, d.config.MoveCategoryAfter)
	if err != nil {
		d.logger.Error("Failed to get tickets", zap.Error(err))
		return
	}

	for _, ticket := range tickets {
		if ticket.ChannelId == nil {
			d.logger.Warn("Channel ID is nil", zap.Uint64("guild_id", ticket.GuildId), zap.Int("ticket_id", ticket.TicketId))
			continue
		}

		if ticket.PanelId == nil {
			d.logger.Warn("Panel ID is nil", zap.Uint64("guild_id", ticket.GuildId), zap.Int("ticket_id", ticket.TicketId))
			continue
		}

		// Get panel so we can get the new category ID
		panel, err := d.db.Panel.GetById(ctx, *ticket.PanelId)
		if err != nil {
			d.logger.Error("Failed to get panel", zap.Error(err))
			continue
		}

		if panel.PanelId == 0 {
			d.logger.Info("Panel for ticket has been deleted", zap.Uint64("guild_id", ticket.GuildId), zap.Int("ticket_id", ticket.TicketId))
			continue
		}

		// If there is no pending category set, then disable the feature
		if panel.PendingCategory == nil {
			d.logger.Debug("No pending category set", zap.Uint64("guild_id", ticket.GuildId), zap.Int("ticket_id", ticket.TicketId))
			continue
		}

		var newCategoryId uint64
		switch ticket.NewStatus {
		case model.TicketStatusOpen:
			newCategoryId = panel.TargetCategory
		case model.TicketStatusPending:
			newCategoryId = *panel.PendingCategory
		}

		if err := d.rpc.ProduceSyncJson(ctx, d.config.Kafka.Topic, rpcmodel.TicketStatusUpdate{
			Ticket: rpcmodel.Ticket{
				GuildId: ticket.GuildId,
				Id:      ticket.TicketId,
			},
			ChannelId:     *ticket.ChannelId,
			NewCategoryId: newCategoryId,
		}); err != nil {
			d.logger.Error("Failed to send message to Kafka", zap.Error(err))
			continue
		}

		d.logger.Info(
			"Sent category update command",
			zap.Uint64("guild_id", ticket.GuildId),
			zap.Int("ticket_id", ticket.TicketId),
			zap.Uint64("new_category", newCategoryId),
		)
	}

	if time.Now().Sub(start) > (d.config.ExecutionTimeout / 2) {
		d.logger.Warn("Execution took more than 50% of the timeout", zap.Duration("duration", time.Now().Sub(start)))
		return
	}
}

func (d *Daemon) Shutdown() {
	close(d.shutdownCh)
}
