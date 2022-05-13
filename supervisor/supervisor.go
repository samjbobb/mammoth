package supervisor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/samjbobb/mammoth/config"
	"github.com/samjbobb/mammoth/sync/service"
	"github.com/samjbobb/mammoth/sync/stream"
	"github.com/samjbobb/mammoth/target/snowflake"
	"github.com/samjbobb/mammoth/utils"
	"github.com/sirupsen/logrus"
)

type Supervisor struct{}

func NewSupervisor() *Supervisor {
	return &Supervisor{}
}

func (s *Supervisor) Run(ctx context.Context, configFile string) error {
	// handle interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(c)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	cfg, err := config.GetConf(config.DefaultConfig, configFile)
	if err != nil {
		logrus.WithError(err).Fatalln("getConf error")
	}
	if err = cfg.Validate(); err != nil {
		logrus.WithError(err).Fatalln("validate config error")
	}

	initLogger(cfg.Logger)

	for {
		pgxConn, replConn, err := initPgxConnections(ctx, cfg.Postgres)
		if err != nil {
			logrus.WithError(err).Fatalln("error creating postgres client")
		}
		sfConn, err := initSnowflakeConnection(cfg.Snowflake)
		if err != nil {
			logrus.WithError(err).Fatalln("error creating snowflake client")
		}
		snowflakePublisher, err := snowflake.NewTarget(ctx, sfConn, cfg.Snowflake.Database, cfg.Snowflake.Schema)
		if err != nil {
			logrus.WithError(err).Fatalln("error creating snowflake target")
		}
		logrus.Infoln(snowflakePublisher)
		srv, err := service.NewService(cfg, pgxConn, replConn, snowflakePublisher)
		if err != nil {
			logrus.WithError(err).Fatalln("error creating sync service")
		}
		if err := srv.Run(ctx); err == stream.ErrRelationChanged {
			logrus.Infoln("sleeping 10 seconds before restarting")
			time.Sleep(time.Second * 10)
		} else {
			if err := srv.Stop(ctx); err != nil {
				logrus.WithError(err).Fatalln("error stopping service")
			}
			return err
		}
	}
}

func StreamToFile(ctx context.Context, configFile string, outFile string) error {
	cfg, err := config.GetConf(config.DefaultStreamToFileConfig, configFile)
	if err != nil {
		logrus.WithError(err).Fatalln("getConf error")
	}
	// Skip validating config so that partial config (omitting snowflake config) can be used

	initLogger(cfg.Logger)

	pgxConn, replConn, err := initPgxConnections(ctx, cfg.Postgres)
	if err != nil {
		logrus.WithError(err).Fatalln("error creating postgres client")
	}
	defer pgxConn.Close()
	defer func() {
		utils.PanicIfErr(replConn.Close(ctx))
	}()

	srm := stream.NewStream(pgxConn, replConn, nil, cfg.Postgres.SlotName, 0, 0, cfg.Sync.SkipAcknowledge)

	return srm.StreamToJSONLines(ctx, outFile)
}

// initLogger sets up logrus from the config
func initLogger(cfg config.LoggerCfg) {
	if cfg.JSON {
		logrus.SetFormatter(&logrus.JSONFormatter{})
		logrus.SetReportCaller(true)
	}
	level, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		logrus.Warnln("invalid log level in config", cfg.Level)
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)
	logrus.SetOutput(os.Stdout)
}

// initPgxConnections initialize db and replication connections.
func initPgxConnections(ctx context.Context, cfg config.PostgresCfg) (*pgxpool.Pool, *pgconn.PgConn, error) {
	var sep string
	if strings.Contains(cfg.Connection, "?") {
		sep = "&"
	} else {
		sep = "?"
	}
	replConnString := strings.Join([]string{cfg.Connection, "replication=database"}, sep)
	replConn, err := pgconn.Connect(ctx, replConnString)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres connection error: %w", err)
	}
	connConfig, err := pgxpool.ParseConfig(cfg.Connection)
	if err != nil {
		return nil, nil, err
	}
	connConfig.ConnConfig.Logger = logrusadapter.NewLogger(logrus.New())
	connConfig.ConnConfig.LogLevel = pgx.LogLevelWarn
	// Disable statement caching. Statement cache causes errors when running select * in the same connection
	// when the underlying table has changed
	connConfig.ConnConfig.BuildStatementCache = nil
	pgxConn, err := pgxpool.ConnectConfig(ctx, connConfig)
	return pgxConn, replConn, nil
}

func initSnowflakeConnection(cfg config.SnowflakeCfg) (*sql.DB, error) {
	joinChar := "?"
	if strings.Contains(cfg.Connection, "?") {
		joinChar = "&"
	}
	connStr := fmt.Sprintf("%s%sclient_session_keep_alive=true", cfg.Connection, joinChar)
	sf, err := sql.Open("snowflake", connStr)
	if err != nil {
		return nil, fmt.Errorf("could not connect to snowflake: %w", err)
	}
	return sf, nil
}
