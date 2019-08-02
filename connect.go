package dotpgx

import (
	"crypto/tls"

	"github.com/inconshreveable/log15"
	"github.com/jackc/pgx"
)

type dbRuntime struct {
	AppName string `usage:"Application name reported to PostgreSQL"`
}

// Config for database
type Config struct {
	Name           string `usage:"PostgreSQL database name"`
	Host           string `usage:"PostgreSQL host"`
	Port           uint   `usage:"Postgresql port number"`
	TLS            bool   `usage:"Enable TLS communication with database server"`
	User           string `usage:"PostgreSQL username"`
	Password       string `usage:"PostgreSQL password"`
	MaxConnections int    `usage:"Maximum DB connection pool size"`
	RunTime        dbRuntime
}

// Default config for database
var Default = Config{
	Name:           "dotpgx_test",
	Host:           "/run/postgresql",
	Port:           5432,
	User:           "postgres",
	MaxConnections: 5,
	RunTime: dbRuntime{
		AppName: "dotpgx connection lib",
	},
}

func (c Config) connPoolConfig() pgx.ConnPoolConfig {
	cpc := pgx.ConnPoolConfig{
		MaxConnections: c.MaxConnections,
		ConnConfig: pgx.ConnConfig{
			Database: c.Name,
			Host:     c.Host,
			Port:     uint16(c.Port),
			User:     c.User,
			Password: c.Password,
		},
	}
	if c.TLS {
		cpc.ConnConfig.TLSConfig = &tls.Config{
			ServerName: c.Host,
		}
	}
	if c.RunTime != (dbRuntime{}) {
		cpc.RuntimeParams = make(map[string]string)
		if c.RunTime.AppName != "" {
			cpc.RuntimeParams["application_name"] = c.RunTime.AppName
		}
	}
	return cpc
}

// InitDB is a wrapper for New() and ParsePath().
// Config is the dotpgx config, which will be parsed into a pgx.ConnPoolConfig.
// Path is where sql queries will be parsed from.
func InitDB(c Config, path string) (db *DB, err error) {
	if db, err = New(c.connPoolConfig()); err != nil {
		return
	}
	if path == "" {
		return
	}
	if err = db.ParsePath(path); err != nil {
		return
	}
	log15.Debug("Loaded sql", "queries", db.List())
	return
}
