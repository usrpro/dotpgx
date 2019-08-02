package dotpgx

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx"
)

var testConfig = Config{
	"name",
	"host",
	321,
	false,
	"user",
	"password",
	123,
	dbRuntime{},
}

func TestConnPoolConfig(t *testing.T) {
	c := testConfig
	exp := pgx.ConnPoolConfig{
		MaxConnections: c.MaxConnections,
		ConnConfig: pgx.ConnConfig{
			Database:      c.Name,
			Host:          c.Host,
			Port:          uint16(c.Port),
			User:          c.User,
			Password:      c.Password,
			RuntimeParams: nil,
		},
	}
	got := c.connPoolConfig()
	if !reflect.DeepEqual(got, exp) {
		t.Error("\nExpected:\n", exp, "\nGot:\n", got)
	}

	c.TLS = true
	got = c.connPoolConfig()
	if got.TLSConfig == nil {
		t.Fatal("TLSConfig nil")
	}
	if got.TLSConfig.ServerName != c.Host {
		t.Error("Expected:", c.Host, "Got:", got.TLSConfig.ServerName)
	}

	c.RunTime.AppName = "appname"
	got = c.connPoolConfig()
	a, ok := got.RuntimeParams["application_name"]
	if !ok {
		t.Fatal("Application name not set")
	}
	if a != c.RunTime.AppName {
		t.Error("Expected:", c.RunTime.AppName, "Got:", a)
	}
}

func TestInitDB(t *testing.T) {
	exp := "no such host"
	_, err := InitDB(testConfig, "")
	if err == nil || !strings.HasSuffix(err.Error(), exp) {
		t.Error("Expected error", exp, "Got:", err)
	}

	exp = "No files to parse"
	_, err = InitDB(Default, "_")
	if err == nil || err.Error() != exp {
		t.Error("Expected error", exp, "Got:", err)
	}

	if _, err = InitDB(Default, ""); err != nil {
		t.Error(err)
	}
}
