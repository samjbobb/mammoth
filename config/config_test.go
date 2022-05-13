package config

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestConfig_Validate(t *testing.T) {
	type args struct {
		config Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "success",
			args: args{
				config: Config{
					Sync: SyncCfg{
						BatchMaxItems: 1000,
						BatchTimeout:  time.Second * 10,
						Tables:        []string{"one", "two"},
					},
					Postgres: PostgresCfg{
						Connection:             "postgres://username:password@localhost/db",
						SlotName:               "mammoth",
						StandbyMessageInterval: time.Second * 10,
					},
					Snowflake: SnowflakeCfg{
						Connection: "username:password@account/database/schema",
					},
				}},
			wantErr: nil,
		},
		{
			name: "incomplete config",
			args: args{
				config: Config{
					Sync: SyncCfg{
						BatchMaxItems: 1000,
						BatchTimeout:  time.Second * 10,
						Tables:        []string{"one", "two"},
					},
					Postgres: PostgresCfg{
						Connection:             "",
						SlotName:               "",
						StandbyMessageInterval: time.Second * 10,
					},
					Snowflake: SnowflakeCfg{
						Connection: "username:password@account/database/schema",
					},
				}},
			wantErr: errors.New("Postgres.Connection: non zero value required;Postgres.SlotName: non zero value required"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.args.config.Validate()
			if tt.wantErr == nil {
				assert.Nil(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr.Error())
			}
		})
	}
}
