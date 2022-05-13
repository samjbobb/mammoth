package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Sync      SyncCfg
	Postgres  PostgresCfg
	Snowflake SnowflakeCfg
	Logger    LoggerCfg
}

type SyncCfg struct {
	BatchMaxItems    int           `valid:"required"`
	BatchTimeout     time.Duration `valid:"required"`
	SkipAcknowledge  bool
	ProhibitDropSlot bool
	Tables           []string `valid:"required"`
}

type SnowflakeCfg struct {
	Connection string `valid:"required"`
	Database   string
	Schema     string
}

type LoggerCfg struct {
	Level string
	JSON  bool
}

type PostgresCfg struct {
	Connection             string        `valid:"required"`
	SlotName               string        `valid:"required"`
	StandbyMessageInterval time.Duration `valid:"required"`
}

var DefaultConfig = Config{
	Sync: SyncCfg{
		BatchMaxItems:    25000,
		BatchTimeout:     time.Second * 60,
		SkipAcknowledge:  false,
		ProhibitDropSlot: false,
	},
	Postgres: PostgresCfg{
		Connection:             "",
		SlotName:               "",
		StandbyMessageInterval: time.Second * 10,
	},
	Snowflake: SnowflakeCfg{
		Connection: "",
	},
	Logger: LoggerCfg{
		Level: "info",
		JSON:  false,
	},
}

var DefaultStreamToFileConfig = Config{
	Sync: SyncCfg{
		BatchMaxItems:    10000,
		BatchTimeout:     time.Second * 20,
		SkipAcknowledge:  true,
		ProhibitDropSlot: true,
	},
	Postgres: PostgresCfg{
		Connection:             "",
		SlotName:               "",
		StandbyMessageInterval: time.Second * 10,
	},
	Logger: LoggerCfg{
		Level: "info",
		JSON:  false,
	},
}

func (c *Config) Validate() error {
	_, err := govalidator.ValidateStruct(c)
	return err
}

func GetConf(defaultCfg Config, path string) (*Config, error) {
	cfg := defaultCfg
	viper.SetConfigFile(path)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	bindEnvs(cfg)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("error reading config: %w", err)
	}

	err = viper.Unmarshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into config struct: %w", err)
	}

	return &cfg, nil
}

func WriteExampleConfig(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	y := yaml.NewEncoder(f)
	defer y.Close()
	y.Encode(DefaultConfig)
	return nil
}

func bindEnvs(iface interface{}, parts ...string) {
	ifv := reflect.ValueOf(iface)
	ift := reflect.TypeOf(iface)
	for i := 0; i < ift.NumField(); i++ {
		fieldv := ifv.Field(i)
		t := ift.Field(i)
		name := strings.ToLower(t.Name)
		tag, exists := t.Tag.Lookup("mapstructure")
		if exists {
			name = tag
		}
		path := append(parts, name)
		switch fieldv.Kind() {
		case reflect.Struct:
			bindEnvs(fieldv.Interface(), path...)
		default:
			viper.BindEnv(strings.Join(path, "."))
		}
	}
}
