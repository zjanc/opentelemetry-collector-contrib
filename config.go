package filterspanprocessor

import (
	"go.opentelemetry.io/collector/config"
)

type FilterConfig struct {
}

type Config struct {
	// NOTE: fields to be in PascalCase for it to work!
	config.ProcessorSettings `mapstructure:",squash"`
	StartTimeOffset          uint64 `mapstructure:"start_time_offset"`
}

var _ config.Processor = (*Config)(nil)

func (cfg *Config) Validate() error {
	return nil
}
