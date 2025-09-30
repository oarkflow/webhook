package webhook

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/json"
	"gopkg.in/yaml.v3"

	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/pkg/config"
)

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	RequestsPerMinute int      `json:"requests_per_minute" yaml:"requests_per_minute"`
	BurstSize         int      `json:"burst_size" yaml:"burst_size"`
	Enabled           bool     `json:"enabled" yaml:"enabled"`
	TrustedIPs        []string `json:"trusted_ips" yaml:"trusted_ips"`
}

// SecurityConfig holds security configuration
type SecurityConfig struct {
	MaxRequestSize int      `json:"max_request_size" yaml:"max_request_size"` // in bytes
	AllowedOrigins []string `json:"allowed_origins" yaml:"allowed_origins"`
	EnableCORS     bool     `json:"enable_cors" yaml:"enable_cors"`
	EnableAuditLog bool     `json:"enable_audit_log" yaml:"enable_audit_log"`
	RequestTimeout int      `json:"request_timeout" yaml:"request_timeout"` // in seconds
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	EnableMetrics     bool   `json:"enable_metrics" yaml:"enable_metrics"`
	MetricsPort       string `json:"metrics_port" yaml:"metrics_port"`
	EnableHealthCheck bool   `json:"enable_health_check" yaml:"enable_health_check"`
	LogLevel          string `json:"log_level" yaml:"log_level"`
}

// RetryPolicyConfig holds retry policy configuration
type RetryPolicyConfig struct {
	MaxRetries            int     `json:"max_retries" yaml:"max_retries"`
	InitialDelay          int     `json:"initial_delay" yaml:"initial_delay"` // in milliseconds
	MaxDelay              int     `json:"max_delay" yaml:"max_delay"`         // in milliseconds
	BackoffFactor         float64 `json:"backoff_factor" yaml:"backoff_factor"`
	EnableDeadLetterQueue bool    `json:"enable_dead_letter_queue" yaml:"enable_dead_letter_queue"`
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	MaxConnections    int  `json:"max_connections" yaml:"max_connections"`
	ConnectionTimeout int  `json:"connection_timeout" yaml:"connection_timeout"` // in seconds
	EnableAutoMigrate bool `json:"enable_auto_migrate" yaml:"enable_auto_migrate"`
}

// Config holds the configuration for the webhook server
type Config struct {
	Port         string                  `json:"port" yaml:"port"`
	MaxWorkers   int                     `json:"max_workers" yaml:"max_workers"`
	Secret       string                  `json:"secret" yaml:"secret"` // For HMAC verification
	DataTypes    []string                `json:"data_types" yaml:"data_types"`
	ETLConfig    string                  `json:"etl_config" yaml:"etl_config"`     // Path to ETL config file
	Integrations string                  `json:"integrations" yaml:"integrations"` // Path to integrations config file
	ETLPipelines map[string]*ETLPipeline `json:"etl_pipelines,omitempty" yaml:"etl_pipelines,omitempty"`
	Parsers      []string                `json:"parsers" yaml:"parsers"` // List of parser names to enable

	// Production-ready features
	RateLimit   RateLimitConfig   `json:"rate_limit" yaml:"rate_limit"`
	Security    SecurityConfig    `json:"security" yaml:"security"`
	Monitoring  MonitoringConfig  `json:"monitoring" yaml:"monitoring"`
	RetryPolicy RetryPolicyConfig `json:"retry_policy" yaml:"retry_policy"`
	Database    DatabaseConfig    `json:"database" yaml:"database"`
}

// ETLPipeline defines an ETL pipeline configuration for webhook processing
type ETLPipeline struct {
	Name        string              `json:"name" yaml:"name"`
	Description string              `json:"description" yaml:"description"`
	DataType    string              `json:"data_type" yaml:"data_type"` // hl7, json, xml, etc.
	Source      config.DataConfig   `json:"source" yaml:"source"`
	Destination config.DataConfig   `json:"destination" yaml:"destination"`
	Mapping     config.TableMapping `json:"mapping" yaml:"mapping"`
	Options     []etl.Option        `json:"options,omitempty" yaml:"options,omitempty"`
	Enabled     bool                `json:"enabled" yaml:"enabled"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Port:         "8080",
		MaxWorkers:   10,
		Secret:       "",
		DataTypes:    []string{"hl7", "json", "xml", "plain"},
		Parsers:      []string{"hl7", "json", "xml", "plain", "smpp"},
		ETLPipelines: make(map[string]*ETLPipeline),

		// Production-ready defaults
		RateLimit: RateLimitConfig{
			RequestsPerMinute: 1000,
			BurstSize:         100,
			Enabled:           true,
			TrustedIPs:        []string{"127.0.0.1", "::1"},
		},

		Security: SecurityConfig{
			MaxRequestSize: 10 * 1024 * 1024, // 10MB
			AllowedOrigins: []string{"*"},
			EnableCORS:     false,
			EnableAuditLog: true,
			RequestTimeout: 30,
		},

		Monitoring: MonitoringConfig{
			EnableMetrics:     true,
			MetricsPort:       "9090",
			EnableHealthCheck: true,
			LogLevel:          "info",
		},

		RetryPolicy: RetryPolicyConfig{
			MaxRetries:            3,
			InitialDelay:          1000,
			MaxDelay:              30000,
			BackoffFactor:         2.0,
			EnableDeadLetterQueue: true,
		},

		Database: DatabaseConfig{
			MaxConnections:    50,
			ConnectionTimeout: 30,
			EnableAutoMigrate: true,
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(path string) (*Config, error) {
	ext := filepath.Ext(path)
	switch ext {
	case ".yaml", ".yml":
		return LoadYaml(path)
	case ".json":
		return LoadJson(path)
	case ".bcl":
		return LoadBCL(path)
	}
	return nil, fmt.Errorf("unsupported config format: %s", ext)
}

// LoadYaml loads configuration from a YAML file
func LoadYaml(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	return &cfg, err
}

// LoadJson loads configuration from a JSON file
func LoadJson(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = json.Unmarshal(data, &cfg)
	return &cfg, err
}

// LoadBCL loads configuration from a BCL file
func LoadBCL(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	_, err = bcl.Unmarshal(data, &cfg)
	return &cfg, err
}
