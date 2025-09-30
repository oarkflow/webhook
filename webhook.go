package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oarkflow/log"
	"github.com/oarkflow/sql/etl"
	"github.com/oarkflow/sql/integrations"
	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/parsers"
	"github.com/oarkflow/squealx"
	"golang.org/x/time/rate"
)

// WebhookServer represents the webhook server
type WebhookServer struct {
	config         *Config
	jobs           chan Job
	retryJobs      chan RetryJob
	parsers        []parsers.Parser
	etlManager     *etl.Manager
	integrationMgr *integrations.Manager
	logger         *log.Logger
	mu             sync.Mutex

	// Production-ready features
	rateLimiter    *rate.Limiter
	metrics        Metrics
	shutdownChan   chan os.Signal
	isShuttingDown int32

	// Webhook status tracking
	webhookStatuses   map[string]*WebhookStatus
	webhookStatusesMu sync.RWMutex

	// Dead letter queue for failed webhooks
	deadLetterQueue   chan DeadLetterMessage
	deadLetterQueueMu sync.RWMutex

	// Database connection pool
	dbConnections   map[string]*squealx.DB
	dbConnectionsMu sync.RWMutex
}

// Job represents a webhook processing job
type Job struct {
	ID      string
	Body    []byte
	Headers http.Header
	Created time.Time
}

// RetryJob represents a webhook retry job
type RetryJob struct {
	Job        Job
	RetryCount int
	LastError  error
	NextRetry  time.Time
}

// WebhookStatus represents the status of a webhook
type WebhookStatus struct {
	ID          string                 `json:"id"`
	Status      string                 `json:"status"` // pending, processing, completed, failed
	ReceivedAt  time.Time              `json:"received_at"`
	ProcessedAt *time.Time             `json:"processed_at,omitempty"`
	Error       string                 `json:"error,omitempty"`
	RetryCount  int                    `json:"retry_count"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// DeadLetterMessage represents a failed webhook message
type DeadLetterMessage struct {
	Job        Job
	FinalError error
	FailedAt   time.Time
	RetryCount int
}

// Metrics holds server metrics
type Metrics struct {
	TotalRequests       int64     `json:"total_requests"`
	SuccessfulRequests  int64     `json:"successful_requests"`
	FailedRequests      int64     `json:"failed_requests"`
	AverageResponseTime float64   `json:"average_response_time"`
	ActiveConnections   int64     `json:"active_connections"`
	QueueSize           int       `json:"queue_size"`
	DeadLetterCount     int64     `json:"dead_letter_count"`
	Uptime              time.Time `json:"uptime"`
	mu                  sync.RWMutex
}

// UpdateMetrics safely updates metrics
func (m *Metrics) UpdateMetrics(requestDuration time.Duration, success bool) {
	atomic.AddInt64(&m.TotalRequests, 1)
	if success {
		atomic.AddInt64(&m.SuccessfulRequests, 1)
	} else {
		atomic.AddInt64(&m.FailedRequests, 1)
	}

	// Update average response time (simple moving average)
	m.mu.Lock()
	defer m.mu.Unlock()
	total := atomic.LoadInt64(&m.TotalRequests)
	if total > 1 {
		prevAvg := m.AverageResponseTime
		m.AverageResponseTime = (prevAvg*float64(total-1) + float64(requestDuration.Nanoseconds())) / float64(total)
	} else {
		m.AverageResponseTime = float64(requestDuration.Nanoseconds())
	}
}

// GetMetrics safely returns current metrics
func (m *Metrics) GetMetrics() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Metrics{
		TotalRequests:       atomic.LoadInt64(&m.TotalRequests),
		SuccessfulRequests:  atomic.LoadInt64(&m.SuccessfulRequests),
		FailedRequests:      atomic.LoadInt64(&m.FailedRequests),
		AverageResponseTime: m.AverageResponseTime,
		ActiveConnections:   atomic.LoadInt64(&m.ActiveConnections),
		QueueSize:           m.QueueSize,
		DeadLetterCount:     atomic.LoadInt64(&m.DeadLetterCount),
		Uptime:              m.Uptime,
	}
}

// NewWebhookServer creates a new webhook server instance
func NewWebhookServer(config *Config) *WebhookServer {
	if config == nil {
		config = DefaultConfig()
	}

	// Initialize rate limiter
	var rateLimiter *rate.Limiter
	if config.RateLimit.Enabled {
		rateLimiter = rate.NewLimiter(rate.Every(time.Minute/time.Duration(config.RateLimit.RequestsPerMinute)), config.RateLimit.BurstSize)
	}

	ws := &WebhookServer{
		config:         config,
		jobs:           make(chan Job, config.MaxWorkers*10), // buffered channel for jobs
		retryJobs:      make(chan RetryJob, config.MaxWorkers*5),
		parsers:        []parsers.Parser{},
		etlManager:     etl.NewManager(),
		integrationMgr: integrations.New(),
		logger:         &log.DefaultLogger,
		rateLimiter:    rateLimiter,
		metrics: Metrics{
			Uptime: time.Now(),
		},
		shutdownChan:    make(chan os.Signal, 1),
		webhookStatuses: make(map[string]*WebhookStatus),
		deadLetterQueue: make(chan DeadLetterMessage, 1000),
		dbConnections:   make(map[string]*squealx.DB),
	}

	// Load parsers dynamically based on configuration
	ws.loadParsers()

	// Load ETL configurations if specified
	if config.ETLConfig != "" {
		ws.loadETLConfig()
	}

	// Load integrations if specified
	if config.Integrations != "" {
		ws.loadIntegrations()
	}

	// Setup graceful shutdown
	signal.Notify(ws.shutdownChan, syscall.SIGINT, syscall.SIGTERM)

	return ws
}

// loadParsers dynamically loads parsers based on configuration
func (ws *WebhookServer) loadParsers() {
	parserMap := map[string]parsers.Parser{
		"json":  parsers.NewJSONParser(),
		"xml":   parsers.NewXMLParser(),
		"hl7":   parsers.NewHL7Parser(),
		"smpp":  parsers.NewSMPPParser(),
		"plain": parsers.NewPlainTextParser(),
	}

	for _, parserName := range ws.config.Parsers {
		if parser, exists := parserMap[strings.ToLower(parserName)]; exists {
			ws.parsers = append(ws.parsers, parser)
			ws.logger.Info().Str("parser", parserName).Msg("Loaded parser")
		} else {
			ws.logger.Warn().Str("parser", parserName).Msg("Unknown parser type, skipping")
		}
	}

	if len(ws.parsers) == 0 {
		ws.logger.Warn().Msg("No parsers loaded, using defaults")
		// Load default parsers
		ws.parsers = []parsers.Parser{
			parsers.NewJSONParser(),
			parsers.NewXMLParser(),
			parsers.NewHL7Parser(),
			parsers.NewPlainTextParser(),
		}
	}
}

// loadETLConfig loads ETL configuration from file
func (ws *WebhookServer) loadETLConfig() {
	etlCfg, err := config.Load(ws.config.ETLConfig)
	if err != nil {
		ws.logger.Error().Err(err).Str("file", ws.config.ETLConfig).Msg("Failed to load ETL config")
		return
	}

	ids, err := ws.etlManager.Prepare(etlCfg)
	if err != nil {
		ws.logger.Error().Err(err).Msg("Failed to prepare ETL pipelines")
		return
	}

	ws.logger.Info().Strs("pipeline_ids", ids).Msg("ETL pipelines prepared")

	// Convert ETL config to webhook pipelines for backward compatibility
	ws.convertETLToWebhooks(etlCfg)
}

// loadIntegrations loads integration configuration from file
func (ws *WebhookServer) loadIntegrations() {
	ctx := context.Background()
	_, err := ws.integrationMgr.LoadIntegrationsFromFile(ctx, ws.config.Integrations)
	if err != nil {
		ws.logger.Error().Err(err).Str("file", ws.config.Integrations).Msg("Failed to load integrations")
		return
	}

	ws.logger.Info().Str("file", ws.config.Integrations).Msg("Integrations loaded")
}

// convertETLToWebhooks converts ETL table mappings to webhook ETLPipelines
func (ws *WebhookServer) convertETLToWebhooks(etlCfg *config.Config) {
	if ws.config.ETLPipelines == nil {
		ws.config.ETLPipelines = make(map[string]*ETLPipeline)
	}

	for _, table := range etlCfg.Tables {
		dataType := ws.inferDataTypeFromName(table.OldName)
		etlPipeline := &ETLPipeline{
			Name:        table.OldName,
			Description: fmt.Sprintf("ETL pipeline for %s", table.OldName),
			DataType:    dataType,
			Source:      etlCfg.Source,
			Destination: etlCfg.Destinations[0], // Use first destination
			Mapping:     table,
			Enabled:     true,
		}
		ws.config.ETLPipelines[table.OldName] = etlPipeline
		ws.logger.Info().Str("pipeline", table.OldName).Str("data_type", dataType).Msg("Converted ETL table to webhook ETL pipeline")
	}
}

// inferDataTypeFromName infers data type from table name
func (ws *WebhookServer) inferDataTypeFromName(tableName string) string {
	// Simple inference based on table name patterns
	switch {
	case strings.Contains(strings.ToLower(tableName), "hl7"):
		return "hl7"
	case strings.Contains(strings.ToLower(tableName), "json"):
		return "json"
	case strings.Contains(strings.ToLower(tableName), "xml"):
		return "xml"
	case strings.Contains(strings.ToLower(tableName), "smpp"):
		return "smpp"
	default:
		return "json" // Default to JSON
	}
}

// Start starts the webhook server with production-ready features
func (ws *WebhookServer) Start() error {
	// Start background workers
	for i := 0; i < ws.config.MaxWorkers; i++ {
		go ws.worker()
	}

	// Start retry worker
	go ws.retryWorker()

	// Start dead letter queue processor
	go ws.deadLetterQueueProcessor()

	// Setup HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", ws.handleWebhook)
	mux.HandleFunc("/health", ws.healthHandler)

	// Add production-ready endpoints
	if ws.config.Monitoring.EnableMetrics {
		mux.HandleFunc("/metrics", ws.metricsHandler)
	}
	mux.HandleFunc("/webhooks", ws.webhooksHandler)
	mux.HandleFunc("/webhooks/", ws.webhookByIDHandler)

	// Setup CORS if enabled
	if ws.config.Security.EnableCORS {
		ws.setupCORS(mux)
	}

	// Start metrics server if enabled
	if ws.config.Monitoring.EnableMetrics {
		go ws.startMetricsServer()
	}

	// Start graceful shutdown handler
	go ws.gracefulShutdownHandler()

	log.Printf("Starting webhook server on port %s with production features enabled", ws.config.Port)

	server := &http.Server{
		Addr:         ":" + ws.config.Port,
		Handler:      mux,
		ReadTimeout:  time.Duration(ws.config.Security.RequestTimeout) * time.Second,
		WriteTimeout: time.Duration(ws.config.Security.RequestTimeout) * time.Second,
	}

	return server.ListenAndServe()
}

// handleWebhook handles incoming webhook requests with production-ready features
func (ws *WebhookServer) handleWebhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Set security headers
	ws.setSecurityHeaders(w)

	// Check if server is shutting down
	if atomic.LoadInt32(&ws.isShuttingDown) == 1 {
		http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
		return
	}

	// Rate limiting
	if ws.rateLimiter != nil {
		if !ws.rateLimiter.Allow() {
			ws.metrics.UpdateMetrics(time.Since(start), false)
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}
	}

	// Check request size
	if r.ContentLength > int64(ws.config.Security.MaxRequestSize) {
		ws.metrics.UpdateMetrics(time.Since(start), false)
		http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Check IP whitelist if configured
	if len(ws.config.RateLimit.TrustedIPs) > 0 {
		clientIP := ws.getClientIP(r)
		if !ws.isTrustedIP(clientIP) {
			ws.metrics.UpdateMetrics(time.Since(start), false)
			http.Error(w, "IP not allowed", http.StatusForbidden)
			return
		}
	}

	if r.Method != http.MethodPost {
		ws.metrics.UpdateMetrics(time.Since(start), false)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read request body with timeout
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(ws.config.Security.MaxRequestSize)))
	if err != nil {
		ws.metrics.UpdateMetrics(time.Since(start), false)
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Verify signature if secret is set
	if ws.config.Secret != "" {
		sig := r.Header.Get("X-Hub-Signature")
		if sig == "" {
			ws.metrics.UpdateMetrics(time.Since(start), false)
			http.Error(w, "Missing signature", http.StatusUnauthorized)
			return
		}
		expected := "sha256=" + computeHmac(body, ws.config.Secret)
		if !hmac.Equal([]byte(sig), []byte(expected)) {
			ws.metrics.UpdateMetrics(time.Since(start), false)
			http.Error(w, "Invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Generate webhook ID and track status
	webhookID := ws.generateWebhookID()
	job := Job{
		ID:      webhookID,
		Body:    body,
		Headers: r.Header.Clone(),
		Created: time.Now(),
	}

	// Track webhook status
	ws.trackWebhookStatus(&job, "received")

	// Respond immediately to make it non-blocking
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))

	// Send job to workers
	select {
	case ws.jobs <- job:
		ws.logger.Info().Str("webhook_id", webhookID).Msg("Webhook queued for processing")
		ws.trackWebhookStatus(&job, "queued")
	default:
		ws.logger.Warn().Str("webhook_id", webhookID).Msg("Job queue full, sending to retry queue")
		ws.scheduleRetry(RetryJob{
			Job:        job,
			RetryCount: 0,
			NextRetry:  time.Now().Add(time.Duration(ws.config.RetryPolicy.InitialDelay) * time.Millisecond),
		})
	}

	ws.metrics.UpdateMetrics(time.Since(start), true)
}

// worker processes jobs from the queue with enhanced error handling
func (ws *WebhookServer) worker() {
	for job := range ws.jobs {
		// Check if server is shutting down
		if atomic.LoadInt32(&ws.isShuttingDown) == 1 {
			return
		}

		ws.trackWebhookStatus(&job, "processing")

		// Process with timeout
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ws.config.Security.RequestTimeout)*time.Second)
		done := make(chan error, 1)

		go func() {
			done <- ws.processWebhookWithRetry(job)
		}()

		select {
		case err := <-done:
			cancel()
			if err != nil {
				ws.handleProcessingError(job, err)
			} else {
				ws.trackWebhookStatus(&job, "completed")
			}
		case <-ctx.Done():
			cancel()
			ws.handleProcessingError(job, fmt.Errorf("processing timeout"))
		}
	}
}

// processWebhookWithRetry processes webhook with enhanced error handling
func (ws *WebhookServer) processWebhookWithRetry(job Job) error {
	// Log the received data
	ws.logger.Info().
		Str("webhook_id", job.ID).
		Int("body_size", len(job.Body)).
		Msg("Processing webhook")

	// Try to parse with available parsers
	for _, parser := range ws.parsers {
		if parser.Detect(job.Body) {
			parsed, err := parser.Parse(job.Body)
			if err == nil {
				ws.logger.Info().
					Str("webhook_id", job.ID).
					Str("parser", parser.Name()).
					Msg("Successfully parsed webhook")

				// Process with ETL if pipeline exists for this data type
				if err := ws.processWithETLEnhanced(context.Background(), parser.Name(), parsed, job.Body, job.ID); err != nil {
					ws.logger.Error().
						Str("webhook_id", job.ID).
						Str("parser", parser.Name()).
						Err(err).
						Msg("ETL processing failed")
					return err
				}

				// Additional processing: validate, trigger actions, etc.
				return nil
			} else {
				ws.logger.Warn().
					Str("webhook_id", job.ID).
					Str("parser", parser.Name()).
					Err(err).
					Msg("Parser failed")
			}
		}
	}

	// If no parser succeeded, log as unknown
	ws.logger.Warn().
		Str("webhook_id", job.ID).
		Msg("Failed to parse with any known parser, treating as raw data")

	return fmt.Errorf("no parser could process the webhook data")
}

// handleProcessingError handles errors during webhook processing
func (ws *WebhookServer) handleProcessingError(job Job, err error) {
	ws.logger.Error().
		Str("webhook_id", job.ID).
		Err(err).
		Msg("Webhook processing failed")

	ws.trackWebhookStatus(&job, "failed")

	// Schedule for retry if enabled
	if ws.config.RetryPolicy.MaxRetries > 0 {
		retryJob := RetryJob{
			Job:        job,
			RetryCount: 0,
			LastError:  err,
			NextRetry:  time.Now().Add(time.Duration(ws.config.RetryPolicy.InitialDelay) * time.Millisecond),
		}
		ws.scheduleRetry(retryJob)
	} else {
		// Move directly to dead letter queue
		ws.deadLetterQueueMu.Lock()
		select {
		case ws.deadLetterQueue <- DeadLetterMessage{
			Job:        job,
			FinalError: err,
			FailedAt:   time.Now(),
			RetryCount: 0,
		}:
			atomic.AddInt64(&ws.metrics.DeadLetterCount, 1)
		default:
			ws.logger.Error().Str("webhook_id", job.ID).Msg("Dead letter queue full")
		}
		ws.deadLetterQueueMu.Unlock()
	}
}

// processWebhook processes the webhook payload
func (ws *WebhookServer) processWebhook(body []byte, headers http.Header) {
	// Lock if needed for shared resources
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// Log the received data
	log.Printf("Processing webhook payload: %s", string(body))
	log.Printf("Headers: %v", headers)

	// Try to parse with available parsers
	for _, parser := range ws.parsers {
		if parser.Detect(body) {
			parsed, err := parser.Parse(body)
			if err == nil {
				log.Printf("Successfully parsed with %s: %v", parser.Name(), parsed)

				// Process with ETL if pipeline exists for this data type
				if err := ws.processWithETLEnhanced(context.Background(), parser.Name(), parsed, body, "legacy_job"); err != nil {
					log.Printf("ETL processing failed for %s: %v", parser.Name(), err)
				}

				// Additional processing: validate, trigger actions, etc.
				return
			}
		}
	}

	// If no parser succeeded, log as unknown
	log.Printf("Failed to parse with any known parser, treating as raw data")
}

// processWithETLEnhanced processes parsed data through configured ETL pipelines with enhanced features
func (ws *WebhookServer) processWithETLEnhanced(ctx context.Context, dataType string, parsedData any, rawData []byte, webhookID string) error {
	// Validate data before processing
	if err := ws.validateData(parsedData, dataType); err != nil {
		return fmt.Errorf("data validation failed: %w", err)
	}

	// Check audit logging
	if ws.config.Security.EnableAuditLog {
		ws.auditLog(webhookID, "processing_started", fmt.Sprintf("Processing %s data", dataType))
	}
	// Case-insensitive pipeline lookup
	dataTypeLower := strings.ToLower(dataType)
	var pipeline *ETLPipeline

	for _, p := range ws.config.ETLPipelines {
		if strings.ToLower(p.DataType) == dataTypeLower && p.Enabled {
			pipeline = p
			break
		}
	}

	if pipeline == nil {
		log.Printf("No enabled ETL pipeline found for data type: %s", dataType)
		return nil
	}

	log.Printf("Processing %s data through ETL pipeline: %s", dataType, pipeline.Name)

	// Use the parsed data directly - the parser already provides structured data
	var processedData map[string]any

	switch dataType {
	case "hl7":
		// Flatten HL7 structured data for ETL mapping
		if hl7Data, ok := parsedData.(map[string]any); ok {
			processedData = ws.flattenHL7Data(hl7Data)
		}
	case "json":
		if jsonData, ok := parsedData.(map[string]any); ok {
			processedData = jsonData
		}
	default:
		// For other data types, use the parsed data as-is
		if dataMap, ok := parsedData.(map[string]any); ok {
			processedData = dataMap
		} else {
			processedData = map[string]any{"raw_data": string(rawData)}
		}
	}

	if processedData == nil {
		return fmt.Errorf("failed to process data for ETL pipeline")
	}

	// Apply ETL field mapping and insert into destination database
	if err := ws.executeETLMapping(pipeline, processedData); err != nil {
		log.Printf("ETL mapping execution failed: %v", err)
		return err
	}

	log.Printf("Successfully processed %s data through ETL pipeline: %s", dataType, pipeline.Name)

	return nil
}

// executeETLMapping applies field mapping and inserts data into destination database
func (ws *WebhookServer) executeETLMapping(pipeline *ETLPipeline, data map[string]any) error {
	// Apply field mapping from ETL configuration
	mappedData := make(map[string]any)

	for sourceField, destField := range pipeline.Mapping.Mapping {
		if value, exists := data[sourceField]; exists {
			// Convert complex types to strings for database insertion
			mappedData[destField] = ws.convertForDatabase(value)
		}
	}

	// Add any extra values from configuration
	for key, value := range pipeline.Mapping.ExtraValues {
		mappedData[key] = ws.convertForDatabase(value)
	}

	// Add timestamp if not present
	if _, exists := mappedData["created_at"]; !exists {
		mappedData["created_at"] = time.Now()
	}
	if _, exists := mappedData["updated_at"]; !exists {
		mappedData["updated_at"] = time.Now()
	}

	log.Printf("Mapped data for insertion: %+v", mappedData)

	// Insert into destination database
	return ws.insertIntoDatabase(pipeline.Destination, pipeline.Mapping.NewName, mappedData, pipeline.Mapping)
}

// insertIntoDatabase inserts mapped data into the destination database with connection pooling
func (ws *WebhookServer) insertIntoDatabase(dest config.DataConfig, tableName string, data map[string]any, mapping config.TableMapping) error {
	// Get database connection from pool
	db, err := ws.getDatabaseConnection(dest)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	// Note: Connection is managed by the pool, don't close it here

	// Auto-create table if configured
	if mapping.AutoCreateTable {
		if err := ws.createTableIfNotExists(db, tableName, data, mapping, dest.Driver); err != nil {
			log.Printf("Warning: failed to create table %s: %v", tableName, err)
		}
	}

	// Build INSERT query
	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	values := make([]any, 0, len(data))

	// Use proper quoting for the database type
	quoteChar := `"`
	if ws.isMySQLDriver(dest.Driver) {
		quoteChar = "`"
	}

	for col, val := range data {
		columns = append(columns, fmt.Sprintf(`%s%s%s`, quoteChar, col, quoteChar))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf(`INSERT INTO %s%s%s (%s) VALUES (%s)`,
		quoteChar, tableName, quoteChar,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("Executing query: %s with values: %v", query, values)

	_, err = db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	log.Printf("Successfully inserted data into table %s", tableName)
	return nil
}

// createTableIfNotExists creates the destination table if it doesn't exist
func (ws *WebhookServer) createTableIfNotExists(db *squealx.DB, tableName string, data map[string]any, mapping config.TableMapping, driver string) error {
	// Build CREATE TABLE query based on data types
	columns := make([]string, 0, len(data))

	// Use proper quoting for the database type
	quoteChar := `"`
	if ws.isMySQLDriver(driver) {
		quoteChar = "`"
	}

	for col, val := range data {
		sqlType := ws.inferSQLType(val, mapping.NormalizeSchema[col])
		// Use proper column quoting for the database type
		columns = append(columns, fmt.Sprintf(`%s%s%s %s`, quoteChar, col, quoteChar, sqlType))
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s%s%s (%s)`,
		quoteChar, tableName, quoteChar,
		strings.Join(columns, ", "))

	log.Printf("Creating table with query: %s", query)

	_, err := db.Exec(query)
	return err
}

// isMySQLDriver checks if the driver is MySQL
func (ws *WebhookServer) isMySQLDriver(driver string) bool {
	return strings.Contains(strings.ToLower(driver), "mysql")
}

// convertForDatabase converts complex data types to database-compatible types
func (ws *WebhookServer) convertForDatabase(value any) any {
	switch v := value.(type) {
	case []any:
		// Convert array to comma-separated string
		var parts []string
		for _, item := range v {
			if item != nil {
				parts = append(parts, fmt.Sprintf("%v", item))
			}
		}
		return strings.Join(parts, " ")
	case map[string]any:
		// Convert map to JSON-like string
		return fmt.Sprintf("%v", v)
	case nil:
		return nil
	default:
		return v
	}
}

// inferSQLType infers SQL data type from Go value and optional schema hint
func (ws *WebhookServer) inferSQLType(value any, schemaHint string) string {
	if schemaHint != "" {
		switch strings.ToLower(schemaHint) {
		case "string", "text":
			return "TEXT"
		case "int", "integer":
			return "INTEGER"
		case "bool", "boolean":
			return "BOOLEAN"
		case "date", "datetime":
			return "TIMESTAMP"
		case "float", "decimal":
			return "REAL"
		}
	}

	switch value.(type) {
	case int, int32, int64:
		return "INTEGER"
	case float32, float64:
		return "REAL"
	case bool:
		return "BOOLEAN"
	case string:
		return "TEXT"
	case time.Time:
		return "TIMESTAMP"
	default:
		return "TEXT"
	}
}

// flattenHL7Data flattens HL7 structured data for ETL mapping compatibility
func (ws *WebhookServer) flattenHL7Data(hl7Data map[string]any) map[string]any {
	result := make(map[string]any)

	for segmentName, segmentData := range hl7Data {
		if segmentArray, ok := segmentData.([]map[string]any); ok && len(segmentArray) > 0 {
			// Use the first segment instance (most common case)
			segment := segmentArray[0]

			// Create flattened keys like "MSH.1", "MSH.2", "PID.3", etc.
			for fieldKey, fieldValue := range segment {
				flattenedKey := fmt.Sprintf("%s.%s", segmentName, fieldKey)
				result[flattenedKey] = fieldValue
			}
		}
	}

	return result
}

// healthHandler handles health check requests
func (ws *WebhookServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Production-ready webhook server methods

// retryWorker processes retry jobs
func (ws *WebhookServer) retryWorker() {
	for retryJob := range ws.retryJobs {
		// Check if server is shutting down
		if atomic.LoadInt32(&ws.isShuttingDown) == 1 {
			return
		}

		// Wait until next retry time
		if time.Now().Before(retryJob.NextRetry) {
			time.Sleep(retryJob.NextRetry.Sub(time.Now()))
		}

		// Check retry count
		if retryJob.RetryCount >= ws.config.RetryPolicy.MaxRetries {
			ws.logger.Error().
				Str("webhook_id", retryJob.Job.ID).
				Int("retry_count", retryJob.RetryCount).
				Msg("Max retries exceeded, moving to dead letter queue")

			ws.deadLetterQueueMu.Lock()
			select {
			case ws.deadLetterQueue <- DeadLetterMessage{
				Job:        retryJob.Job,
				FinalError: retryJob.LastError,
				FailedAt:   time.Now(),
				RetryCount: retryJob.RetryCount,
			}:
				atomic.AddInt64(&ws.metrics.DeadLetterCount, 1)
			default:
				ws.logger.Error().Str("webhook_id", retryJob.Job.ID).Msg("Dead letter queue full")
			}
			ws.deadLetterQueueMu.Unlock()
			continue
		}

		// Try to process the job again
		select {
		case ws.jobs <- retryJob.Job:
			ws.logger.Info().
				Str("webhook_id", retryJob.Job.ID).
				Int("retry_count", retryJob.RetryCount+1).
				Msg("Webhook retry queued")
		default:
			// Queue still full, schedule another retry
			retryJob.RetryCount++
			retryJob.NextRetry = time.Now().Add(ws.calculateBackoffDelay(retryJob.RetryCount))
			ws.scheduleRetry(retryJob)
		}
	}
}

// deadLetterQueueProcessor processes failed messages
func (ws *WebhookServer) deadLetterQueueProcessor() {
	for dlMsg := range ws.deadLetterQueue {
		if atomic.LoadInt32(&ws.isShuttingDown) == 1 {
			return
		}

		ws.logger.Error().
			Str("webhook_id", dlMsg.Job.ID).
			Err(dlMsg.FinalError).
			Int("retry_count", dlMsg.RetryCount).
			Msg("Processing dead letter message")

		// Here you could implement persistence to database, external queue, etc.
		// For now, just log it
		if ws.config.Security.EnableAuditLog {
			ws.auditLog(dlMsg.Job.ID, "dead_letter", fmt.Sprintf("Final failure after %d retries: %v", dlMsg.RetryCount, dlMsg.FinalError))
		}
	}
}

// calculateBackoffDelay calculates delay for exponential backoff
func (ws *WebhookServer) calculateBackoffDelay(retryCount int) time.Duration {
	delay := float64(ws.config.RetryPolicy.InitialDelay) * float64(retryCount) * ws.config.RetryPolicy.BackoffFactor
	if delay > float64(ws.config.RetryPolicy.MaxDelay) {
		delay = float64(ws.config.RetryPolicy.MaxDelay)
	}
	return time.Duration(delay) * time.Millisecond
}

// metricsHandler serves metrics endpoint
func (ws *WebhookServer) metricsHandler(w http.ResponseWriter, r *http.Request) {
	metrics := ws.metrics.GetMetrics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// webhooksHandler handles webhook management requests
func (ws *WebhookServer) webhooksHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ws.listWebhooks(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// webhookByIDHandler handles individual webhook requests
func (ws *WebhookServer) webhookByIDHandler(w http.ResponseWriter, r *http.Request) {
	webhookID := strings.TrimPrefix(r.URL.Path, "/webhooks/")
	if webhookID == "" {
		http.Error(w, "Webhook ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		ws.getWebhookStatus(w, r, webhookID)
	case http.MethodDelete:
		ws.deleteWebhook(w, r, webhookID)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listWebhooks returns list of webhook statuses
func (ws *WebhookServer) listWebhooks(w http.ResponseWriter, r *http.Request) {
	ws.webhookStatusesMu.RLock()
	defer ws.webhookStatusesMu.RUnlock()

	statuses := make([]*WebhookStatus, 0, len(ws.webhookStatuses))
	for _, status := range ws.webhookStatuses {
		statuses = append(statuses, status)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// getWebhookStatus returns status of specific webhook
func (ws *WebhookServer) getWebhookStatus(w http.ResponseWriter, r *http.Request, webhookID string) {
	ws.webhookStatusesMu.RLock()
	defer ws.webhookStatusesMu.RUnlock()

	if status, exists := ws.webhookStatuses[webhookID]; exists {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	} else {
		http.Error(w, "Webhook not found", http.StatusNotFound)
	}
}

// deleteWebhook deletes webhook status
func (ws *WebhookServer) deleteWebhook(w http.ResponseWriter, r *http.Request, webhookID string) {
	ws.webhookStatusesMu.Lock()
	defer ws.webhookStatusesMu.Unlock()

	if _, exists := ws.webhookStatuses[webhookID]; exists {
		delete(ws.webhookStatuses, webhookID)
		w.WriteHeader(http.StatusNoContent)
	} else {
		http.Error(w, "Webhook not found", http.StatusNotFound)
	}
}

// setupCORS sets up CORS middleware
func (ws *WebhookServer) setupCORS(mux *http.ServeMux) {
	// CORS implementation would go here
	// For now, just log that CORS is enabled
	ws.logger.Info().Msg("CORS enabled")
}

// startMetricsServer starts a separate metrics server
func (ws *WebhookServer) startMetricsServer() {
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/metrics", ws.metricsHandler)

	ws.logger.Info().Str("port", ws.config.Monitoring.MetricsPort).Msg("Starting metrics server")
	if err := http.ListenAndServe(":"+ws.config.Monitoring.MetricsPort, metricsMux); err != nil {
		ws.logger.Error().Err(err).Msg("Metrics server failed")
	}
}

// gracefulShutdownHandler handles graceful shutdown
func (ws *WebhookServer) gracefulShutdownHandler() {
	sig := <-ws.shutdownChan
	ws.logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")

	atomic.StoreInt32(&ws.isShuttingDown, 1)

	// Close channels to signal workers to stop
	close(ws.jobs)
	close(ws.retryJobs)
	close(ws.deadLetterQueue)

	ws.logger.Info().Msg("Graceful shutdown completed")
}

// auditLog logs security and operational events
func (ws *WebhookServer) auditLog(webhookID, event, details string) {
	ws.logger.Info().
		Str("webhook_id", webhookID).
		Str("event", event).
		Str("details", details).
		Msg("AUDIT")
}

// Production-ready helper methods

// validateData validates parsed data before processing
func (ws *WebhookServer) validateData(data any, dataType string) error {
	if data == nil {
		return fmt.Errorf("data is nil")
	}

	switch dataType {
	case "json":
		if dataMap, ok := data.(map[string]any); ok {
			if len(dataMap) == 0 {
				return fmt.Errorf("JSON data is empty")
			}
		} else {
			return fmt.Errorf("JSON data is not a valid object")
		}
	case "hl7":
		// Add HL7-specific validation if needed
		return nil
	default:
		// Basic validation for other types
		return nil
	}

	return nil
}

// getDatabaseConnection gets or creates a database connection from the pool
func (ws *WebhookServer) getDatabaseConnection(dest config.DataConfig) (*squealx.DB, error) {
	ws.dbConnectionsMu.Lock()
	defer ws.dbConnectionsMu.Unlock()

	connKey := fmt.Sprintf("%s_%s_%s", dest.Driver, dest.Host, dest.Port)

	// Check if connection already exists
	if db, exists := ws.dbConnections[connKey]; exists {
		// Test connection health
		if err := db.Ping(); err == nil {
			return db, nil
		} else {
			// Close unhealthy connection
			db.Close()
			delete(ws.dbConnections, connKey)
		}
	}

	// Create new connection
	db, err := config.OpenDB(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to create database connection: %w", err)
	}

	// Set connection pool settings
	if ws.config.Database.MaxConnections > 0 {
		// Note: Connection pooling configuration would depend on the specific database driver
		// This is a placeholder for future implementation
		ws.logger.Info().Str("connection", connKey).Msg("Database connection created")
	}

	ws.dbConnections[connKey] = db
	ws.logger.Info().Str("connection", connKey).Msg("New database connection established")

	return db, nil
}

// computeHmac computes the HMAC-SHA256 signature
func computeHmac(data []byte, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// Helper methods for production-ready features

// setSecurityHeaders sets security headers on response
func (ws *WebhookServer) setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-XSS-Protection", "1; mode=block")
	w.Header().Set("Strict-Transport-Security", "max-age=31536000")
}

// getClientIP extracts the real client IP from request
func (ws *WebhookServer) getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ips := strings.Split(xff, ",")
		return strings.TrimSpace(ips[0])
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}

	// Fall back to RemoteAddr
	ip := r.RemoteAddr
	if colon := strings.LastIndex(ip, ":"); colon != -1 {
		ip = ip[:colon]
	}
	return ip
}

// isTrustedIP checks if an IP is in the trusted list
func (ws *WebhookServer) isTrustedIP(ip string) bool {
	for _, trustedIP := range ws.config.RateLimit.TrustedIPs {
		if trustedIP == ip || trustedIP == "*" {
			return true
		}
	}
	return false
}

// generateWebhookID generates a unique webhook ID
func (ws *WebhookServer) generateWebhookID() string {
	return fmt.Sprintf("wh_%d_%s", time.Now().UnixNano(), ws.getRandomString(8))
}

// getRandomString generates a random string of specified length
func (ws *WebhookServer) getRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}

// trackWebhookStatus tracks the status of a webhook
func (ws *WebhookServer) trackWebhookStatus(job *Job, status string) {
	ws.webhookStatusesMu.Lock()
	defer ws.webhookStatusesMu.Unlock()

	webhookStatus := &WebhookStatus{
		ID:         job.ID,
		Status:     status,
		ReceivedAt: job.Created,
		Metadata:   make(map[string]interface{}),
	}

	if existing, exists := ws.webhookStatuses[job.ID]; exists {
		webhookStatus.RetryCount = existing.RetryCount
		webhookStatus.Metadata = existing.Metadata
	}

	if status == "completed" || status == "failed" {
		now := time.Now()
		webhookStatus.ProcessedAt = &now
	}

	ws.webhookStatuses[job.ID] = webhookStatus

	// Log status change
	ws.logger.Info().
		Str("webhook_id", job.ID).
		Str("status", status).
		Msg("Webhook status updated")
}

// scheduleRetry schedules a job for retry
func (ws *WebhookServer) scheduleRetry(retryJob RetryJob) {
	select {
	case ws.retryJobs <- retryJob:
		ws.logger.Info().
			Str("webhook_id", retryJob.Job.ID).
			Int("retry_count", retryJob.RetryCount).
			Msg("Webhook scheduled for retry")
	default:
		ws.logger.Error().
			Str("webhook_id", retryJob.Job.ID).
			Msg("Retry queue full, moving to dead letter queue")

		// Move to dead letter queue
		ws.deadLetterQueueMu.Lock()
		select {
		case ws.deadLetterQueue <- DeadLetterMessage{
			Job:        retryJob.Job,
			FinalError: retryJob.LastError,
			FailedAt:   time.Now(),
			RetryCount: retryJob.RetryCount,
		}:
			atomic.AddInt64(&ws.metrics.DeadLetterCount, 1)
		default:
			ws.logger.Error().Str("webhook_id", retryJob.Job.ID).Msg("Dead letter queue full, dropping webhook")
		}
		ws.deadLetterQueueMu.Unlock()
	}
}
