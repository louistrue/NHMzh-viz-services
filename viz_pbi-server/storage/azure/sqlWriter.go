package azure

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"viz_pbi-server/logger"
	"viz_pbi-server/models"

	mssql "github.com/microsoft/go-mssqldb"
)

// SqlWriter handles writing messages to the database
type SqlWriter struct {
	db     *sql.DB
	logger *logger.Logger
}

// NewSqlWriter creates a new message writer
func NewSqlWriter(db *sql.DB) *SqlWriter {
	return &SqlWriter{
		db:     db,
		logger: logger.New(),
	}
}

// truncateRFC3339 parses an RFC3339 timestamp and truncates it to
// the precision used by DATETIME2(1).
func truncateRFC3339(ts string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}, err
	}
	return t.Truncate(time.Millisecond * 100), nil
}

// WriteMaterials writes an LCA message to the database
func (w *SqlWriter) WriteMaterials(dataItems []models.EavMaterialDataItem) error {
	return w.retryOnDeadlock("write lca message", func() error {
		return w.writeMaterialsWithRetry(dataItems)
	})
}

// WriteElements writes a Cost message to the database
func (w *SqlWriter) WriteElements(dataItems []models.EavElementDataItem) error {
	return w.retryOnDeadlock("write cost message", func() error {
		return w.writeElementsWithRetry(dataItems)
	})
}

// retryOnDeadlock retries the given function if a deadlock is detected
func (w *SqlWriter) retryOnDeadlock(operation string, fn func() error) error {
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		// Check for SQL Server deadlock error (1205)
		if sqlErr, ok := err.(mssql.Error); ok && sqlErr.Number == 1205 {
			w.logger.WithFields(logger.Fields{"error": err, "operation": operation, "attempt": attempt + 1, "maxRetries": maxRetries}).Warn("Deadlock detected, retrying...")
			time.Sleep(time.Millisecond * 100 * time.Duration(attempt+1))
			continue
		}

		return err
	}
	return fmt.Errorf("failed to %s after %d attempts", operation, maxRetries)
}

func (w *SqlWriter) writeElementsWithRetry(items []models.EavElementDataItem) error {
	ctx := context.Background()
	log := w.logger.WithFields(logger.Fields{
		"operation": "write_elements_bulk",
		"count":     len(items),
	})

	const (
		numColumns   = 10
		maxParams    = 2000 // SQL Server limit
		maxBatchSize = maxParams / numColumns
	)

	for start := 0; start < len(items); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]

		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			log.WithFields(logger.Fields{"error": err}).Error("Error starting transaction")
			return fmt.Errorf("error starting transaction: %v", err)
		}

		var (
			valueStrings []string
			valueArgs    = make(map[string]interface{})
			paramIdx     = 1
		)
		for _, item := range batch {
			params := []string{}
			truncatedTimestamp, err := truncateRFC3339(item.Timestamp)
			if err != nil {
				log.WithFields(logger.Fields{"error": err, "timestamp": item.Timestamp}).Error("Error parsing timestamp")
				return fmt.Errorf("error parsing timestamp: %v", err)
			}
			for _, val := range []interface{}{
				item.Project, item.Filename, truncatedTimestamp, item.Id, item.ParamName,
				item.ParamValueString, item.ParamValueNumber, item.ParamValueBoolean, item.ParamValueDate, item.ParamType,
			} {
				paramName := fmt.Sprintf("@p%d", paramIdx)
				params = append(params, paramName)
				valueArgs[paramName] = val
				paramIdx++
			}
			valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(params, ", ")))
		}

		if len(valueStrings) == 0 {
			tx.Rollback()
			continue
		}

		stmt := fmt.Sprintf(`
			MERGE INTO data_eav_elements WITH (HOLDLOCK) AS target
			USING (VALUES %s) AS source (project, filename, timestamp, id, param_name, param_value_string, param_value_number, param_value_boolean, param_value_date, param_type)
			ON target.project = source.project 
				AND target.filename = source.filename 
				AND target.timestamp = source.timestamp
				AND target.id COLLATE Latin1_General_CS_AS = source.id COLLATE Latin1_General_CS_AS
				AND target.param_name = source.param_name
			WHEN MATCHED THEN
				UPDATE SET 
					target.param_value_string = source.param_value_string,
					target.param_value_number = source.param_value_number,
					target.param_value_boolean = source.param_value_boolean,
					target.param_value_date = source.param_value_date,
					target.param_type = source.param_type
			WHEN NOT MATCHED THEN
				INSERT (project, filename, timestamp, id, param_name, param_value_string, param_value_number, param_value_boolean, param_value_date, param_type)
				VALUES (source.project, source.filename, source.timestamp, source.id, source.param_name, source.param_value_string, source.param_value_number, source.param_value_boolean, source.param_value_date, source.param_type);`, strings.Join(valueStrings, ", "))

		args := make([]interface{}, len(valueArgs))
		for i := 1; i <= len(valueArgs); i++ {
			args[i-1] = valueArgs[fmt.Sprintf("@p%d", i)]
		}
		_, err = tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			tx.Rollback()
			log.WithFields(logger.Fields{"error": err}).Error("Error inserting data_eav_elements records (bulk)")
			return fmt.Errorf("error inserting data_eav_elements records (bulk): %v", err)
		}

		if err := tx.Commit(); err != nil {
			log.WithFields(logger.Fields{"error": err}).Error("Error committing transaction")
			return fmt.Errorf("error committing transaction: %v", err)
		}
	}

	log.Info("Successfully wrote elements to database (bulk)")
	return nil
}

func (w *SqlWriter) writeMaterialsWithRetry(items []models.EavMaterialDataItem) error {
	ctx := context.Background()
	log := w.logger.WithFields(logger.Fields{
		"operation": "write_materials_bulk",
		"count":     len(items),
	})

	const (
		numColumns   = 11
		maxParams    = 2000 // SQL Server limit
		maxBatchSize = maxParams / numColumns
	)

	for start := 0; start < len(items); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]

		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			log.WithFields(logger.Fields{"error": err}).Error("Error starting transaction")
			return fmt.Errorf("error starting transaction: %v", err)
		}

		var (
			valueStrings []string
			valueArgs    = make(map[string]interface{})
			paramIdx     = 1
		)
		for _, item := range batch {
			params := []string{}
			truncatedTimestamp, err := truncateRFC3339(item.Timestamp)
			if err != nil {
				log.WithFields(logger.Fields{"error": err, "timestamp": item.Timestamp}).Error("Error parsing timestamp")
				return fmt.Errorf("error parsing timestamp: %v", err)
			}
			for _, val := range []interface{}{
				item.Project, item.Filename, truncatedTimestamp, item.Id, item.Sequence, item.ParamName,
				item.ParamValueString, item.ParamValueNumber, item.ParamValueBoolean, item.ParamValueDate, item.ParamType,
			} {
				paramName := fmt.Sprintf("@p%d", paramIdx)
				params = append(params, paramName)
				valueArgs[paramName] = val
				paramIdx++
			}
			valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(params, ", ")))
		}

		if len(valueStrings) == 0 {
			tx.Rollback()
			continue
		}

		stmt := fmt.Sprintf(`
			MERGE INTO data_eav_materials WITH (HOLDLOCK) AS target
			USING (VALUES %s) AS source (project, filename, timestamp, id, sequence, param_name, param_value_string, param_value_number, param_value_boolean, param_value_date, param_type)
			ON target.project = source.project 
				AND target.filename = source.filename 
				AND target.timestamp = source.timestamp
				AND target.id COLLATE Latin1_General_CS_AS = source.id COLLATE Latin1_General_CS_AS
				AND target.sequence = source.sequence
				AND target.param_name = source.param_name
			WHEN MATCHED THEN
				UPDATE SET 
					target.param_value_string = source.param_value_string,
					target.param_value_number = source.param_value_number,
					target.param_value_boolean = source.param_value_boolean,
					target.param_value_date = source.param_value_date,
					target.param_type = source.param_type
			WHEN NOT MATCHED THEN
				INSERT (project, filename, timestamp, id, sequence, param_name, param_value_string, param_value_number, param_value_boolean, param_value_date, param_type)
				VALUES (source.project, source.filename, source.timestamp, source.id, source.sequence, source.param_name, source.param_value_string, source.param_value_number, source.param_value_boolean, source.param_value_date, source.param_type);`, strings.Join(valueStrings, ", "))

		args := make([]interface{}, len(valueArgs))
		for i := 1; i <= len(valueArgs); i++ {
			args[i-1] = valueArgs[fmt.Sprintf("@p%d", i)]
		}
		_, err = tx.ExecContext(ctx, stmt, args...)
		if err != nil {
			tx.Rollback()
			log.WithFields(logger.Fields{"error": err}).Error("Error inserting data_eav_materials records (bulk)")
			return fmt.Errorf("error inserting data_eav_materials records (bulk): %v", err)
		}

		if err := tx.Commit(); err != nil {
			log.WithFields(logger.Fields{"error": err}).Error("Error committing transaction")
			return fmt.Errorf("error committing transaction: %v", err)
		}
	}

	log.Info("Successfully wrote materials to database (bulk)")
	return nil
}

func (w *SqlWriter) WriteBlobData(item models.BlobData) error {
	ctx := context.Background()
	log := w.logger.WithFields(logger.Fields{
		"operation": "write_blob_data",
		"project":   item.Project,
		"filename":  item.Filename,
		"container": item.Container,
	})

	// Begin transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		log.WithFields(logger.Fields{"error": err}).Error("Error starting transaction")
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Prepare the INSERT statement for data_updates table
	updateStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO data_updates (project, filename, timestamp, model_blob_storage_url, model_blob_storage_container, model_blob_id)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6);`)
	if err != nil {
		log.WithFields(logger.Fields{"error": err}).Error("Error preparing data_updates statement")
		return fmt.Errorf("error preparing data_updates statement: %v", err)
	}
	defer updateStmt.Close()

	truncatedTimestamp, err := truncateRFC3339(item.Timestamp)
	if err != nil {
		log.WithFields(logger.Fields{"error": err, "timestamp": item.Timestamp}).Error("Error parsing timestamp")
		return fmt.Errorf("error parsing timestamp: %v", err)
	}

	_, err = updateStmt.ExecContext(ctx,
		item.Project,           // @p1
		item.Filename,          // @p2
		truncatedTimestamp,     // @p3
		item.StorageServiceURL, // @p4
		item.Container,         // @p5
		item.BlobID,            // @p6
	)
	if err != nil {
		log.WithFields(logger.Fields{"error": err}).Error("Error inserting data_updates record")
		return fmt.Errorf("error inserting data_updates record: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		log.WithFields(logger.Fields{"error": err}).Error("Error committing transaction")
		return fmt.Errorf("error committing transaction: %v", err)
	}

	log.Info("Successfully wrote blob data to database")
	return nil
}
