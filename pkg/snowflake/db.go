package snowflake

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
)

const (
	invalidQueryError = "SQL compilation error"
)

type DB struct {
	conn            *sqlx.DB
	config          *Config
	schemaNameCache *sync.Map
}

func NewDB(c *Config) (*DB, error) {
	dsn, err := c.DSN()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DSN")
	}

	gosnowflake.GetLogger().SetOutput(io.Discard)

	db, err := sqlx.Connect("snowflake", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to snowflake")
	}

	return &DB{
		conn:            db,
		config:          c,
		schemaNameCache: &sync.Map{},
	}, nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.Select(ctx, query)
	return err
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI()
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
	}

	queryString := query.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
	}

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		return nil, err
	}

	var result [][]interface{}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		result = append(result, columns)
	}
	return result, err
}

func (db *DB) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return false, errors.Wrap(err, "failed to create snowflake context")
	}

	rows, err := db.conn.QueryContext(ctx, query.ToExplainQuery())
	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		if strings.Contains(errorMessage, invalidQueryError) {
			errorSegments := strings.Split(errorMessage, "\n")
			if len(errorSegments) > 1 {
				err = errors.New(errorSegments[1])
			}
		}
	}

	if rows != nil {
		defer rows.Close()
	}

	return err == nil, err
}

// Test runs a simple query (SELECT 1) to validate the connection.
func (db *DB) Ping(ctx context.Context) error {
	// Define the test query
	q := query.Query{
		Query: "SELECT 1",
	}

	// Use the existing RunQueryWithoutResult method
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Snowflake connection")
	}

	return nil // Return nil if the query runs successfully
}

func (db *DB) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	// Prepare Snowflake context for the query execution
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snowflake context")
	}

	// Convert query object to string and execute it
	queryString := queryObj.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
		return nil, err
	}
	defer rows.Close()

	// Initialize the result struct
	result := &query.QueryResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}

	// Fetch column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve column names")
	}
	result.Columns = cols

	// Fetch rows and scan into result set
	for rows.Next() {
		row := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range row {
			columnPointers[i] = &row[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result.Rows = append(result.Rows, row)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	return result, nil
}

func (db *DB) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	switch len(tableComponents) {
	case 2:
		schemaName = strings.ToUpper(tableComponents[0])
	case 3:
		schemaName = strings.ToUpper(tableComponents[1])
	default:
		return nil
	}
	// Check the cache for the database
	if _, exists := db.schemaNameCache.Load(schemaName); exists {
		return nil
	}
	createQuery := query.Query{
		Query: "CREATE SCHEMA IF NOT EXISTS " + schemaName,
	}
	if err := db.RunQueryWithoutResult(ctx, &createQuery); err != nil {
		return errors.Wrapf(err, "failed to create or ensure database: %s", schemaName)
	}
	db.schemaNameCache.Store(schemaName, true)

	return nil
}

func (db *DB) RecreateTableOnMaterializationTypeMismatch(ctx context.Context, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	var tableName string
	switch len(tableComponents) {
	case 2:
		schemaName = strings.ToUpper(tableComponents[0])
		tableName = strings.ToUpper(tableComponents[1])
	case 3:
		schemaName = strings.ToUpper(tableComponents[1])
		tableName = strings.ToUpper(tableComponents[2])
	default:
		return errors.New("invalid asset name format")
	}

	queryStr := fmt.Sprintf(
		`SELECT TABLE_TYPE FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'`,
		db.config.Database, schemaName, tableName,
	)

	result, err := db.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return errors.Wrapf(err, "unable to retrieve table metadata for '%s.%s'", schemaName, tableName)
	}

	if len(result) == 0 {
		return errors.New(fmt.Sprintf("table or view %s.%s does not exist", schemaName, tableName))
	}

	var materializationType string
	if typeField, ok := result[0][0].(string); ok {
		materializationType = typeField
	}

	if materializationType == "" {
		return errors.New("could not determine the materialization type")
	}
	var dbMaterializationType pipeline.MaterializationType
	switch materializationType {
	case "BASE TABLE":
		dbMaterializationType = pipeline.MaterializationTypeTable
		materializationType = "TABLE"
	case "VIEW":
		dbMaterializationType = pipeline.MaterializationTypeView
	default:
		dbMaterializationType = pipeline.MaterializationTypeNone
	}
	if dbMaterializationType != asset.Materialization.Type {
		dropQuery := query.Query{
			Query: fmt.Sprintf("DROP %s IF EXISTS %s.%s", materializationType, schemaName, tableName),
		}

		if dropErr := db.RunQueryWithoutResult(ctx, &dropQuery); dropErr != nil {
			return errors.Wrapf(dropErr, "failed to drop existing %s: %s.%s", materializationType, schemaName, tableName)
		}
	}

	return nil
}
