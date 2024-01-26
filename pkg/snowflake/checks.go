package snowflake

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type NotNullCheck struct {
	conn connectionFetcher
}

func CastQualityCheckResultToInteger(check string, res [][]interface{}) (int64, error) {
	if len(res) != 1 || len(res[0]) != 1 {
		return 0, errors.Errorf("multiple results are returned from query, please make sure your checks just expect one value. Check: '%s' - Value: %v", check, res)
	}

	switch v := res[0][0].(type) {
	case nil:
		return 0, errors.Errorf("unexpected result from query during %s check, result is nil", check)
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		atoi, err := strconv.Atoi(v)
		if err != nil {
			return 0, errors.Errorf("unexpected result from query during %s check, cannot cast result string to integer: %v", check, res)
		}

		return int64(atoi), nil
	}

	return 0, errors.Errorf("unexpected result from query during %s check, cannot cast result to integer: %v", check, res)
}

func (c *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", ti.GetAsset().Name, ti.Column.Name)

	return (&countableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       &query.Query{Query: qq},
		checkName:           "not_null",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d null values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type PositiveCheck struct {
	conn connectionFetcher
}

func (c *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s <= 0", ti.GetAsset().Name, ti.Column.Name)
	return (&countableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       &query.Query{Query: qq},
		checkName:           "positive",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-positive values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type UniqueCheck struct {
	conn connectionFetcher
}

func (c *UniqueCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT(%s) - COUNT(DISTINCT %s) FROM %s", ti.Column.Name, ti.Column.Name, ti.GetAsset().Name)
	return (&countableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       &query.Query{Query: qq},
		checkName:           "unique",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-unique values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type AcceptedValuesCheck struct {
	conn connectionFetcher
}

func (c *AcceptedValuesCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.StringArray == nil && ti.Check.Value.IntArray == nil {
		return errors.Errorf("unexpected value for accepted_values check, the values must to be an array, instead %T", ti.Check.Value)
	}

	if ti.Check.Value.StringArray != nil && len(*ti.Check.Value.StringArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	if ti.Check.Value.IntArray != nil && len(*ti.Check.Value.IntArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	var val []string
	if ti.Check.Value.StringArray != nil {
		val = *ti.Check.Value.StringArray
	} else {
		for _, v := range *ti.Check.Value.IntArray {
			val = append(val, strconv.Itoa(v))
		}
	}

	res := strings.Join(val, "','")
	res = fmt.Sprintf("'%s'", res)

	qq := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE CAST(%s as STRING) NOT IN (%s)", ti.GetAsset().Name, ti.Column.Name, res)
	return (&countableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       &query.Query{Query: qq},
		checkName:           "accepted_values",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d rows that are not in the accepted values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type countableQueryCheck struct {
	conn                connectionFetcher
	expectedQueryResult int64
	queryInstance       *query.Query
	checkName           string
	customError         func(count int64) error
}

func (c *countableQueryCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	return c.check(ctx, ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset()))
}

func (c *countableQueryCheck) CustomCheck(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	return c.check(ctx, ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset()))
}

func (c *countableQueryCheck) check(ctx context.Context, connectionName string) error {
	q, err := c.conn.GetSfConnection(connectionName)
	if err != nil {
		return errors.Wrapf(err, "failed to get connection for '%s' check", c.checkName)
	}

	res, err := q.Select(ctx, c.queryInstance)
	if err != nil {
		return errors.Wrapf(err, "failed '%s' check", c.checkName)
	}

	count, err := CastQualityCheckResultToInteger(c.checkName, res)
	if err != nil {
		return err
	}

	if count != c.expectedQueryResult {
		return c.customError(count)
	}

	return nil
}

type CustomCheck struct {
	conn connectionFetcher
}

func (c *CustomCheck) Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	return (&countableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: ti.Check.Value,
		queryInstance:       &query.Query{Query: ti.Check.Query},
		checkName:           ti.Check.Name,
		customError: func(count int64) error {
			return errors.Errorf("custom check '%s' has returned %d instead of the expected %d", ti.Check.Name, count, ti.Check.Value)
		},
	}).CustomCheck(ctx, ti)
}
