package ansisql

import (
	"context"
	"fmt"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"strings"
	"testing"
)

type mockConnectionRetriever struct {
	conn *mockConnection
	err  error
}

func (m *mockConnectionRetriever) GetConnection(_ string) (interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.conn, nil
}

type mockExtractor struct {
	ExtractQueriesFromStringFunc func(s string) ([]*query.Query, error)
}

func (m *mockExtractor) ExtractQueriesFromString(s string) ([]*query.Query, error) {
	if m.ExtractQueriesFromStringFunc != nil {
		return m.ExtractQueriesFromStringFunc(s)
	}
	return []*query.Query{{Query: s}}, nil
}

func (m *mockExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	return content, nil
}

func (m *mockExtractor) CloneForAsset(_ context.Context, _ *pipeline.Pipeline, _ *pipeline.Asset) query.QueryExtractor {
	return m
}

func TestQuerySensor_RunTask(t *testing.T) {
	type fields struct {
		connection connectionFetcher
		extractor  query.QueryExtractor
		sensorMode string
	}
	type args struct {
		ctx context.Context
		p   *pipeline.Pipeline
		t   *pipeline.Asset
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		errContains string
	}{
		{
			name: "success",
			fields: fields{
				connection: &mockConnectionRetriever{conn: &mockConnection{selectResult: [][]interface{}{{1}}, selectErr: nil}, err: nil},
				extractor:  &mockExtractor{},
				sensorMode: "",
			},
			args: args{
				ctx: context.Background(),
				t:   &pipeline.Asset{Parameters: map[string]string{"query": "SELECT 1"}},
			},
			wantErr: false,
		},
		{
			name: "missing query parameter",
			fields: fields{
				connection: &mockConnectionRetriever{conn: &mockConnection{}, err: nil},
				extractor:  &mockExtractor{},
				sensorMode: "",
			},
			args: args{
				ctx: context.Background(),
				p:   &pipeline.Pipeline{},
				t:   &pipeline.Asset{Parameters: map[string]string{}},
			},
			wantErr:     true,
			errContains: "query sensor requires a parameter named 'query'",
		},
		{
			name: "extractor error",
			fields: fields{
				connection: &mockConnectionRetriever{conn: &mockConnection{}, err: nil},
				extractor: &mockExtractor{
					ExtractQueriesFromStringFunc: func(s string) ([]*query.Query, error) {
						return nil, fmt.Errorf("extract error")
					},
				},
				sensorMode: "",
			},
			args: args{
				ctx: context.Background(),
				p:   &pipeline.Pipeline{},
				t:   &pipeline.Asset{Parameters: map[string]string{"query": "SELECT 1"}},
			},
			wantErr:     true,
			errContains: "failed to render query sensor query",
		},
		{
			name: "connection fetch error",
			fields: fields{
				connection: &mockConnectionRetriever{conn: nil, err: fmt.Errorf("conn error")},
				extractor:  &mockExtractor{},
				sensorMode: "",
			},
			args: args{
				ctx: context.Background(),
				p:   &pipeline.Pipeline{},
				t:   &pipeline.Asset{Parameters: map[string]string{"query": "SELECT 1"}},
			},
			wantErr:     true,
			errContains: "conn error",
		},
		{
			name: "select error",
			fields: fields{
				connection: &mockConnectionRetriever{conn: &mockConnection{selectErr: fmt.Errorf("select error")}, err: nil},
				extractor:  &mockExtractor{},
				sensorMode: "",
			},
			args: args{
				ctx: context.Background(),
				p:   &pipeline.Pipeline{},
				t:   &pipeline.Asset{Parameters: map[string]string{"query": "SELECT 1"}},
			},
			wantErr:     true,
			errContains: "select error",
		},
		{
			name: "cast result error",
			fields: fields{
				connection: &mockConnectionRetriever{conn: &mockConnection{selectResult: [][]interface{}{{"bad"}}}, err: nil},
				extractor:  &mockExtractor{},
				sensorMode: "",
			},
			args: args{
				ctx: context.Background(),
				p:   &pipeline.Pipeline{},
				t:   &pipeline.Asset{Parameters: map[string]string{"query": "SELECT 1"}},
			},
			wantErr:     true,
			errContains: "failed to parse query sensor result",
		},
		{
			name: "sensorMode skip",
			fields: fields{
				connection: &mockConnectionRetriever{conn: &mockConnection{}, err: nil},
				extractor:  &mockExtractor{},
				sensorMode: "skip",
			},
			args: args{
				ctx: context.Background(),
				p:   &pipeline.Pipeline{},
				t:   &pipeline.Asset{Parameters: map[string]string{}},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sensor := &QuerySensor{
				connection: tt.fields.connection,
				extractor:  tt.fields.extractor,
				sensorMode: tt.fields.sensorMode,
			}
			err := sensor.RunTask(tt.args.ctx, tt.args.p, tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("RunTask() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.errContains != "" && err != nil && !contains(err.Error(), tt.errContains) {
				t.Errorf("RunTask() error = %v, want error containing %v", err, tt.errContains)
			}
		})
	}
}

// contains is a helper for substring matching
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

type mockConnection struct {
	connectionFetcher
	selectResult [][]interface{}
	selectErr    error
}

type testPipeline struct {
	pipeline.Pipeline
}
