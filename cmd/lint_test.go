package cmd

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
)

func BenchmarkLint(b *testing.B) {
	isDebug := false
	app := cli.NewApp()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		if err := Lint(&isDebug).Run(cli.NewContext(app, nil, nil), "./testdata/lineage"); err != nil {
			b.Fatalf("Failed to run Lint command: %v", err)
		}
		b.StopTimer()
		elapsed := b.Elapsed()
		if elapsed > 100*time.Millisecond {
			b.Fatalf("Benchmark took too long: %v", elapsed)
		}
	}
}

func Test_unwrapAllErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "nil error",
			err:  nil,
			want: []string{},
		},
		{
			name: "single error",
			err:  errors.New("single error"),
			want: []string{"single error"},
		},
		{
			name: "nested errors",
			err:  errors.Wrap(errors.Wrapf(errors.New("inner error"), "%s error", "middle"), "outer error"),
			want: []string{"outer error", "middle error", "inner error"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := unwrapAllErrors(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
