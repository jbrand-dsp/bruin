package jinja

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJinjaRenderer_RenderQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		args    Context
		want    string
		wantErr bool
	}{
		{
			name:  "simple render for ds",
			query: "set analysis_end_date = '{{ ds }}'::date; select * from {{ ref('abc') }}",
			args: Context{
				"ds": "2022-02-03",
				"ref": func(str string) string {
					return "some-ref-here"
				},
			},
			want: "set analysis_end_date = '2022-02-03'::date; select * from some-ref-here",
		},
		{
			name:  "add_days",
			query: "{{ start_date | add_days(3) | add_days(1) | add_days(-5) | date_format('%Y/%m/%d') }}",
			args: Context{
				"start_date": "2022-02-03",
			},
			want: "2022/02/02",
		},
		{
			name:  "multiple variables",
			query: "set analysis_end_date = '{{ ds }}'::date and '{{testVar}}' == 'testvar' and another date {{    ds }} - {{ someMissingVariable }};",
			args: Context{
				"ds":      "2022-02-03",
				"testVar": "testvar",
			},
			want: "set analysis_end_date = '2022-02-03'::date and 'testvar' == 'testvar' and another date 2022-02-03 - ;",
		},
		{
			name: "jinja variables work as well",
			query: `
{% set payment_method = "bank_transfer" %}

select
    order_id,
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: Context{},
			want: `


select
    order_id,
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "array variables work",
			query: `
{% set payment_methods = ["bank_transfer", "credit_card", "gift_card"] %}

select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {% endfor %}
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: Context{},
			want: `


select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "given array from outside is rendered",
			query: `
select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {% endfor %}
    sum(amount) as total_amount
from app_data.payments
group by 1`,
			args: Context{
				"payment_methods": []string{"bank_transfer", "credit_card", "gift_card"},
			},
			want: `
select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    
    sum(amount) as total_amount
from app_data.payments
group by 1`,
		},
		{
			name: "for loop set",
			query: `
{%- for num in range(8, 11) %}
{{ ("0" ~ num|string)[-2:] -}}
{% endfor %}`,
			args: Context{},
			want: `
08
09
10`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			receiver := NewRenderer(tt.args)
			got, err := receiver.Render(tt.query)
			require.NoError(t, err)

			require.Equal(t, tt.want, got)
		})
	}
}

func TestJinjaRendererWithStartEndDate(t *testing.T) {
	t.Parallel()

	startDate, err := time.Parse("2006-01-02 15:04:05", "2022-02-03 04:00:00")
	require.NoError(t, err)

	endDate := time.Date(2022, 2, 4, 4, 0, 0, 948740170, time.UTC)

	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{
			name:  "simple render for ds",
			query: "{{ end_date }}, {{ end_datetime | add_days(3) }}, {{ end_timestamp | add_days(3) }}, {{ end_timestamp | add_days(2) | date_format('%Y-%m-%d') }}",
			want:  "2022-02-04, 2022-02-07T04:00:00, 2022-02-07T04:00:00.948740Z, 2022-02-06",
		},
		{
			name:  "things that are not in the template should be remove",
			query: "set analysis_end_date = '{{ whatever }}'::date;",
			want:  "set analysis_end_date = ''::date;",
		},
		{
			name: "array variables work",
			query: `
{%- set payment_methods = ["bank_transfer", "credit_card", "gift_card"] -%}

select
    order_id,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}' then amount end) as {{payment_method}}_amount,
    {%- endfor %}
    sum(amount) as total_amount
from app_data.payments
	where created_at >= '{{ start_datetime }}' and created_at < '{{ end_datetime }}'
group by 1`,
			want: `select
    order_id,
    
    sum(case when payment_method = 'bank_transfer' then amount end) as bank_transfer_amount,
    sum(case when payment_method = 'credit_card' then amount end) as credit_card_amount,
    sum(case when payment_method = 'gift_card' then amount end) as gift_card_amount,
    sum(amount) as total_amount
from app_data.payments
	where created_at >= '2022-02-03T04:00:00' and created_at < '2022-02-04T04:00:00'
group by 1`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			receiver := NewRendererWithStartEndDates(&startDate, &endDate)
			got, err := receiver.Render(tt.query)
			require.NoError(t, err)

			require.Equal(t, tt.want, got)
		})
	}
}
