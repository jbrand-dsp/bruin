/* @bruin

name: dashboard.hello_sf
type: sf.sql

depends:
   - hello_python

materialization:
   type: table

columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]

custom_checks:
  - name: This is a custom check name
    value: 2
    query: select count(*) from dashboard.hello_sf

@bruin */

select 1 as one
union all
select 2 as one
