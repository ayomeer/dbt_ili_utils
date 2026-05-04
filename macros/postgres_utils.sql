
-- conflict target as comma seperated list of columns as string
{% macro upsert_into(schema_name, table_name, conflict_target) %}

  {% set cols = dbt_utils.get_filtered_columns_in_relation(this) %}
  {% set cols_str = cols | join(',\n  ') %}

  {% set insert_query %}
    INSERT INTO {{schema_name}}.{{table_name}}(
      -- Get list of column names present in boundary model
      -- assumption: boundary model column names match target column names 
      {{ cols_str }}
    ) 
    SELECT
      *
    FROM {{this}}
    ON CONFLICT ({{ conflict_target }}) DO UPDATE
    SET 
      {% for col in cols -%}
      {{ col }} = EXCLUDED.{{ col }}{% if not loop.last %},{% endif %}
      {% endfor %}
  {% endset %}
  {% set query_return = run_query(insert_query)%}

{%- endmacro %}

