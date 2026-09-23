
-- conflict target as comma seperated list of columns as string
{%- macro upsert_into(
  schema_name, 
  table_name, 
  conflict_target=[],
  conflict_except_values={},
  update_except_cols=[]
) -%}

  {% set insert_cols = dbt_utils.get_filtered_columns_in_relation(this) %}

  {% set update_cols = dbt_utils.get_filtered_columns_in_relation(
    this,
    except=update_except_cols
  ) %}

  INSERT INTO {{schema_name}}.{{table_name}} AS target(
    -- Get list of column names present in boundary model
    -- assumption: boundary model column names match target column names 
    {{ insert_cols | join(',\n  ') }}
  ) 
  SELECT
    *
  FROM {{this}}
  ON CONFLICT ({{ conflict_target | join(', ') }}) 
  {% if conflict_except_values %}
    WHERE
    {% for col, value in conflict_except_values.items() %}
      {{ col }} <> {{ dbt.string_literal(value) }}
      {% if not loop.last %}AND{% endif %}
    {% endfor %}
  {% endif %}
  DO UPDATE SET 
    {% for col in update_cols -%}
    {{ col }} = COALESCE(EXCLUDED.{{ col }}, target.{{ col }}){% if not loop.last %},{% endif %}
    {% endfor %}

{%- endmacro -%}

