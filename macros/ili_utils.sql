--- General Utility -----------------------------------------------------------

-- Create t_ili2db_sequence
-- intended use: first time dbt schema setup
--  call through run-operations e.g.:
--  dbt run-operation create_ili_sequence --args '{schema: dbt_quellkataster}'
{% macro create_ili_sequence(schema_name) -%}
  {% if execute %}
    -- create sequence
    {% set query %}
      CREATE SEQUENCE IF NOT EXISTS {{schema_name}}.t_ili2db_seq
      INCREMENT 1
      START 1
      MINVALUE 1
      MAXVALUE 9223372036854775807
      CACHE 1;
    {% endset %}
    {% set query_return = run_query(query) %}
  {% endif %}
{%- endmacro %}

-- Reset 't_ili2db_seq' in target schema
{% macro reset_ili_sequence(schema_name, starting_value=1) -%}
  
  {% if execute %}
    {{ log("Restarting " ~ schema_name ~ ".t_ili2db_seq with " ~ starting_value, info=True) }}

    {% set sql_query %}
      ALTER SEQUENCE {{schema_name}}.t_ili2db_seq RESTART WITH {{starting_value}};
    {% endset%}
    {% do run_query(sql_query) %}
  {% endif %}
{%- endmacro %}


-- Set up roles, such that there are no access issues for the dbt user 
-- (target.user). Run this before restoring backups, so the roles can be granted.
{% macro setup_roles_for_schema(schema_name) -%}
  {{ log("Creating Read/Write roles for schema " ~ schema_name, info=True) }}
  {{ log("Creating role " ~ schema_name ~ "_read", info=True) }}
  {{ log("Creating role " ~ schema_name ~ "_write", info=True) }}

  {% if execute %}
    {% set sql_query %}
      CREATE ROLE {{schema_name}}_read;
      CREATE ROLE {{schema_name}}_write;
    {% endset %}
    {% do run_query(sql_query) %}

    {{ log("Assigning created roles to " ~ target.user, info=True) }}
    {% set sql_query %}
      GRANT {{schema_name}}_read to {{target.user}};
      GRANT {{schema_name}}_write to {{target.user}};
    {% endset %}
    {% do run_query(sql_query) %}
  {% endif %}
{%- endmacro %}

{% macro grant_select_on_all_tables(schema_name) -%}
  {{ log(
    "Granting USAGE on schema " ~ schema_name 
    ~ " to " ~ target.user, 
    info=True
  )}}
  {{ log(
    "Granting ALL on all tables in schema " ~ schema_name 
    ~ " to " ~ target.user, 
    info=True
  )}}

  {% if execute %}
    {% set sql_query %}
      GRANT USAGE ON SCHEMA {{schema_name}} TO {{target.user}};
      GRANT ALL ON ALL TABLES IN SCHEMA {{schema_name}} TO {{target.user}};
    {% endset %}
    {% do run_query(sql_query) %}
  {% endif %}
{%- endmacro %}


--- Baskets and Datasets ------------------------------------------------------
-- Populate basket table based on project configuration:
-- Add a row (i.e.) a basket for each basket defined in dbt_project.yml
{% macro setup_baskets(schema_name) %}
  {% if execute %}
    {% for key, value_dict in var('baskets').items() %}
      {{ log(
          "Writing " ~ key ~ " into " ~ schema_name ~ ".t_ili2db_basket",
          info=True
      )}}
      {% set sql_basket_row %}
        INSERT INTO {{schema_name}}.t_ili2db_basket(
          t_id,         -- NOT NULL
          dataset,
          topic,        -- NOT NULL
          t_ili_tid,
          attachmentkey,-- NOT NULL (but also not used -> write '-')
          domains
        )
        VALUES (
          {{ value_dict['t_id'] }},
          {{ value_dict['dataset_t_id'] }},
          '{{ value_dict['topic'] }}',
          uuid_generate_v4(),
          '-',
          NULL
        );
        -- advance t_ili2db_seq to make up for manually set t_id
        SELECT nextval('{{schema_name}}.t_ili2db_seq'::regclass);
      {% endset %}
      {% set query_return = run_query(sql_basket_row)%}
    {% endfor %}
  {% endif %}
{%- endmacro %}

{% macro setup_datasets(schema_name) %}
  -- Populate dataset table based on project configuration:
  -- Add a row (i.e.) a dataset for each dataset defined in dbt_project.yml
  {% if execute %}
    {% for key, value_dict in var('datasets').items() %}
      {{ log(
          "Writing " ~ key ~ " into " ~ schema_name ~ ".t_ili2db_dataset",
          info=True
      )}}
      {% set sql_dataset_row %}
        INSERT INTO {{schema_name}}.t_ili2db_dataset(
          t_id,
          datasetname
        )
        VALUES (
          {{ value_dict['t_id'] }},
          '{{ value_dict['datasetname'] }}'
        );
        -- advance t_ili2db_seq to make up for manually set t_id
        SELECT nextval('{{schema_name}}.t_ili2db_seq'::regclass);
      {% endset %}
      {% set query_return = run_query(sql_dataset_row)%}
    {% endfor %}
  {% endif %}
{%- endmacro %}

{% macro reset_target_schema(schema_name) %}
  {% if execute %}
    {{ ili_utils.reset_ili_sequence(schema_name) }}

    -- Clear truncate tables
    {% set sql_truncate %}
      TRUNCATE TABLE {{schema_name}}.t_ili2db_dataset;
      TRUNCATE TABLE {{schema_name}}.t_ili2db_basket;
    {% endset %}
    {% do run_query(sql_truncate) %}

    -- Populate dataset table
    {{ ili_utils.setup_datasets(schema_name) }}

    -- Populate basket table
    {{ ili_utils.setup_baskets(schema_name) }}
  {% endif %}

  SELECT 1 -- temporary fix for "cannot execute empty sql query"
{%- endmacro %}


--- Transferring across boundary: dbt -> INTERLIS schema ----------------------

-- Export dbt table to target table
{% macro insert_into(schema_name, table_name, truncate_target=false) %}
  {{ log(
      "Inserting into " ~ schema_name ~ "." ~ table_name ~ 
      " with truncate_target=" ~ truncate_target, 
      info=true
    )}}
  {% if execute %}
    {% if truncate_target %}
      {{ log(
        "Truncating " ~ schema_name ~ "." ~ table_name,
        info=true
      )}}
      {% set truncate_query %}
        TRUNCATE TABLE {{schema_name}}.{{table_name}};
      {% endset%}
      {% set query_return = run_query(truncate_query)%}
    {% endif %}

    {% set insert_query %}
      INSERT INTO {{schema_name}}.{{table_name}}(
        -- Get list of column names present in boundary model
        -- assumption: boundary model column names match target column names 
        {{ dbt_utils.get_filtered_columns_in_relation(this) | join(',\n  ') }}
      )
      SELECT
        *
      FROM {{this}}
    {% endset %}
    {% set query_return = run_query(insert_query)%}
  {% endif %}

{%- endmacro %}


--- Parsing on dbt Triggers ---------------------------------------------------
{% macro run_start_parsing(target_ili_schema) %}
  SELECT 1;

  {% if execute %}
    {{ log(
        "Running ili_utils.run_start_parsing() macro \n"
        ~ "enable_transfer: " ~ var('enable_transfer', false),
        info=True
    )}}

    -- reset dbt schema's t_ili2db_seq
    {{ ili_utils.reset_ili_sequence(target.schema) }}

    {% if var('enable_transfer', false) %}
      {{ ili_utils.reset_target_schema(target_ili_schema) }}
    {% endif %}
  {% endif %} -- endif execute
{%- endmacro %}
