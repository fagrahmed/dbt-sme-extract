{{ config(
    materialized='incremental',
    unique_key= ['clientid'],
    depends_on=['clients_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'clients_stg_new') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'clients_stg_new') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_stg_new')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    stg.id,  
    stg.operation,
    stg.currentflag,
    stg.expdate,      
    stg.clientId,    
    stg.hash_column,
    stg.clientname_en,
    stg.clienttype,
    stg.client_createdat_local,
    stg.client_modifiedat_local,
    stg.utc,
    stg.client_status,
    stg.industrytype,
    stg.address_governorate,
    stg.address_city,
    stg.numofemployees,
    stg.salaryadvanceaccesslevel,
    stg.loaddate

    FROM {{source('dbt-dimensions', 'clients_stg')}} stg
    LEFT JOIN {{source('dbt-dimensions', 'clients_dimension')}} dim on stg.clientid = dim.clientid
    WHERE dim.clientid is null OR (dim.hash_column != stg.hash_column AND dim.currentflag = true)

{% else %}
-- dimension doesnt exists so all is new

SELECT
    stg.id,  
    stg.operation,
    stg.currentflag,
    stg.expdate,      
    stg.clientId,    
    stg.hash_column,
    stg.clientname_en,
    stg.clienttype,
    stg.client_createdat_local,
    stg.client_modifiedat_local,
    stg.utc,
    stg.client_status,
    stg.industrytype,
    stg.address_governorate,
    stg.address_city,
    stg.numofemployees,
    stg.salaryadvanceaccesslevel,
    stg.loaddate

FROM {{source('dbt-dimensions', 'clients_stg')}} stg

{% endif %}