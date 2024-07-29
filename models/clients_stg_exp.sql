{{ config(
    materialized='incremental',
    unique_key= ['clientid'],
    depends_on=['clients_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'clients_stg_exp') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'clients_stg_exp') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS expdate,
    final.clientId,    
    final.hash_column,
    final.clientname_en,
    final.clienttype,
    final.client_createdat_local,
    final.client_modifiedat_local,
    final.utc,
    final.client_status,
    final.industrytype,
    final.address_governorate,
    final.address_city,
    final.numofemployees,
    final.salaryadvanceaccesslevel,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate  

FROM {{ source('dbt-dimensions', 'clients_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'clients_dimension')}} final
    ON stg.clientid = final.clientid 
WHERE stg.loaddate > final.loaddate AND final.hash_column != stg.hash_column AND final.currentflag = true

{% else %}
-- do nothing (extremely high comparison date)

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

FROM {{ source('dbt-dimensions', 'clients_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz 

{% endif %}
    