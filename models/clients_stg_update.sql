{{ config(
    materialized='incremental',
    unique_key= ['clientid'],
    depends_on=['clients_stg'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}
--update old records (in dim)

    SELECT
        final.id,
        'update' AS operation,
        true AS currentflag,
        null::timestamptz AS expdate,
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
        (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate  

    FROM {{ source('dbt-dimensions', 'clients_stg') }} stg
    LEFT JOIN {{ source('dbt-dimensions', 'clients_dimension')}} final
        ON stg.clientid = final.clientid 
    WHERE final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column AND final.operation != 'exp'
        AND stg.loaddate > final.loaddate


{% else %}

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

