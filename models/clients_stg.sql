
{{ config(
    materialized='incremental',
    unique_key= ['clientid'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'clients_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists = stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

SELECT
    md5(random()::text || '-' || COALESCE(clientid, '') || '-' || COALESCE(clientcode, '') || '-' || COALESCE(lastmodifiedat::text, '') || '-' || now()::text) AS id,  
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,      
    clientId,    
    md5(
        COALESCE(clientid, '') || '::' || COALESCE(planid, '') || '::' || COALESCE(status, '') || '::' ||
        COALESCE(clientname::text, '') || '::' || COALESCE(clienttype, '') || '::' || COALESCE(clientcode, '') || '::' ||
        COALESCE(suspended::text, '') || '::' || COALESCE(hasgroupwallet::text, '') || '::' || COALESCE(numofemployees::text, '') || '::' || 
        COALESCE(bankpaymentwalletid, '') || '::' || COALESCE(walletpaymentwalletid, '') || '::' || COALESCE(salaryadvanceaccesslevel, '')
    ) AS hash_column,

    clientname->>'en' as clientname_en,
    clienttype,
    (createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS client_createdat_local,
    (lastmodifiedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '2 hours') AS client_modifiedat_local,
    3 as utc,

    status as client_status,
    industrytype,
    address->>'governorate' as address_governorate,
    address->>'city' as address_city,
    numofemployees,
    salaryadvanceaccesslevel,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as loaddate

FROM {{source('axis_sme', 'clients') }} src

{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE (src._airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') 
            > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'clients_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
