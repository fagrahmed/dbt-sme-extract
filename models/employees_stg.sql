
{{ config(
    materialized='incremental',
    unique_key= ['employeeid', 'employee_mobile'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists =stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}


SELECT
    md5(random()::text || '-' || COALESCE(ce.clientid, '') || '-' || COALESCE(ce.clientemployeeid, '') || '-' || COALESCE(ce.lastmodifiedat::text, '') || '-' || now()::text) AS id,
    'insert' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,    
    c.clientid,
    ce.mobilenumber as employee_mobile,
    ce.clientemployeeid as employeeid,
    md5(
        COALESCE(ce.clientemployeeid, '') || '::' || COALESCE(ce.mobilenumber, '') || '::' || COALESCE(ce.status, '') || '::' ||
        COALESCE(ce.address, '') || '::' || COALESCE(ce.clientid, '') || '::' || COALESCE(ce.fullname, '') || '::' ||
        COALESCE(ce.nationalid, '') || '::' || COALESCE(ce.salarytype, '') || '::' || COALESCE(ce.salary_aibyte_transform, '') || '::' || 
        COALESCE(ce.iseligibleforclaimrequest::text, '') || '::' || COALESCE(ce.iseligibleforadvancerequest::text, '') || '::' || COALESCE(ce.advancerequestrequiresapproval::text, '')
    ) AS hash_column,
    ce.status as employee_status,
    (ce.createdat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as employee_createdat_local,
    (ce.lastmodifiedat::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as employee_modifiedat_local,
    (ce.deletedtime::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') as employee_deletedat_local,
    3 as utc,
    ce.salarytype as employee_salarytype,
    ce.tookfirstsalary,
    ce.iseligibleforclaimrequest,
    ce.iseligibleforadvancerequest,
    ce.advancerequestrequiresapproval,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate


FROM {{ source('axis_sme', 'clientemployees') }} ce
LEFT JOIN {{ source('axis_sme', 'clients') }} c ON ce.clientid = c.clientid

{% if is_incremental() and table_exists and stg_table_exists %}
    WHERE (ce._airbyte_emitted_at::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours')
             > COALESCE((SELECT max(loaddate::timestamptz) FROM {{ source('dbt-dimensions', 'employees_dimension') }}), '1900-01-01'::timestamp)
{% endif %}
