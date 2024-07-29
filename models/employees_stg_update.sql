
{{ config(
    materialized='incremental',
    unique_key= ['employeeid', 'employee_mobile'],
    depends_on=['employees_stg'],
    on_schema_change='append_new_columns',
    pre_hook=[
        "{% if target.schema == 'dbt-dimensions' and source('dbt-dimensions', 'employees_stg_update') is not none %}TRUNCATE TABLE {{ source('dbt-dimensions', 'employees_stg_update') }};{% endif %}"
    ]
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id,
    'update' AS operation,
    true AS currentflag,
    null::timestamptz AS expdate,
    stg.clientid,
    stg.employee_mobile,
    stg.employeeid,
    stg.hash_column,
    stg.employee_status,
    stg.employee_createdat_local,
    stg.employee_modifiedat_local,
    stg.employee_deletedat_local,
    stg.utc,
    stg.employee_salarytype,
    stg.tookfirstsalary,
    stg.iseligibleforclaimrequest,
    stg.iseligibleforadvancerequest,
    stg.advancerequestrequiresapproval,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate 

FROM {{ source('dbt-dimensions', 'employees_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'employees_dimension')}} final
    ON stg.employeeid = final.employeeid 
WHERE final.hash_column IS NOT NULL AND final.hash_column = stg.hash_column AND final.operation != 'exp'
    AND stg.loaddate > final.loaddate 


{% else %}

SELECT
    stg.id,
    stg.operation,
    stg.currentflag,
    stg.expdate,
    stg.clientid,
    stg.employee_mobile,
    stg.employeeid,
    stg.hash_column,
    stg.employee_status,
    stg.employee_createdat_local,
    stg.employee_modifiedat_local,
    stg.employee_deletedat_local,
    stg.utc,
    stg.employee_salarytype,
    stg.tookfirstsalary,
    stg.iseligibleforclaimrequest,
    stg.iseligibleforadvancerequest,
    stg.advancerequestrequiresapproval,
    stg.loaddate

FROM {{ ref('employees_stg') }} stg
WHERE stg.loaddate > '2050-01-01'::timestamptz

{% endif %}