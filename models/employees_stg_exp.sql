
{{ config(
    materialized='incremental',
    unique_key= ['employeeid', 'employee_mobile'],
    depends_on=['employees_stg'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% if table_exists %}

SELECT
    final.id,
    'exp' AS operation,
    false AS currentflag,
    (now()::timestamp AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS expdate,
    final.clientid,
    final.employee_mobile,
    final.employeeid,
    final.hash_column,
    final.employee_status,
    final.employee_createdat_local,
    final.employee_modifiedat_local,
    final.employee_deletedat_local,
    final.utc,
    final.employee_salarytype,
    final.tookfirstsalary,
    final.iseligibleforclaimrequest,
    final.iseligibleforadvancerequest,
    final.advancerequestrequiresapproval,
    (now()::timestamptz AT TIME ZONE 'UTC' + INTERVAL '3 hours') AS loaddate

FROM {{ source('dbt-dimensions', 'employees_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'employees_dimension')}} final
    ON stg.employeeid = final.employeeid 
WHERE stg.loaddate > final.loaddate AND final.hash_column != stg.hash_column AND final.currentflag = true

{% else %}

-- do nothing (extremely high comparison date)
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