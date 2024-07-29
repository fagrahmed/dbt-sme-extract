
{{ config(
    materialized='incremental',
    unique_key= ['employeeid', 'employee_mobile'],
    depends_on=['employees_stg'],
    on_schema_change='append_new_columns'
)}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

{% set stg_table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_stg')" %}
{% set stg_table_exists_result = run_query(stg_table_exists_query) %}
{% set stg_table_exists =stg_table_exists_result.rows[0][0] if stg_table_exists_result and stg_table_exists_result.rows else False %}

{% if table_exists %}

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

FROM {{ source('dbt-dimensions', 'employees_stg') }} stg
LEFT JOIN {{ source('dbt-dimensions', 'employees_dimension') }} dim on stg.employeeid = dim.employeeid
WHERE dim.employeeid is null OR (dim.hash_column != stg.hash_column AND dim.currentflag = true)

{% else %}
-- dimension doesnt exists so all is new

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

FROM {{ source('dbt-dimensions', 'employees_stg') }} stg

{% endif %}