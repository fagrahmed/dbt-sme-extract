

{{
    config(
        materialized="incremental",
        unique_key= ["hash_column"],
        on_schema_change='append_new_columns',
        incremental_strategy = 'merge'
    )
}}

{% set table_exists_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dbt-dimensions' AND table_name = 'employees_dimension')" %}
{% set table_exists_result = run_query(table_exists_query) %}
{% set table_exists = table_exists_result.rows[0][0] if table_exists_result and table_exists_result.rows else False %}

-- Ensure dependencies are clearly defined for dbt
{% set _ = ref('employees_stg_update') %}
{% set _ = ref('employees_stg_exp') %}
{% set _ = ref('employees_stg_new') %}
{% set _ = ref('employees_stg') %}

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientid,
    employee_mobile,
    employeeid,
    hash_column,
    employee_status,
    employee_createdat_local,
    employee_modifiedat_local,
    employee_deletedat_local,
    utc,
    employee_salarytype,
    tookfirstsalary,
    iseligibleforclaimrequest,
    iseligibleforadvancerequest,
    advancerequestrequiresapproval,
    loaddate

FROM {{ ref("employees_stg_update") }}

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientid,
    employee_mobile,
    employeeid,
    hash_column,
    employee_status,
    employee_createdat_local,
    employee_modifiedat_local,
    employee_deletedat_local,
    utc,
    employee_salarytype,
    tookfirstsalary,
    iseligibleforclaimrequest,
    iseligibleforadvancerequest,
    advancerequestrequiresapproval,
    loaddate

FROM {{ ref("employees_stg_exp") }}

UNION ALL

SELECT
    id,
    operation,
    currentflag,
    expdate,
    clientid,
    employee_mobile,
    employeeid,
    hash_column,
    employee_status,
    employee_createdat_local,
    employee_modifiedat_local,
    employee_deletedat_local,
    utc,
    employee_salarytype,
    tookfirstsalary,
    iseligibleforclaimrequest,
    iseligibleforadvancerequest,
    advancerequestrequiresapproval,
    loaddate

FROM {{ ref("employees_stg_new") }}

