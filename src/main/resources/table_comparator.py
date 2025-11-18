# -*- coding: utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from hybrid_toolkit.operator.hybridoperator import HybridOperator

# --- Global variables (from Airflow Variables) ---
initiative_alias = Variable.get("AMLTOOLS_initiative_alias")
job_name = Variable.get("AMLTOOLS_job_name")
preferences = Variable.get("AMLTOOLS_preferences", default_var="[]", deserialize_json=True)
size = Variable.get("AMLTOOLS_size", default_var="S")
check_retry_time = int(Variable.get("AMLTOOL_check_retry_time", default_var=300))
sleep_secs = int(Variable.get("AMLTOOL_sleep_secs", default_var=90))

default_args = {
    "owner": initiative_alias,
    "start_date": datetime(2018, 4, 1),
}

with DAG(
    dag_id="AMLTOOLS_TABLE_COMPARATOR",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["aml-tools"],
    render_template_as_native_obj=True,  # native: lists/dicts from Jinja
) as dag:

    start = EmptyOperator(task_id="start")

    # --- Jinja template that returns a real LIST of "key=value" ---
    kv_params = r"""
{% set exec_date_iso = (dag_run.conf.get('args', [ds, 'default'])[0] if dag_run.conf.get('args') else ds) %}
{% set suffix       = (dag_run.conf.get('args', [ds, 'default'])[1] if dag_run.conf.get('args') else 'default') %}

{# Base config from Airflow Variable: AMLTOOLS_tables_comparator_<SUFFIX> #}
{% set cfg_key = 'AMLTOOLS_tables_comparator_' ~ suffix %}
{% set base_cfg = var.json.get(cfg_key, {}) %}

{# Overrides from dag_run.conf (everything except 'args') #}
{% set cfg = base_cfg.copy() %}
{% for k, v in (dag_run.conf or {}).items() %}
  {% if k != 'args' and v is not none %}
    {% set _ = cfg.update({k: v}) %}
  {% endif %}
{% endfor %}

{# Prepare derived date formats #}
{% set exec_date_dmy   = macros.ds_format(exec_date_iso, '%Y-%m-%d', '%d/%m/%Y') %}
{% set exec_date_basic = macros.ds_format(exec_date_iso, '%Y-%m-%d', '%Y%m%d') %}

{# Local token replacement function in strings #}
{% macro repl_tokens(s) -%}
  {{- (s | string)
      | replace('YYYY-MM-dd', exec_date_iso)
      | replace('yyyy-MM-dd', exec_date_iso)
      | replace('$EXEC_DATE', exec_date_iso)
      | replace('${EXEC_DATE}', exec_date_iso)
      | replace('{{ds}}', exec_date_iso)
      | replace('dd/MM/YYYY', exec_date_dmy)
      | replace('dd/MM/yyyy', exec_date_dmy)
      | replace('YYYYMMdd', exec_date_basic)
      | replace('yyyyMMdd', exec_date_basic)
  -}}
{%- endmacro %}

{# Normalize partitionSpec (accepts string or dict) #}
{% set raw_spec = cfg.get('partitionSpec') %}
{% if raw_spec is mapping %}
  {% set parts = [] %}
  {% for k, v in raw_spec.items() %}
    {# remove surrounding quotes if any and apply tokens #}
    {% set vv = repl_tokens(v) | replace('"','') | replace("'", '') %}
    {% set _ = parts.append(k ~ '=' ~ vv) %}
  {% endfor %}
  {% set partitionSpec = parts | join('/') %}
{% elif raw_spec %}
  {% set partitionSpec = repl_tokens(raw_spec) | replace('"','') | replace("'", '') %}
{% else %}
  {% set partitionSpec = '' %}
{% endif %}

{# Per-side overrides (strings) with token replacement #}
{% set refPartitionSpec = cfg.get('refPartitionSpec') %}
{% if refPartitionSpec %}
  {% set refPartitionSpec = repl_tokens(refPartitionSpec) | replace('"','') | replace("'", '') %}
{% endif %}
{% set newPartitionSpec = cfg.get('newPartitionSpec') %}
{% if newPartitionSpec %}
  {% set newPartitionSpec = repl_tokens(newPartitionSpec) | replace('"','') | replace("'", '') %}
{% endif %}

{# Window strings (e.g., "-2..+3"). We pass as-is; el JAR los procesa. #}
{% set refWindowDays = cfg.get('refWindowDays') %}
{% set newWindowDays = cfg.get('newWindowDays') %}

{# Helpers lists -> CSV #}
{% set ck = cfg.get('compositeKeyCols', []) %}
{% set ig = cfg.get('ignoreCols', []) %}

{# Build final LIST #}
{# Build final LIST base #}
{% set params = [
  "refTable=" ~ cfg.get('refTable'),
  "newTable=" ~ cfg.get('newTable'),
  "initiativeName=" ~ cfg.get('initiativeName'),
  "tablePrefix=" ~ cfg.get('tablePrefix'),
  "outputBucket=" ~ cfg.get('outputBucket'),
  "executionDate=" ~ exec_date_iso,
  "partitionSpec=" ~ partitionSpec,
  "compositeKeyCols=" ~ (ck | join(',')),
  "ignoreCols=" ~ (ig | join(',')),
  "checkDuplicates=" ~ (cfg.get('checkDuplicates', false) | string | lower),
  "includeEqualsInDiff=" ~ (cfg.get('includeEqualsInDiff', false) | string | lower)
] %}

{# Optional per-side overrides: precedence handled in JAR; we solo los añadimos si existen #}
{% if refPartitionSpec %}
  {% set _ = params.append("refPartitionSpec=" ~ refPartitionSpec) %}
{% endif %}
{% if newPartitionSpec %}
  {% set _ = params.append("newPartitionSpec=" ~ newPartitionSpec) %}
{% endif %}
{% if refWindowDays %}
  {% set _ = params.append("refWindowDays=" ~ refWindowDays) %}
{% endif %}
{% if newWindowDays %}
  {% set _ = params.append("newWindowDays=" ~ newWindowDays) %}
{% endif %}

{{ params }}
"""

    # Pass the rendered list directly as "parameters"
    run_table_comparator = HybridOperator(
        task_id="run_table_comparator",
        body_balancer={
            "preferences": preferences,
            "size": size,
            "parameters": kv_params,   # <- with render_template_as_native_obj it comes out as a real LIST
        },
        initiative_alias=initiative_alias,
        process_name=job_name,
        check_retry_time=check_retry_time,
        sleep_secs=sleep_secs
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> run_table_comparator >> end

