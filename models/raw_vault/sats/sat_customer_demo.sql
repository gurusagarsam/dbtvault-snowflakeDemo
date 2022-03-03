{{
    config(
        materialized='incremental', transient =False, temporary=False,
        unique_key='CUSTOMER_HASHDIFF'
    )
}}

{%- set source_model = "v_stg_customer_demo" -%}
{%- set src_pk = "CUSTOMER_PK" -%}
{%- set src_hashdiff = "CUSTOMER_HASHDIFF" -%}
{%- set src_payload = ["CUSTOMERKEY","CUSTOMER_NAME","CUSTOMER_ADDRESS","CUSTOMER_PHONE","EFFECTIVE_TO"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
