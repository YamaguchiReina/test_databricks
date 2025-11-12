{{
    config(
        materialized="table",
        catalog="dev_first_usecase",
        schema="silver",
        alias="silver_dbt_account",
    )
}}

with
    bronze_mst_account as (
        select id, s4_sapcode1
        from {{ source("dev_first_usecase", "bronze_mst_account") }}
    ),
    filter as (select * from bronze_mst_account where not s4_sapcode1 is null),
    untitled_sql as (select * from filter)
select *
from untitled_sql
