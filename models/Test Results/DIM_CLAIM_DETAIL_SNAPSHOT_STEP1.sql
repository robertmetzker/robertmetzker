
    
    {% snapshot DIM_CLAIM_DETAIL_SNAPSHOT_STEP1 %}

    {{
        config(
        target_schema='EDW_STAGING_SNAPSHOT',
        unique_key='UNIQUE_ID_KEY',
        strategy='check',
        check_cols=[],
        )
    }}

    select UNIQUE_ID_KEY 
        from  {{ref('DSV_CLAIM_DETAIL') }}

    {% endsnapshot %}
    