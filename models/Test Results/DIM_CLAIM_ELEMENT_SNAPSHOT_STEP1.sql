
    
    {% snapshot DIM_CLAIM_ELEMENT_SNAPSHOT_STEP1 %}

    {{
        config(
        target_schema='EDW_STAGING_SNAPSHOT',
        unique_key='',
        strategy='check',
        check_cols=[],
        )
    }}

    select  
        from  {{ref('DSV_CLAIM_ELEMENT') }}

    {% endsnapshot %}
    