

with gaps as ( 
    SELECT UNIQUE_ID_KEY
    FROM  EDW_STAGING_DIM.DIM_INJURED_WORKER
    QUALIFY (CASE WHEN UNIQUE_ID_KEY = LEAD(UNIQUE_ID_KEY) OVER (ORDER BY UNIQUE_ID_KEY, RECORD_EFFECTIVE_DATE, RECORD_END_DATE)
            AND RECORD_END_DATE+1 <> LEAD(RECORD_EFFECTIVE_DATE)OVER (PARTITION BY UNIQUE_ID_KEY ORDER BY RECORD_EFFECTIVE_DATE, RECORD_END_DATE)
            THEN 'Y' ELSE 'N' END) ='Y' 
)
    select count( UNIQUE_ID_KEY ) from gaps

