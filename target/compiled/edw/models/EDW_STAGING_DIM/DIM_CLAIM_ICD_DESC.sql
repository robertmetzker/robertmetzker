

 WITH  SCD AS ( 
	SELECT   UNIQUE_ID_KEY,
     last_value(CLAIM_ICD_DESC) over 
        (partition by UNIQUE_ID_KEY 
        order by UNIQUE_ID_KEY 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as CLAIM_ICD_DESC
	FROM EDW_STAGING.DIM_CLAIM_ICD_DESC_SCDALL_STEP2),
ETL AS (
Select  
  md5(cast(
    
    coalesce(cast(CLAIM_ICD_DESC as 
    varchar
), '')

 as 
    varchar
)) As  CLAIM_ICD_DESC_HKEY
, md5(cast(
    
    coalesce(cast(CLAIM_ICD_DESC as 
    varchar
), '')

 as 
    varchar
)) As  UNIQUE_ID_KEY
, CLAIM_ICD_DESC
, CURRENT_TIMESTAMP AS  LOAD_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
from SCD
QUALIFY RANK() OVER (PARTITION BY CLAIM_ICD_DESC ORDER BY UNIQUE_ID_KEY DESC) = 1
)    

select * from ETL