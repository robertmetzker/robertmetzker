---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.STG_CLAIM ),
//SRC_CLM as ( SELECT *     from     STG_CLAIM) ,

---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 


		  TRIM( CLM_LOSS_DESC )                              as                                      CLM_LOSS_DESC 
		, TRIM( CLM_CLMT_JOB_TTL )                           as                                   CLM_CLMT_JOB_TTL 
		from SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),

---- JOIN LAYER ----

ETL_FINAL AS (				
 SELECT 
--   iff(regexp_like(CLM_LOSS_DESC, '.*\\w.*'),CLM_LOSS_DESC, null ) as CLM_LOSS_DESC
-- , iff(regexp_like(CLM_CLMT_JOB_TTL, '.*\\w.*'),CLM_CLMT_JOB_TTL, null ) as  CLM_CLMT_JOB_TTL
  CLM_LOSS_DESC
, CLM_CLMT_JOB_TTL
FROM  FILTER_CLM )

-----------ETL Layer

 SELECT DISTINCT
   md5(cast(
    
    coalesce(cast(CLM_LOSS_DESC as 
    varchar
), '') || '-' || coalesce(cast(CLM_CLMT_JOB_TTL as 
    varchar
), '')

 as 
    varchar
))  as UNIQUE_ID_KEY  
, CLM_LOSS_DESC
, CLM_CLMT_JOB_TTL
from ETL_FINAL