---- SRC LAYER ----
WITH
SRC_CT as ( SELECT *     from     STAGING.STG_CLAIM_TYPE ),
//SRC_CT as ( SELECT *     from     STG_CLAIM_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CT as ( SELECT 
		   md5(cast(
    
    coalesce(cast(CLM_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                         UNIQUE_ID_KEY           
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( CLM_TYP_NM )                                 as                                         CLM_TYP_NM 
		from SRC_CT
            )

---- RENAME LAYER ----
,

RENAME_CT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
				FROM     LOGIC_CT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CT                             as ( SELECT * from    RENAME_CT   ),

---- JOIN LAYER ----

 JOIN_CT  as  ( SELECT * 
				FROM  FILTER_CT )
 SELECT * FROM  JOIN_CT