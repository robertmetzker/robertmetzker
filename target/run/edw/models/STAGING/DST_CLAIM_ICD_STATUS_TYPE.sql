

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_ICD_STATUS_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_ICD as ( SELECT *     from     STAGING.STG_CLAIM_ICD_STATUS_HISTORY ),
//SRC_ICD as ( SELECT *     from     STG_CLAIM_ICD_STATUS_HISTORY) ,

---- LOGIC LAYER ----


LOGIC_ICD as ( SELECT 
		   md5(cast(
    
    coalesce(cast(ICD_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_ICD_STS_PRI_IND as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                      UNIQUE_ID_KEY                 
		, TRIM( ICD_STS_TYP_CD )                             as                                     ICD_STS_TYP_CD 
		, TRIM( CLM_ICD_STS_PRI_IND )                        as                                CLM_ICD_STS_PRI_IND 
		, TRIM( ICD_STS_TYP_NM )                             as                                     ICD_STS_TYP_NM 
		from SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM 
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),

---- JOIN LAYER ----

 JOIN_ICD  as  ( SELECT * 
				FROM  FILTER_ICD )
 SELECT DISTINCT * FROM  JOIN_ICD
      );
    