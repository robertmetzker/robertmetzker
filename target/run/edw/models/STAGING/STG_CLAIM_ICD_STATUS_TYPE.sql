

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_ICD_STATUS_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_IST as ( SELECT *     from     DEV_VIEWS.PCMP.ICD_STATUS_TYPE ),
SRC_CIS as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_ICD_STATUS ),
//SRC_IST as ( SELECT *     from     ICD_STATUS_TYPE) ,
//SRC_CIS as ( SELECT *     from     CLAIM_ICD_STATUS) ,

---- LOGIC LAYER ----

LOGIC_IST as ( SELECT 
		  upper( TRIM( ICD_STS_TYP_CD ) )                    AS                                     ICD_STS_TYP_CD 
		, upper( TRIM( ICD_STS_TYP_NM ) )                    AS                                     ICD_STS_TYP_NM 
		from SRC_IST
            ),
LOGIC_CIS as ( SELECT 
		  upper( CLM_ICD_STS_PRI_IND )                       AS                                CLM_ICD_STS_PRI_IND 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    AS                                     ICD_STS_TYP_CD 
		from SRC_CIS
            )

---- RENAME LAYER ----
,

RENAME_IST as ( SELECT 
		  ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM 
				FROM     LOGIC_IST   ), 
RENAME_CIS as ( SELECT 
		  CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, ICD_STS_TYP_CD                                     as                                 CIS_ICD_STS_TYP_CD 
				FROM     LOGIC_CIS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_IST                            as ( SELECT * from    RENAME_IST   ),
FILTER_CIS                            as ( SELECT * from    RENAME_CIS   ),

---- JOIN LAYER ----

IST as ( SELECT DISTINCT * 
				FROM  FILTER_IST
				LEFT JOIN FILTER_CIS ON  FILTER_IST.ICD_STS_TYP_CD =  FILTER_CIS.CIS_ICD_STS_TYP_CD  )
SELECT * 
from IST
      );
    