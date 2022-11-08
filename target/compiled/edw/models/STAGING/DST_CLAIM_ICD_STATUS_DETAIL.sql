---- SRC LAYER ----
WITH
SRC_ICD            as ( SELECT *     FROM     STAGING.STG_CLAIM_ICD_STATUS_HISTORY ),
//SRC_ICD            as ( SELECT *     FROM     STG_CLAIM_ICD_STATUS_HISTORY) ,

---- LOGIC LAYER ----


LOGIC_ICD as ( SELECT 												 
		  TRIM( ICD_STS_TYP_CD )                             as                                     ICD_STS_TYP_CD 
		, TRIM( ICD_LOC_TYP_CD )                             as                                     ICD_LOC_TYP_CD 
		, TRIM( ICD_SITE_TYP_CD )                            as                                    ICD_SITE_TYP_CD 
		, TRIM( CLM_ICD_STS_PRI_IND )                        as                                CLM_ICD_STS_PRI_IND 
	    , case when HIST_END_DTM is null then 'Y' ELSE 'N' END as                           CURRENT_ICD_STATUS_IND 
		, TRIM( ICD_STS_TYP_NM )                             as                                     ICD_STS_TYP_NM 
		, TRIM( ICD_SITE_TYP_NM )                            as                                    ICD_SITE_TYP_NM 


		FROM SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_ICD        as ( SELECT 
		  ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD 
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD 
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
	    , CURRENT_ICD_STATUS_IND                             as                             CURRENT_ICD_STATUS_IND 
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM 
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * FROM    RENAME_ICD   ),

---- JOIN LAYER ----

 JOIN_ICD         as  ( SELECT 	DISTINCT	
                         md5(cast(
    
    coalesce(cast(ICD_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ICD_LOC_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ICD_SITE_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_ICD_STS_PRI_IND as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_ICD_STATUS_IND as 
    varchar
), '')

 as 
    varchar
))  as UNIQUE_ID_KEY, * 
				FROM  FILTER_ICD )
 SELECT * FROM  JOIN_ICD