
  create or replace  view DEV_EDW.STAGING.DSV_CLAIM_ICD_STATUS_DETAIL  as (
    

---- SRC LAYER ----
WITH
SRC_ICD            as ( SELECT *     FROM     STAGING.DST_CLAIM_ICD_STATUS_DETAIL ),
//SRC_ICD            as ( SELECT *     FROM     DST_CLAIM_ICD_STATUS_DETAIL) ,

---- LOGIC LAYER ----


LOGIC_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD 
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD 
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, CURRENT_ICD_STATUS_IND                             as                             CURRENT_ICD_STATUS_IND 
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM 
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM 
		FROM SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_ICD        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ICD_STS_TYP_CD                                     as                              CLAIM_ICD_STATUS_CODE
		, ICD_LOC_TYP_CD                                     as                            CLAIM_ICD_LOCATION_CODE
		, ICD_SITE_TYP_CD                                    as                                CLAIM_ICD_SITE_CODE
		, CLM_ICD_STS_PRI_IND                                as                              CLAIM_ICD_PRIMARY_IND
		, CURRENT_ICD_STATUS_IND                             as                                    CURRENT_ICD_IND
		, ICD_STS_TYP_NM                                     as                              CLAIM_ICD_STATUS_DESC
		, ICD_SITE_TYP_NM                                    as                                CLAIM_ICD_SITE_DESC 
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * FROM    RENAME_ICD   ),

---- JOIN LAYER ----

 JOIN_ICD         as  ( SELECT * 
				FROM  FILTER_ICD )
 SELECT * FROM  JOIN_ICD
  );
