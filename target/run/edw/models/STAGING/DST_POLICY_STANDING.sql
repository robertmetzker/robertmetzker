

      create or replace  table DEV_EDW.STAGING.DST_POLICY_STANDING  as
      (---- SRC LAYER ----
WITH
SRC_PSRH as ( SELECT *     from     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
//SRC_PSRH as ( SELECT *     from     STG_POLICY_STATUS_REASON_HISTORY) ,

---- LOGIC LAYER ----

LOGIC_PSRH as ( SELECT DISTINCT
		  TRIM( PLCY_TYP_CODE )                              as                                      PLCY_TYP_CODE 
		, TRIM( PLCY_TYP_NAME )                              as                                      PLCY_TYP_NAME 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_TYP_NM )                            as                                    PLCY_STS_TYP_NM 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_NM )                        as                                PLCY_STS_RSN_TYP_NM 
		, CASE WHEN  TRIM(PLCY_STS_TYP_CD) IN ('EXP', 'ACT') THEN 'Y' ELSE 'N' END as 						   ACT_IND
		from SRC_PSRH
            )

---- RENAME LAYER ----
,

RENAME_PSRH as ( SELECT 
		  PLCY_TYP_CODE                                      as                                      PLCY_TYP_CODE
		, PLCY_TYP_NAME                                      as                                      PLCY_TYP_NAME
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_TYP_NM                                    as                                    PLCY_STS_TYP_NM
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, PLCY_STS_RSN_TYP_NM                                as                                PLCY_STS_RSN_TYP_NM 
		, ACT_IND		                                     as                                      	   ACT_IND
				FROM     LOGIC_PSRH   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PSRH                           as ( SELECT * from    RENAME_PSRH   ),

---- JOIN LAYER ----

 JOIN_PSRH  as  ( SELECT * 
				FROM  FILTER_PSRH )
SELECT 
md5(cast(
    
    coalesce(cast(PLCY_TYP_CODE as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ACT_IND as 
    varchar
), '')

 as 
    varchar
))  AS  UNIQUE_ID_KEY
, PLCY_TYP_CODE        
, PLCY_TYP_NAME        
, PLCY_STS_TYP_CD      
, PLCY_STS_TYP_NM      
, PLCY_STS_RSN_TYP_CD  
, PLCY_STS_RSN_TYP_NM  
, ACT_IND		       
  FROM  JOIN_PSRH
      );
    