

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_TYPE_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_CTH as ( SELECT *     from     STAGING.STG_CLAIM_TYPE_HISTORY ),
//SRC_CTH as ( SELECT *     from     STG_CLAIM_TYPE_HISTORY) ,

---- LOGIC LAYER ----


LOGIC_CTH as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( CLM_TYP_NM )                                 as                                         CLM_TYP_NM 
		, cast( HIST_EFF_DTM as DATE )                       as                                       HIST_EFF_DTM 
		, cast( HIST_END_DTM as DATE )                       as                                       HIST_END_DTM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, TRIM(CHNG_OVR_IND)                                 as                                       CHNG_OVR_IND 
		from SRC_CTH
		       QUALIFY RANK() OVER (PARTITION BY CLM_NO,HIST_EFF_DTM ORDER BY HIST_EFF_DTM DESC) = 1
            )

---- RENAME LAYER ----
,

RENAME_CTH as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, HIST_EFF_DTM                                       as                                        HIST_EFF_DT
		, HIST_END_DTM                                       as                                        HIST_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, TRIM(CHNG_OVR_IND)                                 as                                       CHNG_OVR_IND
				FROM     LOGIC_CTH   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CTH                            as ( SELECT * from    RENAME_CTH 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),

---- JOIN LAYER ----

 JOIN_CTH  as  ( SELECT * 
				FROM  FILTER_CTH ),
 -------ETL LAYER---------

ETL AS ( SELECT 
 CLM_NO,
 CLM_TYP_CD,
 CLM_TYP_NM,
 HIST_EFF_DT,
 dateadd(day, -1, HIST_END_DT) AS  HIST_END_DT,
 AUDIT_USER_ID_CREA,
 AUDIT_USER_CREA_DTM,
 AUDIT_USER_ID_UPDT,
 AUDIT_USER_UPDT_DTM,
 CLM_REL_SNPSHT_IND,
 CHNG_OVR_IND
  FROM JOIN_CTH)

 SELECT * FROM ETL
      );
    