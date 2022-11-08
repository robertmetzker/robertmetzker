---- SRC LAYER ----
WITH
SRC_CS             as ( SELECT *     FROM     STAGING.STG_CLAIM_INJURY_HISTORY_CS ),
SRC_ODS            as ( SELECT *     FROM     STAGING.STG_CLAIM_INJURY_HISTORY_FRZN ),
//SRC_CS             as ( SELECT *     FROM     STG_CLAIM_INJURY_HISTORY_CS) ,
//SRC_ODS            as ( SELECT *     FROM     STG_CLAIM_INJURY_HISTORY_FRZN) ,

---- LOGIC LAYER ----


LOGIC_CS as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, TRIM( ICD_CODE )                                   as                                           ICD_CODE 
		, TRIM( ICDC_MPD9_CODE )                             as                                     ICDC_MPD9_CODE 
		, TRIM( ICDV_CODE )                                  as                                          ICDV_CODE 
		, ICD_VER_NO                                         as                                         ICD_VER_NO 
		, TRIM( ICDV_MPD9_VRSN_CODE )                        as                                ICDV_MPD9_VRSN_CODE 
		, TRIM( ICD_DESC )                                   as                                           ICD_DESC 
		, TRIM( CLM_ICD_DESC )                               as                                       CLM_ICD_DESC 
		, TRIM( ICD_LOC_TYP_IND )                            as                                    ICD_LOC_TYP_IND 
		, TRIM( ICD_LOC_TYP_CD )                             as                                     ICD_LOC_TYP_CD 
		, TRIM( ICD_LOC_TYP_NM )                             as                                     ICD_LOC_TYP_NM 
		, TRIM( ICD_SITE_TYP_CD )                            as                                    ICD_SITE_TYP_CD 
		, TRIM( ICD_SITE_TYP_NM )                            as                                    ICD_SITE_TYP_NM 
		, TRIM( ICD_STS_TYP_CD )                             as                                     ICD_STS_TYP_CD 
		, TRIM( ICD_STS_TYP_NM )                             as                                     ICD_STS_TYP_NM 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		, ICD_CTRPH_IND                                      as                                      ICD_CTRPH_IND 
		, PSYCH_ICD_FLAG                                     as                                     PSYCH_ICD_FLAG 
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG 
		, TRIM( SOURCE )                                     as                                             SOURCE 
		, CRNT_CLAIM_INJURY_FLAG                             as                             CRNT_CLAIM_INJURY_FLAG 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, VOID_IND                                           as                                           VOID_IND 
		, TRIM( OBLC_BDY_LCTN_CODE )                         as                                 OBLC_BDY_LCTN_CODE 
		, TRIM( OBLC_BDY_LCTN_TEXT )                         as                                 OBLC_BDY_LCTN_TEXT 
		FROM SRC_CS
            ),

LOGIC_ODS as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, TRIM( ICD_CODE )                                   as                                           ICD_CODE 
		, TRIM( ICDC_MPD9_CODE )                             as                                     ICDC_MPD9_CODE 
		, TRIM( ICDV_CODE )                                  as                                          ICDV_CODE 
		, ICD_VER_NO                                         as                                         ICD_VER_NO 
		, TRIM( ICDV_MPD9_VRSN_CODE )                        as                                ICDV_MPD9_VRSN_CODE 
		, TRIM( ICD_DESC )                                   as                                           ICD_DESC 
		, TRIM( CLM_ICD_DESC )                               as                                       CLM_ICD_DESC 
		, TRIM( ICD_LOC_TYP_IND )                            as                                    ICD_LOC_TYP_IND 
		, TRIM( ICD_LOC_TYP_CD )                             as                                     ICD_LOC_TYP_CD 
		, TRIM( ICD_LOC_TYP_NM )                             as                                     ICD_LOC_TYP_NM 
		, TRIM( ICD_SITE_TYP_CD )                            as                                    ICD_SITE_TYP_CD 
		, TRIM( ICD_SITE_TYP_NM )                            as                                    ICD_SITE_TYP_NM 
		, TRIM( ICD_STS_TYP_CD )                             as                                     ICD_STS_TYP_CD 
		, TRIM( ICD_STS_TYP_NM )                             as                                     ICD_STS_TYP_NM 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		, ICD_CTRPH_IND                                      as                                      ICD_CTRPH_IND 
		, PSYCH_ICD_FLAG                                     as                                     PSYCH_ICD_FLAG 
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG 
		, TRIM( SOURCE )                                     as                                             SOURCE 
		, 'N'                                                as                             CRNT_CLAIM_INJURY_FLAG 
		, '-1'                                               as                                 AUDIT_USER_ID_CREA 
		, null                                               as                                AUDIT_USER_CREA_DTM
		, '-1'                                               as                                 AUDIT_USER_ID_UPDT 
		, null                                               as                                AUDIT_USER_UPDT_DTM
		, 'N'                                                as                                           VOID_IND
		, TRIM( OBLC_BDY_LCTN_CODE )                         as                                 OBLC_BDY_LCTN_CODE 
		, TRIM( OBLC_BDY_LCTN_TEXT )                         as                                 OBLC_BDY_LCTN_TEXT 
		FROM SRC_ODS
            )

---- RENAME LAYER ----
,

RENAME_CS         as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, ICD_CODE                                           as                                           ICD_CODE
		, ICDC_MPD9_CODE                                     as                                     ICDC_MPD9_CODE
		, ICDV_CODE                                          as                                          ICDV_CODE
		, ICD_VER_NO                                         as                                         ICD_VER_NO
		, ICDV_MPD9_VRSN_CODE                                as                                ICDV_MPD9_VRSN_CODE
		, ICD_DESC                                           as                                           ICD_DESC
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC
		, ICD_LOC_TYP_IND                                    as                                    ICD_LOC_TYP_IND
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, ICD_CTRPH_IND                                      as                                      ICD_CTRPH_IND
		, PSYCH_ICD_FLAG                                     as                                     PSYCH_ICD_FLAG
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG
		, SOURCE                                             as                                             SOURCE
		, CRNT_CLAIM_INJURY_FLAG                             as                             CRNT_CLAIM_INJURY_FLAG
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, OBLC_BDY_LCTN_CODE                                 as                                 OBLC_BDY_LCTN_CODE
		, OBLC_BDY_LCTN_TEXT                                 as                                 OBLC_BDY_LCTN_TEXT 
				FROM     LOGIC_CS   ), 
RENAME_ODS        as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, ICD_CODE                                           as                                           ICD_CODE
		, ICDC_MPD9_CODE                                     as                                     ICDC_MPD9_CODE
		, ICDV_CODE                                          as                                          ICDV_CODE
		, ICD_VER_NO                                         as                                         ICD_VER_NO
		, ICDV_MPD9_VRSN_CODE                                as                                ICDV_MPD9_VRSN_CODE
		, ICD_DESC                                           as                                           ICD_DESC
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC
		, ICD_LOC_TYP_IND                                    as                                    ICD_LOC_TYP_IND
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, ICD_CTRPH_IND                                      as                                      ICD_CTRPH_IND
		, PSYCH_ICD_FLAG                                     as                                     PSYCH_ICD_FLAG
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG
		, SOURCE                                             as                                             SOURCE
		, CRNT_CLAIM_INJURY_FLAG                             as                             CRNT_CLAIM_INJURY_FLAG
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, OBLC_BDY_LCTN_CODE                                 as                                 OBLC_BDY_LCTN_CODE
		, OBLC_BDY_LCTN_TEXT                                 as                                 OBLC_BDY_LCTN_TEXT 
				FROM     LOGIC_ODS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * FROM    RENAME_CS   ),
FILTER_ODS                            as ( SELECT * FROM    RENAME_ODS   ),

---- UNION LAYER ----

 UNION_ODS         as  ( SELECT * FROM  FILTER_CS
                        UNION ALL
						SELECT * FROM  FILTER_ODS  )


SELECT md5(cast(
    
    coalesce(cast(CLM_NO as 
    varchar
), '') || '-' || coalesce(cast(CLM_ICD_STS_ID as 
    varchar
), '') || '-' || coalesce(cast(ICD_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLM_ICD_DESC as 
    varchar
), '') || '-' || coalesce(cast(ICD_LOC_TYP_IND as 
    varchar
), '') || '-' || coalesce(cast(HIST_EFF_DTM as 
    varchar
), '') || '-' || coalesce(cast(HIST_END_DTM as 
    varchar
), '') || '-' || coalesce(cast(ICD_SITE_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ICD_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ICD_CTRPH_IND as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, * FROM UNION_ODS