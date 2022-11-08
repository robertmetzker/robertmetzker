---- SRC LAYER ----
WITH
SRC_FRZN           as ( SELECT *     FROM     DEV_VIEWS.BWCODS.THFIJDT_SCRBD ),
SRC_OB             as ( SELECT *     FROM     DEV_VIEWS.DBDWQP00.TDDOBLI ),
//SRC_FRZN           as ( SELECT *     FROM     THFIJDT_SCRBD) ,
//SRC_OB             as ( SELECT *     FROM     TDDOBLI) ,

---- LOGIC LAYER ----


LOGIC_FRZN as ( SELECT DISTINCT
          0                                                  as                                            HIST_ID 
		, TRIM( CLAIM_NO )                                   as                                           CLAIM_NO 
		, CLAIM_AGRE_ID                                      as                                      CLAIM_AGRE_ID 
		, PS_CLM_ICD_STS_ID                                  as                                  PS_CLM_ICD_STS_ID 
		, NULL                                               as                                CLM_ICD_STS_PRI_IND 
		, upper( TRIM( ICD_CODE ) )                          as                                           ICD_CODE 
		, upper( TRIM( ICDC_MPD_CODE ) )                     as                                      ICDC_MPD_CODE 
		, upper( TRIM( ICDV_CODE ) )                         as                                          ICDV_CODE 
		, substr(ICDV_CODE,position('-', ICDV_CODE, 1)+1)    as                                         ICD_VER_NO 
		, upper( TRIM( ICDV_MPD_CODE ) )                     as                                      ICDV_MPD_CODE 
		, upper( TRIM( ICD_DESC ) )                          as                                           ICD_DESC 
		, NULLIF(TRIM(BODY_LCTN_IND ), '')                   as                                    ICD_LOC_TYP_IND 
		, CASE WHEN BODY_LCTN_IND = 'L' THEN 'LEFT'
               WHEN BODY_LCTN_IND = 'R' THEN 'RIGHT'
               WHEN BODY_LCTN_IND = 'B' THEN 'BILATERAL' 
			   ELSE NULL    END                              as                                     ICD_LOC_TYP_CD 
		, CASE WHEN BODY_LCTN_IND = 'L' THEN 'LEFT'
               WHEN BODY_LCTN_IND = 'R' THEN 'RIGHT'
               WHEN BODY_LCTN_IND = 'B' THEN 'BILATERAL' 
			   ELSE NULL    END                              as                                     ICD_LOC_TYP_NM 
		, upper( TRIM( BODY_LCTN_IND ) )                     as                                      BODY_LCTN_IND 
		, upper( TRIM( PS_ICD_SITE_TYPE_CODE ) )             as                              PS_ICD_SITE_TYPE_CODE 
		, upper( TRIM( PS_ICD_SITE_TYPE_NAME ) )             as                              PS_ICD_SITE_TYPE_NAME 
		, upper( TRIM( PS_ICD_STS_TYPE_CODE ) )              as                               PS_ICD_STS_TYPE_CODE 
		, upper( TRIM( PS_ICD_STS_TYPE_NAME ) )              as                               PS_ICD_STS_TYPE_NAME 
		, INJRY_DTL_BGN_DATE                                 as                                 INJRY_DTL_BGN_DATE 
		, INJRY_DTL_END_DATE                                 as                                 INJRY_DTL_END_DATE 
		, upper( CTSTR_FLAG )                                as                                         CTSTR_FLAG 
		, upper( PSYCH_FLAG )                                as                                         PSYCH_FLAG 
		, upper( ICD_PSC_FLAG )                              as                                       ICD_PSC_FLAG 
		, upper( CRNT_STS_FLAG )                             as                                      CRNT_STS_FLAG 
		FROM SRC_FRZN
            ),

LOGIC_OB as ( SELECT DISTINCT
		  upper( TRIM( ICDC_CODE ) )                         as                                          ICDC_CODE 
		, upper( OBLI_CRNT_MAX_FLAG )                        as                                 OBLI_CRNT_MAX_FLAG 
		, upper( TRIM( OBLC_BDY_LCTN_CODE ) )                as                                 OBLC_BDY_LCTN_CODE 
		, upper( TRIM( OBLC_BDY_LCTN_TEXT ) )                as                                 OBLC_BDY_LCTN_TEXT 
		FROM SRC_OB
            )

---- RENAME LAYER ----
,

RENAME_FRZN       as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLAIM_NO                                           as                                             CLM_NO
		, CLAIM_AGRE_ID                                      as                                            AGRE_ID
		, PS_CLM_ICD_STS_ID                                  as                                     CLM_ICD_STS_ID
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, ICD_CODE                                           as                                           ICD_CODE
		, ICDC_MPD_CODE                                      as                                     ICDC_MPD9_CODE
		, ICDV_CODE                                          as                                          ICDV_CODE
		, ICD_VER_NO                                         as                                         ICD_VER_NO
		, ICDV_MPD_CODE                                      as                                ICDV_MPD9_VRSN_CODE
		, ICD_DESC                                           as                                           ICD_DESC
		, ICD_DESC                                           as                                       CLM_ICD_DESC
		, ICD_LOC_TYP_IND                                    as                                    ICD_LOC_TYP_IND
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_NM
		, PS_ICD_SITE_TYPE_CODE                              as                                    ICD_SITE_TYP_CD
		, PS_ICD_SITE_TYPE_NAME                              as                                    ICD_SITE_TYP_NM
		, PS_ICD_STS_TYPE_CODE                               as                                     ICD_STS_TYP_CD
		, PS_ICD_STS_TYPE_NAME                               as                                     ICD_STS_TYP_NM
		, INJRY_DTL_BGN_DATE                                 as                                       HIST_EFF_DTM
		, INJRY_DTL_END_DATE                                 as                                       HIST_END_DTM
		, CTSTR_FLAG                                         as                                      ICD_CTRPH_IND
		, PSYCH_FLAG                                         as                                     PSYCH_ICD_FLAG
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG
		, CRNT_STS_FLAG                                      as                                      CRNT_STS_FLAG
				FROM     LOGIC_FRZN   ), 
RENAME_OB         as ( SELECT 
		  ICDC_CODE                                          as                                       OB_ICDC_CODE
		, OBLI_CRNT_MAX_FLAG                                 as                                 OBLI_CRNT_MAX_FLAG
		, OBLC_BDY_LCTN_CODE                                 as                                 OBLC_BDY_LCTN_CODE
		, OBLC_BDY_LCTN_TEXT                                 as                                 OBLC_BDY_LCTN_TEXT 
				FROM     LOGIC_OB   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FRZN                           as ( SELECT * FROM    RENAME_FRZN 
                                            WHERE CRNT_STS_FLAG = 'N'  ),
FILTER_OB                             as ( SELECT * FROM    RENAME_OB 
                                            WHERE OBLI_CRNT_MAX_FLAG = 'Y'  ),

---- JOIN LAYER ----

FRZN as ( SELECT * 
				FROM  FILTER_FRZN
				LEFT JOIN FILTER_OB ON  FILTER_FRZN.ICD_CODE =  FILTER_OB.OB_ICDC_CODE  )
SELECT 
		  HIST_ID
		, CLM_NO
		, AGRE_ID
		, CLM_ICD_STS_ID
		, CLM_ICD_STS_PRI_IND
		, ICD_CODE
		, ICDC_MPD9_CODE
		, ICDV_CODE
		, ICD_VER_NO
		, ICDV_MPD9_VRSN_CODE
		, ICD_DESC
		, CLM_ICD_DESC
		, ICD_LOC_TYP_IND
		, ICD_LOC_TYP_CD
		, ICD_LOC_TYP_NM
		, ICD_SITE_TYP_CD
		, ICD_SITE_TYP_NM
		, ICD_STS_TYP_CD
		, ICD_STS_TYP_NM
		, HIST_EFF_DTM
		, HIST_END_DTM
		, ICD_CTRPH_IND
		, PSYCH_ICD_FLAG
		, ICD_PSC_FLAG
		, 'FRZN' AS SOURCE
		,  CASE WHEN ICD_VER_NO = 10 AND OBLC_BDY_LCTN_CODE <> ICD_LOC_TYP_IND  THEN ICD_LOC_TYP_IND
                     WHEN ICD_VER_NO = 10 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_IND 
                     WHEN ICD_VER_NO = 9 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_IND ELSE OBLC_BDY_LCTN_CODE
					 END AS OBLC_BDY_LCTN_CODE
		, CASE WHEN ICD_VER_NO = 10 AND OBLC_BDY_LCTN_CODE <> ICD_LOC_TYP_IND  THEN ICD_LOC_TYP_NM  
                     WHEN ICD_VER_NO = 10 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_NM 
                     WHEN ICD_VER_NO = 9 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_NM ELSE OBLC_BDY_LCTN_TEXT 
					 END AS OBLC_BDY_LCTN_TEXT
FROM FRZN