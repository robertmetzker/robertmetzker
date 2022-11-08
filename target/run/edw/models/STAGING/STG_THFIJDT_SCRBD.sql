

      create or replace  table DEV_EDW.STAGING.STG_THFIJDT_SCRBD  as
      (---- SRC LAYER ----
WITH
SRC_THFIJDT as ( SELECT *     from     DEV_VIEWS.BWCODS.THFIJDT_SCRBD ),
//SRC_THFIJDT as ( SELECT *     from     THFIJDT_SCRBD) ,

---- LOGIC LAYER ----

LOGIC_THFIJDT as ( SELECT 
		  upper( TRIM( CLAIM_NO ) )                          AS                                           CLAIM_NO 
		, CLAIM_AGRE_ID                                      AS                                      CLAIM_AGRE_ID 
		, upper( TRIM( INJRY_NO ) )                          AS                                           INJRY_NO 
		, upper( TRIM( ADJST_INJRY_NO ) )                    AS                                     ADJST_INJRY_NO 
		, PS_CLM_ICD_STS_ID                                  AS                                  PS_CLM_ICD_STS_ID 
		, upper( TRIM( ICD_CODE ) )                          AS                                           ICD_CODE 
		, upper( TRIM( ICDV_CODE ) )                         AS                                          ICDV_CODE 
		, upper( TRIM( ICD_DESC ) )                          AS                                           ICD_DESC 
		, upper( ICD_DESC_OVRD_FLAG )                        AS                                 ICD_DESC_OVRD_FLAG 
		, case when upper( TRIM( BODY_LCTN_IND ) ) = '' then null
			else  upper( TRIM( BODY_LCTN_IND ) ) END         AS                                      BODY_LCTN_IND 
		, upper( TRIM( BODY_OBJCT_TEXT ) )                   AS                                    BODY_OBJCT_TEXT 
		, upper( TRIM( PSTN_RANGE_TEXT ) )                   AS                                    PSTN_RANGE_TEXT 
		, upper( TRIM( BODY_CNDTN_TEXT ) )                   AS                                    BODY_CNDTN_TEXT 
		, upper( TRIM( SCRBD_BODY_OBJCT_TEXT ) )             AS                              SCRBD_BODY_OBJCT_TEXT 
		, upper( TRIM( PS_ICD_SITE_TYPE_CODE ) )             AS                              PS_ICD_SITE_TYPE_CODE 
		, upper( TRIM( PS_ICD_SITE_TYPE_NAME ) )             AS                              PS_ICD_SITE_TYPE_NAME 
		, upper( TRIM( INJRY_STS ) )                         AS                                          INJRY_STS 
		, upper( TRIM( SA_STS_TEXT ) )                       AS                                        SA_STS_TEXT 
		, upper( TRIM( PS_ICD_STS_TYPE_CODE ) )              AS                               PS_ICD_STS_TYPE_CODE 
		, upper( TRIM( PS_ICD_STS_TYPE_NAME ) )              AS                               PS_ICD_STS_TYPE_NAME 
		, INJRY_DTL_BGN_DATE                                 AS                                 INJRY_DTL_BGN_DATE 
		, INJRY_DTL_END_DATE                                 AS                                 INJRY_DTL_END_DATE 
		, upper( PSYCH_FLAG )                                AS                                         PSYCH_FLAG 
		, upper( CTSTR_FLAG )                                AS                                         CTSTR_FLAG 
		, upper( ICD_PRVCY_FLAG )                            AS                                     ICD_PRVCY_FLAG 
		, upper( ICD_PSC_FLAG )                              AS                                       ICD_PSC_FLAG 
		, upper( CRNT_STS_FLAG )                             AS                                      CRNT_STS_FLAG 
		, upper( TRIM( ICDC_MPD_CODE ) )                     AS                                      ICDC_MPD_CODE 
		, upper( TRIM( ICDV_MPD_CODE ) )                     AS                                      ICDV_MPD_CODE 
		, case when upper( TRIM( OBLC_BDY_LCTN_CODE ) ) = '' then null 
			else  upper( TRIM( OBLC_BDY_LCTN_CODE ) ) END            AS                                 OBLC_BDY_LCTN_CODE 
		, upper( TRIM( ICD_OVRD_EMPLY_ID ) )                 AS                                  ICD_OVRD_EMPLY_ID 
		, cast( DW_CNTRL_DATE as DATE )                      AS                                      DW_CNTRL_DATE 
		from SRC_THFIJDT
            )

---- RENAME LAYER ----
,

RENAME_THFIJDT as ( SELECT 
		  CLAIM_NO                                           as                                           CLAIM_NO
		, CLAIM_AGRE_ID                                      as                                      CLAIM_AGRE_ID
		, INJRY_NO                                           as                                           INJRY_NO
		, ADJST_INJRY_NO                                     as                                     ADJST_INJRY_NO
		, PS_CLM_ICD_STS_ID                                  as                                  PS_CLM_ICD_STS_ID
		, ICD_CODE                                           as                                           ICD_CODE
		, ICDV_CODE                                          as                                          ICDV_CODE
		, ICD_DESC                                           as                                           ICD_DESC
		, ICD_DESC_OVRD_FLAG                                 as                                 ICD_DESC_OVRD_FLAG
		, BODY_LCTN_IND                                      as                                      BODY_LCTN_IND
		, BODY_OBJCT_TEXT                                    as                                    BODY_OBJCT_TEXT
		, PSTN_RANGE_TEXT                                    as                                    PSTN_RANGE_TEXT
		, BODY_CNDTN_TEXT                                    as                                    BODY_CNDTN_TEXT
		, SCRBD_BODY_OBJCT_TEXT                              as                              SCRBD_BODY_OBJCT_TEXT
		, PS_ICD_SITE_TYPE_CODE                              as                              PS_ICD_SITE_TYPE_CODE
		, PS_ICD_SITE_TYPE_NAME                              as                              PS_ICD_SITE_TYPE_NAME
		, INJRY_STS                                          as                                          INJRY_STS
		, SA_STS_TEXT                                        as                                        SA_STS_TEXT
		, PS_ICD_STS_TYPE_CODE                               as                               PS_ICD_STS_TYPE_CODE
		, PS_ICD_STS_TYPE_NAME                               as                               PS_ICD_STS_TYPE_NAME
		, INJRY_DTL_BGN_DATE                                 as                                 INJRY_DTL_BGN_DATE
		, INJRY_DTL_END_DATE                                 as                                 INJRY_DTL_END_DATE
		, PSYCH_FLAG                                         as                                         PSYCH_FLAG
		, CTSTR_FLAG                                         as                                         CTSTR_FLAG
		, ICD_PRVCY_FLAG                                     as                                     ICD_PRVCY_FLAG
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG
		, CRNT_STS_FLAG                                      as                                      CRNT_STS_FLAG
		, ICDC_MPD_CODE                                      as                                      ICDC_MPD_CODE
		, ICDV_MPD_CODE                                      as                                      ICDV_MPD_CODE
		, OBLC_BDY_LCTN_CODE                                 as                                 OBLC_BDY_LCTN_CODE
		, ICD_OVRD_EMPLY_ID                                  as                                  ICD_OVRD_EMPLY_ID
		, DW_CNTRL_DATE                                      as                                      DW_CNTRL_DATE 
				FROM     LOGIC_THFIJDT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_THFIJDT                        as ( SELECT * from    RENAME_THFIJDT   ),

---- JOIN LAYER ----

 JOIN_THFIJDT  as  ( SELECT * 
				FROM  FILTER_THFIJDT )
 SELECT * FROM  JOIN_THFIJDT
      );
    