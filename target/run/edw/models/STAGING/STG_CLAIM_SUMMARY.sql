

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_SUMMARY  as
      (---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_SUMMARY ),
SRC_NIT as ( SELECT *     from     DEV_VIEWS.PCMP.NCCI_INJURY_TYPE ),
//SRC_CS as ( SELECT *     from     CLAIM_SUMMARY) ,
//SRC_NIT as ( SELECT *     from     NCCI_INJURY_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CS as ( SELECT 
		  CS_ID                                              as                                              CS_ID 
		, upper( CS_TRAN_TYP_ID )                            as                                     CS_TRAN_TYP_ID 
		, upper( TRIM( CS_CLM_NO ) )                         as                                          CS_CLM_NO 
		, upper( TRIM( CS_PLCY_NO ) )                        as                                         CS_PLCY_NO 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, cast( CS_PLCY_EFF_DT as DATE )                     as                                     CS_PLCY_EFF_DT 
		, upper( TRIM( CS_RISK_LOC_NO ) )                    as                                     CS_RISK_LOC_NO 
		, CS_OCCR_DT                                         as                                         CS_OCCR_DT 
		, CS_OCCR_RPT_DT                                     as                                     CS_OCCR_RPT_DT 
		, upper( TRIM( CS_NCCI_STS_CD ) )                    as                                     CS_NCCI_STS_CD 
		, upper( TRIM( CS_CLM_STT_TYP_NM ) )                 as                                  CS_CLM_STT_TYP_NM 
		, upper( TRIM( CS_CLM_STS_TYP_NM ) )                 as                                  CS_CLM_STS_TYP_NM 
		, upper( TRIM( CS_CLM_TYP_NM ) )                     as                                      CS_CLM_TYP_NM 
		, upper( TRIM( CS_NCCI_INJR_TYP_CD ) )               as                                CS_NCCI_INJR_TYP_CD 
		, CUST_ID_CLMT                                       as                                       CUST_ID_CLMT 
		, upper( TRIM( CS_CLMT_NM_FST ) )                    as                                     CS_CLMT_NM_FST 
		, upper( TRIM( CS_CLMT_NM_LST ) )                    as                                     CS_CLMT_NM_LST 
		, upper( CS_CLMT_NM_MID )                            as                                     CS_CLMT_NM_MID 
		, upper( TRIM( CS_CLMT_SSN_NO ) )                    as                                     CS_CLMT_SSN_NO 
		, STT_ID_JUR                                         as                                         STT_ID_JUR 
		, CS_CTRPH_NO                                        as                                        CS_CTRPH_NO 
		, upper( TRIM( CS_CLS_CD ) )                         as                                          CS_CLS_CD 
		, upper( TRIM( CS_CLS_SUFX_NM ) )                    as                                     CS_CLS_SUFX_NM 
		, upper( TRIM( CS_NCCI_ACT_IN_EFF_TYP_CD ) )         as                          CS_NCCI_ACT_IN_EFF_TYP_CD 
		, upper( TRIM( CS_NCCI_LOSS_TYP_CD ) )               as                                CS_NCCI_LOSS_TYP_CD 
		, upper( TRIM( CS_NCCI_RCVR_TYP_CD ) )               as                                CS_NCCI_RCVR_TYP_CD 
		, upper( TRIM( CS_NCCI_COV_TYP_CD ) )                as                                 CS_NCCI_COV_TYP_CD 
		, upper( TRIM( CS_NCCI_SETL_TYP_CD ) )               as                                CS_NCCI_SETL_TYP_CD 
		, upper( TRIM( CS_MCO_TYP_CD ) )                     as                                      CS_MCO_TYP_CD 
		, upper( TRIM( CS_LOSS_DESC ) )                      as                                       CS_LOSS_DESC 
		, upper( TRIM( CS_NCCI_PRI_POB_TYP_CD ) )            as                             CS_NCCI_PRI_POB_TYP_CD 
		, upper( TRIM( CS_NCCI_POB_TYP_NM ) )                as                                 CS_NCCI_POB_TYP_NM 
		, upper( TRIM( CS_NCCI_NOI_TYP_CD ) )                as                                 CS_NCCI_NOI_TYP_CD 
		, upper( TRIM( CS_NCCI_NOI_TYP_NM ) )                as                                 CS_NCCI_NOI_TYP_NM 
		, upper( TRIM( CS_NCCI_CAUS_OF_LOSS_TYP_CD ) )       as                        CS_NCCI_CAUS_OF_LOSS_TYP_CD 
		, upper( TRIM( CS_CLMT_JOB_TTL ) )                   as                                    CS_CLMT_JOB_TTL 
		, upper( CS_LUMPSUM_IND )                            as                                     CS_LUMPSUM_IND 
		, upper( TRIM( CS_FRDT_CLM_CD ) )                    as                                     CS_FRDT_CLM_CD 
		, CS_SCHD_INDM_PCT                                   as                                   CS_SCHD_INDM_PCT 
		, STT_ID_EXPSR                                       as                                       STT_ID_EXPSR 
		, upper( TRIM( CS_INTFC_ENTY_ID_SRC_CD ) )           as                            CS_INTFC_ENTY_ID_SRC_CD 
		, cast( CS_EFF_DT as DATE )                          as                                          CS_EFF_DT 
		, cast( CS_END_DT as DATE )                          as                                          CS_END_DT 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CS
            ),

LOGIC_NIT as ( SELECT 
		  upper( TRIM( NCCI_INJR_TYP_NM ) )                  as                                   NCCI_INJR_TYP_NM 
		, upper( TRIM( NCCI_INJR_TYP_CD ) )                  as                                   NCCI_INJR_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_NIT
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  CS_ID                                              as                                              CS_ID
		, CS_TRAN_TYP_ID                                     as                                     CS_TRAN_TYP_ID
		, CS_CLM_NO                                          as                                          CS_CLM_NO
		, CS_PLCY_NO                                         as                                         CS_PLCY_NO
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, CS_PLCY_EFF_DT                                     as                                   CS_PLCY_EFF_DATE
		, CS_RISK_LOC_NO                                     as                                     CS_RISK_LOC_NO
		, CS_OCCR_DT                                         as                                         CS_OCCR_DT
		, CS_OCCR_RPT_DT                                     as                                     CS_OCCR_RPT_DT
		, CS_NCCI_STS_CD                                     as                                     CS_NCCI_STS_CD
		, CS_CLM_STT_TYP_NM                                  as                                  CS_CLM_STT_TYP_NM
		, CS_CLM_STS_TYP_NM                                  as                                  CS_CLM_STS_TYP_NM
		, CS_CLM_TYP_NM                                      as                                      CS_CLM_TYP_NM
		, CS_NCCI_INJR_TYP_CD                                as                                CS_NCCI_INJR_TYP_CD
		, CUST_ID_CLMT                                       as                                       CUST_ID_CLMT
		, CS_CLMT_NM_FST                                     as                                     CS_CLMT_NM_FST
		, CS_CLMT_NM_LST                                     as                                     CS_CLMT_NM_LST
		, CS_CLMT_NM_MID                                     as                                     CS_CLMT_NM_MID
		, CS_CLMT_SSN_NO                                     as                                     CS_CLMT_SSN_NO
		, STT_ID_JUR                                         as                                         STT_ID_JUR
		, CS_CTRPH_NO                                        as                                        CS_CTRPH_NO
		, CS_CLS_CD                                          as                                          CS_CLS_CD
		, CS_CLS_SUFX_NM                                     as                                     CS_CLS_SUFX_NM
		, CS_NCCI_ACT_IN_EFF_TYP_CD                          as                          CS_NCCI_ACT_IN_EFF_TYP_CD
		, CS_NCCI_LOSS_TYP_CD                                as                                CS_NCCI_LOSS_TYP_CD
		, CS_NCCI_RCVR_TYP_CD                                as                                CS_NCCI_RCVR_TYP_CD
		, CS_NCCI_COV_TYP_CD                                 as                                 CS_NCCI_COV_TYP_CD
		, CS_NCCI_SETL_TYP_CD                                as                                CS_NCCI_SETL_TYP_CD
		, CS_MCO_TYP_CD                                      as                                      CS_MCO_TYP_CD
		, CS_LOSS_DESC                                       as                                       CS_LOSS_DESC
		, CS_NCCI_PRI_POB_TYP_CD                             as                             CS_NCCI_PRI_POB_TYP_CD
		, CS_NCCI_POB_TYP_NM                                 as                                 CS_NCCI_POB_TYP_NM
		, CS_NCCI_NOI_TYP_CD                                 as                                 CS_NCCI_NOI_TYP_CD
		, CS_NCCI_NOI_TYP_NM                                 as                                 CS_NCCI_NOI_TYP_NM
		, CS_NCCI_CAUS_OF_LOSS_TYP_CD                        as                        CS_NCCI_CAUS_OF_LOSS_TYP_CD
		, CS_CLMT_JOB_TTL                                    as                                    CS_CLMT_JOB_TTL
		, CS_LUMPSUM_IND                                     as                                     CS_LUMPSUM_IND
		, CS_FRDT_CLM_CD                                     as                                     CS_FRDT_CLM_CD
		, CS_SCHD_INDM_PCT                                   as                                   CS_SCHD_INDM_PCT
		, STT_ID_EXPSR                                       as                                       STT_ID_EXPSR
		, CS_INTFC_ENTY_ID_SRC_CD                            as                            CS_INTFC_ENTY_ID_SRC_CD
		, CS_EFF_DT                                          as                                        CS_EFF_DATE
		, CS_END_DT                                          as                                        CS_END_DATE
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CS   ), 
RENAME_NIT as ( SELECT 
		  NCCI_INJR_TYP_NM                                   as                                   NCCI_INJR_TYP_NM
		, NCCI_INJR_TYP_CD                                   as                                   NCCI_INJR_TYP_CD
		, VOID_IND                                           as                                       NIT_VOID_IND 
				FROM     LOGIC_NIT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_NIT                            as ( SELECT * from    RENAME_NIT 
                                            WHERE NIT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CS as ( SELECT * 
				FROM  FILTER_CS
				LEFT JOIN FILTER_NIT ON  FILTER_CS.CS_NCCI_INJR_TYP_CD =  FILTER_NIT.NCCI_INJR_TYP_CD  )
SELECT 
 CS_ID
, CS_TRAN_TYP_ID
, CS_CLM_NO
, CS_PLCY_NO
, PLCY_PRD_ID
, CS_PLCY_EFF_DATE
, CS_RISK_LOC_NO
, CS_OCCR_DT
, CS_OCCR_RPT_DT
, CS_NCCI_STS_CD
, CS_CLM_STT_TYP_NM
, CS_CLM_STS_TYP_NM
, CS_CLM_TYP_NM
, CS_NCCI_INJR_TYP_CD
, NCCI_INJR_TYP_NM
, CUST_ID_CLMT
, CS_CLMT_NM_FST
, CS_CLMT_NM_LST
, CS_CLMT_NM_MID
, CS_CLMT_SSN_NO
, STT_ID_JUR
, CS_CTRPH_NO
, CS_CLS_CD
, CS_CLS_SUFX_NM
, CS_NCCI_ACT_IN_EFF_TYP_CD
, CS_NCCI_LOSS_TYP_CD
, CS_NCCI_RCVR_TYP_CD
, CS_NCCI_COV_TYP_CD
, CS_NCCI_SETL_TYP_CD
, CS_MCO_TYP_CD
, CS_LOSS_DESC
, CS_NCCI_PRI_POB_TYP_CD
, CS_NCCI_POB_TYP_NM
, CS_NCCI_NOI_TYP_CD
, CS_NCCI_NOI_TYP_NM
, CS_NCCI_CAUS_OF_LOSS_TYP_CD
, CS_CLMT_JOB_TTL
, CS_LUMPSUM_IND
, CS_FRDT_CLM_CD
, CS_SCHD_INDM_PCT
, STT_ID_EXPSR
, CS_INTFC_ENTY_ID_SRC_CD
, CS_EFF_DATE
, CS_END_DATE
, AUDIT_USER_CREA_DTM
, AUDIT_USER_ID_CREA
, AUDIT_USER_ID_UPDT
, AUDIT_USER_UPDT_DTM
, VOID_IND
from CS
      );
    