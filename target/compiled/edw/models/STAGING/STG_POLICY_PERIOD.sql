---- SRC LAYER ----
WITH
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_PNRRT as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_NON_RENEWAL_REASON_TYPE ),
SRC_STRT as ( SELECT *     from     DEV_VIEWS.PCMP.SHORT_TERM_REASON_TYPE ),
SRC_A as ( SELECT *     from     DEV_VIEWS.PCMP.AGREEMENT ),
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_PNRRT as ( SELECT *     from     POLICY_NON_RENEWAL_REASON_TYPE) ,
//SRC_STRT as ( SELECT *     from     SHORT_TERM_REASON_TYPE) ,
//SRC_A as ( SELECT *     from     AGREEMENT) ,

---- LOGIC LAYER -----

LOGIC_PP as ( SELECT 
		  PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		, cast( PLCY_PRD_EFF_DT as DATE )                    AS                                    PLCY_PRD_EFF_DT 
		, cast( PLCY_PRD_END_DT as DATE )                    AS                                    PLCY_PRD_END_DT 
		, cast( PLCY_PRD_ORIG_END_DT as DATE )               AS                               PLCY_PRD_ORIG_END_DT 
		, upper( PLCY_PRD_PRO_RT_IND )                       AS                                PLCY_PRD_PRO_RT_IND 
		, upper( PLCY_PRD_DO_NOT_RN_IND )                    AS                             PLCY_PRD_DO_NOT_RN_IND 
		, upper( PLCY_PRD_RN_TO_PLCY_IND )                   AS                            PLCY_PRD_RN_TO_PLCY_IND 
		, upper( PLCY_PRD_KEY_ACCT_IND )                     AS                              PLCY_PRD_KEY_ACCT_IND 
		, upper( PLCY_PRD_CONT_COV_IND )                     AS                              PLCY_PRD_CONT_COV_IND 
		, upper( TRIM( PLCY_PRD_DESC_OF_OPER ) )             AS                              PLCY_PRD_DESC_OF_OPER 
		, cast( PLCY_PRD_NRN_DT as DATE )                    AS                                    PLCY_PRD_NRN_DT 
		, upper( TRIM( PLCY_NRN_RSN_TYP_CD ) )               AS                                PLCY_NRN_RSN_TYP_CD 
		, upper( TRIM( INVC_DD_TYP_CD ) )                    AS                                     INVC_DD_TYP_CD 
		, upper( TRIM( SHR_TRM_RSN_TYP_CD ) )                AS                                 SHR_TRM_RSN_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PP
            ),
LOGIC_PNRRT as ( SELECT 
		  upper( TRIM( PLCY_NRN_RSN_TYP_NM ) )               AS                                PLCY_NRN_RSN_TYP_NM 
		, upper( TRIM( PLCY_NRN_RSN_TYP_CD ) )               AS                                PLCY_NRN_RSN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PNRRT
            ),
LOGIC_STRT as ( SELECT 
		  upper( TRIM( SHR_TRM_RSN_TYP_NM ) )                AS                                 SHR_TRM_RSN_TYP_NM 
		, upper( TRIM( SHR_TRM_RSN_TYP_RSN ) )               AS                                SHR_TRM_RSN_TYP_RSN 
		, upper( TRIM( SHR_TRM_RSN_TYP_CD ) )                AS                                 SHR_TRM_RSN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_STRT
            ),
LOGIC_A as ( SELECT 
		  AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( AGRE_TYP_CD ) )                       AS                                        AGRE_TYP_CD 
		from SRC_A
            )

---- RENAME LAYER ----
,

RENAME_PP as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
		, PLCY_PRD_ORIG_END_DT                               as                               PLCY_PRD_ORIG_END_DT
		, PLCY_PRD_PRO_RT_IND                                as                                PLCY_PRD_PRO_RT_IND
		, PLCY_PRD_DO_NOT_RN_IND                             as                             PLCY_PRD_DO_NOT_RN_IND
		, PLCY_PRD_RN_TO_PLCY_IND                            as                            PLCY_PRD_RN_TO_PLCY_IND
		, PLCY_PRD_KEY_ACCT_IND                              as                              PLCY_PRD_KEY_ACCT_IND
		, PLCY_PRD_CONT_COV_IND                              as                              PLCY_PRD_CONT_COV_IND
		, PLCY_PRD_DESC_OF_OPER                              as                              PLCY_PRD_DESC_OF_OPER
		, PLCY_PRD_NRN_DT                                    as                                    PLCY_PRD_NRN_DT
		, PLCY_NRN_RSN_TYP_CD                                as                                PLCY_NRN_RSN_TYP_CD
		, INVC_DD_TYP_CD                                     as                                     INVC_DD_TYP_CD
		, SHR_TRM_RSN_TYP_CD                                 as                                 SHR_TRM_RSN_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PP   ), 
RENAME_PNRRT as ( SELECT 
		  PLCY_NRN_RSN_TYP_NM                                as                                PLCY_NRN_RSN_TYP_NM
		, PLCY_NRN_RSN_TYP_CD                                as                          PNRRT_PLCY_NRN_RSN_TYP_CD
		, VOID_IND                                           as                                     PNRRT_VOID_IND 
				FROM     LOGIC_PNRRT   ), 
RENAME_STRT as ( SELECT 
		  SHR_TRM_RSN_TYP_NM                                 as                                 SHR_TRM_RSN_TYP_NM
		, SHR_TRM_RSN_TYP_RSN                                as                                SHR_TRM_RSN_TYP_RSN
		, SHR_TRM_RSN_TYP_CD                                 as                            STRT_SHR_TRM_RSN_TYP_CD
		, VOID_IND                                           as                                      STRT_VOID_IND 
				FROM     LOGIC_STRT   ), 
RENAME_A as ( SELECT 
		  AGRE_ID                                            as                                          A_AGRE_ID
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD 
				FROM     LOGIC_A   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * from    RENAME_PP   ),
FILTER_A                              as ( SELECT * from    RENAME_A 
				WHERE AGRE_TYP_CD = 'PLCY'  ),
FILTER_PNRRT                          as ( SELECT * from    RENAME_PNRRT 
				WHERE PNRRT_VOID_IND = 'N'  ),
FILTER_STRT                           as ( SELECT * from    RENAME_STRT 
				WHERE STRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PP as ( SELECT * 
				FROM  FILTER_PP
				        INNER JOIN FILTER_A ON  FILTER_PP.AGRE_ID =  FILTER_A.A_AGRE_ID 
						LEFT JOIN FILTER_PNRRT ON  FILTER_PP.PLCY_NRN_RSN_TYP_CD =  FILTER_PNRRT.PNRRT_PLCY_NRN_RSN_TYP_CD 
						LEFT JOIN FILTER_STRT ON  FILTER_PP.SHR_TRM_RSN_TYP_CD =  FILTER_STRT.STRT_SHR_TRM_RSN_TYP_CD  )
SELECT 
  PLCY_PRD_ID
, AGRE_ID
, PLCY_NO
, PLCY_PRD_EFF_DT
, PLCY_PRD_END_DT
, PLCY_PRD_ORIG_END_DT
, PLCY_PRD_PRO_RT_IND
, PLCY_PRD_DO_NOT_RN_IND
, PLCY_PRD_RN_TO_PLCY_IND
, PLCY_PRD_KEY_ACCT_IND
, PLCY_PRD_CONT_COV_IND
, PLCY_PRD_DESC_OF_OPER
, PLCY_PRD_NRN_DT
, PLCY_NRN_RSN_TYP_CD
, PLCY_NRN_RSN_TYP_NM
, INVC_DD_TYP_CD
, SHR_TRM_RSN_TYP_CD
, SHR_TRM_RSN_TYP_NM
, SHR_TRM_RSN_TYP_RSN
, AUDIT_USER_ID_CREA
, AUDIT_USER_CREA_DTM
, AUDIT_USER_ID_UPDT
, AUDIT_USER_UPDT_DTM
, VOID_IND 
, case when PLCY_PRD_EFF_DT = min(PLCY_PRD_EFF_DT) over (partition by PLCY_NO, AGRE_ID) then 'Y' else 'N' END AS NEW_POLICY_IND

from PP