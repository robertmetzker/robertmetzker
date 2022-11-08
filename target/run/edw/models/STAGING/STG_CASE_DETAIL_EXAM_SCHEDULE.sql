

      create or replace  table DEV_EDW.STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE  as
      (---- SRC LAYER ----
WITH
SRC_CDES as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_EXM_SCH ),
SRC_CDERT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_EXM_REQS_TYPE ),
SRC_CPST as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PROV_SPL_TYPE ),
SRC_CPST2 as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PROV_SPL_TYPE ),
SRC_LT as ( SELECT *     from     DEV_VIEWS.PCMP.LANGUAGE_TYPE ),
SRC_S as ( SELECT *     from     DEV_VIEWS.PCMP.STATE ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.COUNTRY ),
SRC_CDART as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_ADNDM_REQS_TYPE ),
//SRC_CDES as ( SELECT *     from     CASE_DETAIL_EXM_SCH) ,
//SRC_CDERT as ( SELECT *     from     CASE_DETAIL_EXM_REQS_TYPE) ,
//SRC_CPST as ( SELECT *     from     CASE_PROV_SPL_TYPE) ,
//SRC_CPST2 as ( SELECT *     from     CASE_PROV_SPL_TYPE) ,
//SRC_LT as ( SELECT *     from     LANGUAGE_TYPE) ,
//SRC_S as ( SELECT *     from     STATE) ,
//SRC_C as ( SELECT *     from     COUNTRY) ,
//SRC_CDART as ( SELECT *     from     CASE_DETAIL_ADNDM_REQS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CDES as ( SELECT 
		  CDES_ID                                            AS                                            CDES_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CD_EXM_REQS_TYP_CD ) )                AS                                 CD_EXM_REQS_TYP_CD 
		, upper( TRIM( CPS_TYP_CD ) )                        AS                                         CPS_TYP_CD 
		, upper( TRIM( CPS_TYP_CD_SCND ) )                   AS                                    CPS_TYP_CD_SCND 
		, upper( TRIM( LANG_TYP_CD ) )                       AS                                        LANG_TYP_CD 
		, cast( CDES_EXM_DT as DATE )                        AS                                        CDES_EXM_DT 
		, cast( CDES_EXM_RPT_RECV_DT as DATE )               AS                               CDES_EXM_RPT_RECV_DT 
		, CDES_EXM_PHYS_IMPR_RT                              AS                              CDES_EXM_PHYS_IMPR_RT 
		, CDES_EXM_FNL_IMPR_RT                               AS                               CDES_EXM_FNL_IMPR_RT 
		, upper( TRIM( CDES_CLMT_AVL_NAR_DESC ) )            AS                             CDES_CLMT_AVL_NAR_DESC 
		, upper( CDES_CLMT_AVL_MON_IND )                     AS                              CDES_CLMT_AVL_MON_IND 
		, upper( CDES_CLMT_AVL_TUE_IND )                     AS                              CDES_CLMT_AVL_TUE_IND 
		, upper( CDES_CLMT_AVL_WED_IND )                     AS                              CDES_CLMT_AVL_WED_IND 
		, upper( CDES_CLMT_AVL_THU_IND )                     AS                              CDES_CLMT_AVL_THU_IND 
		, upper( CDES_CLMT_AVL_FRI_IND )                     AS                              CDES_CLMT_AVL_FRI_IND 
		, upper( CDES_CLMT_AVL_SAT_IND )                     AS                              CDES_CLMT_AVL_SAT_IND 
		, upper( CDES_CLMT_AVL_SUN_IND )                     AS                              CDES_CLMT_AVL_SUN_IND 
		, upper( TRIM( CDES_SPL_REQD ) )                     AS                                      CDES_SPL_REQD 
		, upper( CDES_ITPRT_NEED_IND )                       AS                                CDES_ITPRT_NEED_IND 
		, upper( TRIM( CDES_EXM_ADDR_STR_1 ) )               AS                                CDES_EXM_ADDR_STR_1 
		, upper( TRIM( CDES_EXM_ADDR_STR_2 ) )               AS                                CDES_EXM_ADDR_STR_2 
		, upper( TRIM( CDES_EXM_ADDR_CITY_NM ) )             AS                              CDES_EXM_ADDR_CITY_NM 
		, upper( TRIM( CDES_EXM_ADDR_CNTY_NM ) )             AS                              CDES_EXM_ADDR_CNTY_NM 
		, upper( TRIM( CDES_EXM_ADDR_POST_CD ) )             AS                              CDES_EXM_ADDR_POST_CD 
		, STT_ID                                             AS                                             STT_ID 
		, CNTRY_ID                                           AS                                           CNTRY_ID 
		, upper( TRIM( CDES_EXM_UN_NRML_SITU ) )             AS                              CDES_EXM_UN_NRML_SITU 
		, upper( TRIM( CDES_EXM_QA ) )                       AS                                        CDES_EXM_QA 
		, upper( CDES_GRTT_45_IND )                          AS                                   CDES_GRTT_45_IND 
		, upper( CDES_TRVL_REMB_IND )                        AS                                 CDES_TRVL_REMB_IND 
		, upper( CDES_ADDTNL_TST_IND )                       AS                                CDES_ADDTNL_TST_IND 
		, upper( TRIM( CDES_REQS_ADDTNL_TST ) )              AS                               CDES_REQS_ADDTNL_TST 
		, upper( CDES_ADNDM_REQS_IND )                       AS                                CDES_ADNDM_REQS_IND 
		, upper( TRIM( CD_ADNDM_REQS_TYP_CD ) )              AS                               CD_ADNDM_REQS_TYP_CD 
		, upper( CDES_RSLT_SUSPD_IND )                       AS                                CDES_RSLT_SUSPD_IND 
		, upper( TRIM( CDES_ADR_NO ) )                       AS                                        CDES_ADR_NO 
		, upper( TRIM( CDES_ADR_TYP ) )                      AS                                       CDES_ADR_TYP 
		, upper( TRIM( CDES_ADR_TRT_REQSTR ) )               AS                                CDES_ADR_TRT_REQSTR 
		, upper( TRIM( CDES_ADR_TRT_DSP ) )                  AS                                   CDES_ADR_TRT_DSP 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDES
            ),
LOGIC_CDERT as ( SELECT 
		  upper( TRIM( CD_EXM_REQS_TYP_NM ) )                AS                                 CD_EXM_REQS_TYP_NM 
		, upper( TRIM( CD_EXM_REQS_TYP_CD ) )                AS                                 CD_EXM_REQS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDERT
            ),
LOGIC_CPST as ( SELECT 
		  upper( TRIM( CPS_TYP_NM ) )                        AS                                         CPS_TYP_NM 
		, upper( TRIM( CPS_TYP_CD ) )                        AS                                         CPS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CPST
            ),
LOGIC_CPST2 as ( SELECT 
		  upper( TRIM( CPS_TYP_NM ) )                        AS                                         CPS_TYP_NM 
		, upper( TRIM( CPS_TYP_CD ) )                        AS                                         CPS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CPST2
            ),
LOGIC_LT as ( SELECT 
		  upper( TRIM( LANG_TYP_NM ) )                       AS                                        LANG_TYP_NM 
		, upper( TRIM( LANG_TYP_CD ) )                       AS                                        LANG_TYP_CD 
		, upper( LANG_TYP_VOID_IND )                         AS                                  LANG_TYP_VOID_IND 
		from SRC_LT
            ),
LOGIC_S as ( SELECT 
		  upper( TRIM( STT_ABRV ) )                          AS                                           STT_ABRV 
		, upper( TRIM( STT_NM ) )                            AS                                             STT_NM 
		, STT_ID                                             AS                                             STT_ID 
		, upper( STT_VOID_IND )                              AS                                       STT_VOID_IND 
		from SRC_S
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CNTRY_NM ) )                          AS                                           CNTRY_NM 
		, CNTRY_ID                                           AS                                           CNTRY_ID 
		, upper( CNTRY_VOID_IND )                            AS                                     CNTRY_VOID_IND 
		from SRC_C
            ),
LOGIC_CDART as ( SELECT 
		  upper( TRIM( CD_ADNDM_REQS_TYP_NM ) )              AS                               CD_ADNDM_REQS_TYP_NM 
		, upper( TRIM( CD_ADNDM_REQS_TYP_CD ) )              AS                               CD_ADNDM_REQS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDART
            )

---- RENAME LAYER ----
,

RENAME_CDES as ( SELECT 
		  CDES_ID                                            as                                            CDES_ID
		, CASE_ID                                            as                                            CASE_ID
		, CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD
		, CPS_TYP_CD                                         as                                         CPS_TYP_CD
		, CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND
		, LANG_TYP_CD                                        as                                        LANG_TYP_CD
		, CDES_EXM_DT                                        as                                        CDES_EXM_DT
		, CDES_EXM_RPT_RECV_DT                               as                               CDES_EXM_RPT_RECV_DT
		, CDES_EXM_PHYS_IMPR_RT                              as                              CDES_EXM_PHYS_IMPR_RT
		, CDES_EXM_FNL_IMPR_RT                               as                               CDES_EXM_FNL_IMPR_RT
		, CDES_CLMT_AVL_NAR_DESC                             as                             CDES_CLMT_AVL_NAR_DESC
		, CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND
		, CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND
		, CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND
		, CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND
		, CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND
		, CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND
		, CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND
		, CDES_SPL_REQD                                      as                                      CDES_SPL_REQD
		, CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND
		, CDES_EXM_ADDR_STR_1                                as                                CDES_EXM_ADDR_STR_1
		, CDES_EXM_ADDR_STR_2                                as                                CDES_EXM_ADDR_STR_2
		, CDES_EXM_ADDR_CITY_NM                              as                              CDES_EXM_ADDR_CITY_NM
		, CDES_EXM_ADDR_CNTY_NM                              as                              CDES_EXM_ADDR_CNTY_NM
		, CDES_EXM_ADDR_POST_CD                              as                              CDES_EXM_ADDR_POST_CD
		, STT_ID                                             as                                             STT_ID
		, CNTRY_ID                                           as                                           CNTRY_ID
		, CDES_EXM_UN_NRML_SITU                              as                              CDES_EXM_UN_NRML_SITU
		, CDES_EXM_QA                                        as                                        CDES_EXM_QA
		, CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND
		, CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND
		, CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND
		, CDES_REQS_ADDTNL_TST                               as                               CDES_REQS_ADDTNL_TST
		, CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD
		, CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND
		, CDES_ADR_NO                                        as                                        CDES_ADR_NO
		, CDES_ADR_TYP                                       as                                       CDES_ADR_TYP
		, CDES_ADR_TRT_REQSTR                                as                                CDES_ADR_TRT_REQSTR
		, CDES_ADR_TRT_DSP                                   as                                   CDES_ADR_TRT_DSP
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CDES   ), 
RENAME_CDERT as ( SELECT 
		  CD_EXM_REQS_TYP_NM                                 as                                 CD_EXM_REQS_TYP_NM
		, CD_EXM_REQS_TYP_CD                                 as                           CDERT_CD_EXM_REQS_TYP_CD
		, VOID_IND                                           as                                     CDERT_VOID_IND 
				FROM     LOGIC_CDERT   ), 
RENAME_CPST as ( SELECT 
		  CPS_TYP_NM                                         as                                         CPS_TYP_NM
		, CPS_TYP_CD                                         as                                    CPST_CPS_TYP_CD
		, VOID_IND                                           as                                      CPST_VOID_IND 
				FROM     LOGIC_CPST   ), 
RENAME_CPST2 as ( SELECT 
		  CPS_TYP_NM                                         as                                    CPS_TYP_NM_SCND
		, CPS_TYP_CD                                         as                                   CPST2_CPS_TYP_CD
		, VOID_IND                                           as                                     CPST2_VOID_IND 
				FROM     LOGIC_CPST2   ), 
RENAME_LT as ( SELECT 
		  LANG_TYP_NM                                        as                                        LANG_TYP_NM
		, LANG_TYP_CD                                        as                                     LT_LANG_TYP_CD
		, LANG_TYP_VOID_IND                                  as                                  LANG_TYP_VOID_IND 
				FROM     LOGIC_LT   ), 
RENAME_S as ( SELECT 
		  STT_ABRV                                           as                             CDES_EXM_ADDR_STT_ABRV
		, STT_NM                                             as                               CDES_EXM_ADDR_STT_NM
		, STT_ID                                             as                                           S_STT_ID
		, STT_VOID_IND                                       as                                       STT_VOID_IND 
				FROM     LOGIC_S   ), 
RENAME_C as ( SELECT 
		  CNTRY_NM                                           as                             CDES_EXM_ADDR_CNTRY_NM
		, CNTRY_ID                                           as                                         C_CNTRY_ID
		, CNTRY_VOID_IND                                     as                                     CNTRY_VOID_IND 
				FROM     LOGIC_C   ), 
RENAME_CDART as ( SELECT 
		  CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM
		, CD_ADNDM_REQS_TYP_CD                               as                         CDART_CD_ADNDM_REQS_TYP_CD
		, VOID_IND                                           as                                     CDART_VOID_IND 
				FROM     LOGIC_CDART   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDES                           as ( SELECT * from    RENAME_CDES   ),
FILTER_CDERT                          as ( SELECT * from    RENAME_CDERT 
				WHERE CDERT_VOID_IND = 'N'  ),
FILTER_CPST                           as ( SELECT * from    RENAME_CPST 
				WHERE CPST_VOID_IND = 'N'  ),
FILTER_CPST2                          as ( SELECT * from    RENAME_CPST2 
				WHERE CPST2_VOID_IND = 'N'  ),
FILTER_CDART                          as ( SELECT * from    RENAME_CDART 
				WHERE CDART_VOID_IND = 'N'  ),
FILTER_LT                             as ( SELECT * from    RENAME_LT 
				WHERE LANG_TYP_VOID_IND = 'N'  ),
FILTER_S                              as ( SELECT * from    RENAME_S 
				WHERE STT_VOID_IND = 'N'  ),
FILTER_C                              as ( SELECT * from    RENAME_C 
				WHERE CNTRY_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CDES as ( SELECT * 
				FROM  FILTER_CDES
				LEFT JOIN FILTER_CDERT ON  FILTER_CDES.CD_EXM_REQS_TYP_CD =  FILTER_CDERT.CDERT_CD_EXM_REQS_TYP_CD 
						LEFT JOIN FILTER_CPST ON  FILTER_CDES.CPS_TYP_CD =  FILTER_CPST.CPST_CPS_TYP_CD 
						LEFT JOIN FILTER_CPST2 ON  FILTER_CDES.CPS_TYP_CD_SCND =  FILTER_CPST2.CPST2_CPS_TYP_CD 
						LEFT JOIN FILTER_CDART ON  FILTER_CDES.CD_ADNDM_REQS_TYP_CD =  FILTER_CDART.CDART_CD_ADNDM_REQS_TYP_CD 
						LEFT JOIN FILTER_LT ON  FILTER_CDES.LANG_TYP_CD =  FILTER_LT.LT_LANG_TYP_CD 
						LEFT JOIN FILTER_S ON  FILTER_CDES.STT_ID =  FILTER_S.S_STT_ID 
						LEFT JOIN FILTER_C ON  FILTER_CDES.CNTRY_ID =  FILTER_C.C_CNTRY_ID  )
SELECT * 
from CDES
      );
    