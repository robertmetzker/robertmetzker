

      create or replace  table DEV_EDW.STAGING.STG_CASES  as
      (---- SRC LAYER ----
WITH
SRC_CS as ( SELECT *     from     DEV_VIEWS.PCMP.CASES ),
SRC_ACT as ( SELECT *     from     DEV_VIEWS.PCMP.APPLICATION_CONTEXT_TYPE ),
SRC_CCT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_CATEGORY_TYPE ),
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_TYPE ),
SRC_CPT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PRIORITY_TYPE ),
SRC_CRT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_RESOLUTION_TYPE ),
SRC_CST as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_SOURCE_TYPE ),
//SRC_CS as ( SELECT *     from     CASES) ,
//SRC_ACT as ( SELECT *     from     APPLICATION_CONTEXT_TYPE) ,
//SRC_CCT as ( SELECT *     from     CASE_CATEGORY_TYPE) ,
//SRC_CT as ( SELECT *     from     CASE_TYPE) ,
//SRC_CPT as ( SELECT *     from     CASE_PRIORITY_TYPE) ,
//SRC_CRT as ( SELECT *     from     CASE_RESOLUTION_TYPE) ,
//SRC_CST as ( SELECT *     from     CASE_SOURCE_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CS as ( SELECT 
		  CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CASE_NO ) )                           AS                                            CASE_NO 
		, CASE_CNTX_ID                                       AS                                       CASE_CNTX_ID 
		, upper( TRIM( CASE_CNTX_NO ) )                      AS                                       CASE_CNTX_NO 
		, upper( TRIM( CASE_NM ) )                           AS                                            CASE_NM 
		, upper( TRIM( CASE_EXTRNL_NO ) )                    AS                                     CASE_EXTRNL_NO 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   AS                                    APP_CNTX_TYP_CD 
		, upper( TRIM( CASE_CTG_TYP_CD ) )                   AS                                    CASE_CTG_TYP_CD 
		, upper( TRIM( CASE_TYP_CD ) )                       AS                                        CASE_TYP_CD 
		, cast( CASE_INT_DT as DATE )                        AS                                        CASE_INT_DT 
		, cast( CASE_EFF_DT as DATE )                        AS                                        CASE_EFF_DT 
		, cast( CASE_DUE_DT as DATE )                        AS                                        CASE_DUE_DT 
		, cast( CASE_COMP_DT as DATE )                       AS                                       CASE_COMP_DT 
		, upper( TRIM( CASE_RSN_SUM_TXT ) )                  AS                                   CASE_RSN_SUM_TXT 
		, upper( TRIM( CASE_PRTY_TYP_CD ) )                  AS                                   CASE_PRTY_TYP_CD 
		, upper( TRIM( CASE_RSOL_TYP_CD ) )                  AS                                   CASE_RSOL_TYP_CD 
		, upper( TRIM( CASE_SRC_TYP_CD ) )                   AS                                    CASE_SRC_TYP_CD 
		, upper( TRIM( CASE_ACTN_PLN_TXT ) )                 AS                                  CASE_ACTN_PLN_TXT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CS
            ),
LOGIC_ACT as ( SELECT 
		  upper( TRIM( APP_CNTX_TYP_NM ) )                   AS                                    APP_CNTX_TYP_NM 
		, upper( TRIM( APP_CNTX_TYP_CD ) )                   AS                                    APP_CNTX_TYP_CD 
		, upper( APP_CNTX_TYP_VOID_IND )                     AS                              APP_CNTX_TYP_VOID_IND 
		from SRC_ACT
            ),
LOGIC_CCT as ( SELECT 
		  upper( TRIM( CASE_CTG_TYP_NM ) )                   AS                                    CASE_CTG_TYP_NM 
		, upper( TRIM( CASE_CTG_TYP_CD ) )                   AS                                    CASE_CTG_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CCT
            ),
LOGIC_CT as ( SELECT 
		  upper( TRIM( CASE_TYP_NM ) )                       AS                                        CASE_TYP_NM 
		, upper( TRIM( CASE_TYP_CD ) )                       AS                                        CASE_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CT
            ),
LOGIC_CPT as ( SELECT 
		  upper( TRIM( CASE_PRTY_TYP_NM ) )                  AS                                   CASE_PRTY_TYP_NM 
		, upper( TRIM( CASE_PRTY_TYP_CD ) )                  AS                                   CASE_PRTY_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CPT
            ),
LOGIC_CRT as ( SELECT 
		  upper( TRIM( CASE_RSOL_TYP_NM ) )                  AS                                   CASE_RSOL_TYP_NM 
		, upper( TRIM( CASE_RSOL_TYP_CD ) )                  AS                                   CASE_RSOL_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CRT
            ),
LOGIC_CST as ( SELECT 
		  upper( TRIM( CASE_SRC_TYP_NM ) )                   AS                                    CASE_SRC_TYP_NM 
		, upper( TRIM( CASE_SRC_TYP_CD ) )                   AS                                    CASE_SRC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CST
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID
		, CASE_NO                                            as                                            CASE_NO
		, CASE_CNTX_ID                                       as                                       CASE_CNTX_ID
		, CASE_CNTX_NO                                       as                                       CASE_CNTX_NO
		, CASE_NM                                            as                                            CASE_NM
		, CASE_EXTRNL_NO                                     as                                     CASE_EXTRNL_NO
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD
		, CASE_INT_DT                                        as                                        CASE_INT_DT
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT
		, CASE_COMP_DT                                       as                                       CASE_COMP_DT
		, CASE_RSN_SUM_TXT                                   as                                   CASE_RSN_SUM_TXT
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD
		, CASE_SRC_TYP_CD                                    as                                    CASE_SRC_TYP_CD
		, CASE_ACTN_PLN_TXT                                  as                                  CASE_ACTN_PLN_TXT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CS   ), 
RENAME_ACT as ( SELECT 
		  APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM
		, APP_CNTX_TYP_CD                                    as                                ACT_APP_CNTX_TYP_CD
		, APP_CNTX_TYP_VOID_IND                              as                              APP_CNTX_TYP_VOID_IND 
				FROM     LOGIC_ACT   ), 
RENAME_CCT as ( SELECT 
		  CASE_CTG_TYP_NM                                    as                                    CASE_CTG_TYP_NM
		, CASE_CTG_TYP_CD                                    as                                CCT_CASE_CTG_TYP_CD
		, VOID_IND                                           as                                       CCT_VOID_IND 
				FROM     LOGIC_CCT   ), 
RENAME_CT as ( SELECT 
		  CASE_TYP_NM                                        as                                        CASE_TYP_NM
		, CASE_TYP_CD                                        as                                     CT_CASE_TYP_CD
		, VOID_IND                                           as                                        CT_VOID_IND 
				FROM     LOGIC_CT   ), 
RENAME_CPT as ( SELECT 
		  CASE_PRTY_TYP_NM                                   as                                   CASE_PRTY_TYP_NM
		, CASE_PRTY_TYP_CD                                   as                               CPT_CASE_PRTY_TYP_CD
		, VOID_IND                                           as                                       CPT_VOID_IND 
				FROM     LOGIC_CPT   ), 
RENAME_CRT as ( SELECT 
		  CASE_RSOL_TYP_NM                                   as                                   CASE_RSOL_TYP_NM
		, CASE_RSOL_TYP_CD                                   as                               CRT_CASE_RSOL_TYP_CD
		, VOID_IND                                           as                                       CRT_VOID_IND 
				FROM     LOGIC_CRT   ), 
RENAME_CST as ( SELECT 
		  CASE_SRC_TYP_NM                                    as                                    CASE_SRC_TYP_NM
		, CASE_SRC_TYP_CD                                    as                                CST_CASE_SRC_TYP_CD
		, VOID_IND                                           as                                       CST_VOID_IND 
				FROM     LOGIC_CST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_ACT                            as ( SELECT * from    RENAME_ACT 
				WHERE APP_CNTX_TYP_VOID_IND = 'N'  ),
FILTER_CT                             as ( SELECT * from    RENAME_CT 
				WHERE CT_VOID_IND = 'N'  ),
FILTER_CCT                            as ( SELECT * from    RENAME_CCT 
				WHERE CCT_VOID_IND = 'N'  ),
FILTER_CST                            as ( SELECT * from    RENAME_CST 
				WHERE CST_VOID_IND = 'N'  ),
FILTER_CPT                            as ( SELECT * from    RENAME_CPT 
				WHERE CPT_VOID_IND = 'N'  ),
FILTER_CRT                            as ( SELECT * from    RENAME_CRT 
				WHERE CRT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CS as ( SELECT * 
				FROM  FILTER_CS
				LEFT JOIN FILTER_ACT ON  FILTER_CS.APP_CNTX_TYP_CD =  FILTER_ACT.ACT_APP_CNTX_TYP_CD 
						LEFT JOIN FILTER_CT ON  FILTER_CS.CASE_TYP_CD =  FILTER_CT.CT_CASE_TYP_CD 
						LEFT JOIN FILTER_CCT ON  FILTER_CS.CASE_CTG_TYP_CD =  FILTER_CCT.CCT_CASE_CTG_TYP_CD 
						LEFT JOIN FILTER_CST ON  FILTER_CS.CASE_SRC_TYP_CD =  FILTER_CST.CST_CASE_SRC_TYP_CD 
						LEFT JOIN FILTER_CPT ON  FILTER_CS.CASE_PRTY_TYP_CD =  FILTER_CPT.CPT_CASE_PRTY_TYP_CD 
						LEFT JOIN FILTER_CRT ON  FILTER_CS.CASE_RSOL_TYP_CD =  FILTER_CRT.CRT_CASE_RSOL_TYP_CD  )
SELECT * 
from CS
      );
    