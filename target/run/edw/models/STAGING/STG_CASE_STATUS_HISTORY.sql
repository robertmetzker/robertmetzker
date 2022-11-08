

      create or replace  table DEV_EDW.STAGING.STG_CASE_STATUS_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_CSSH as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_STATE_STATUS_HISTORY ),
SRC_CS as ( SELECT *     from     DEV_VIEWS.PCMP.CASES ),
SRC_STT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_STATE_TYPE ),
SRC_STS as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_STATUS_TYPE ),
SRC_RSN as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_STATUS_REASON_TYPE ),
//SRC_CSSH as ( SELECT *     from     CASE_STATE_STATUS_HISTORY) ,
//SRC_CS as ( SELECT *     from     CASES) ,
//SRC_STT as ( SELECT *     from     CASE_STATE_TYPE) ,
//SRC_STS as ( SELECT *     from     CASE_STATUS_TYPE) ,
//SRC_RSN as ( SELECT *     from     CASE_STATUS_REASON_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CSSH as ( SELECT 
		  HIST_ID                                            AS                                            HIST_ID 
		, CASE_STT_STS_ID                                    AS                                    CASE_STT_STS_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, cast( CASE_STT_STS_STT_EFF_DT as DATE )            AS                            CASE_STT_STS_STT_EFF_DT 
		, upper( TRIM( CASE_STT_TYP_CD ) )                   AS                                    CASE_STT_TYP_CD 
		, cast( CASE_STT_STS_STS_EFF_DT as DATE )            AS                            CASE_STT_STS_STS_EFF_DT 
		, upper( TRIM( CASE_STS_TYP_CD ) )                   AS                                    CASE_STS_TYP_CD 
		, cast( CASE_STT_STS_STS_RSN_EFF_DT as DATE )        AS                        CASE_STT_STS_STS_RSN_EFF_DT 
		, upper( TRIM( CASE_STS_RSN_TYP_CD ) )               AS                                CASE_STS_RSN_TYP_CD 
		, upper( TRIM( CASE_STT_STS_COMT_TXT ) )             AS                              CASE_STT_STS_COMT_TXT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM                                       AS                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       AS                                       HIST_END_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CSSH
            ),
LOGIC_CS as ( SELECT 
		  upper( TRIM( CASE_NO ) )                           AS                                            CASE_NO 
		, CASE_ID                                            AS                                            CASE_ID 
		from SRC_CS
            ),
LOGIC_STT as ( SELECT 
		  upper( TRIM( CASE_STT_TYP_NM ) )                   AS                                    CASE_STT_TYP_NM 
		, upper( TRIM( CASE_STT_TYP_CD ) )                   AS                                    CASE_STT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_STT
            ),
LOGIC_STS as ( SELECT 
		  upper( TRIM( CASE_STS_TYP_NM ) )                   AS                                    CASE_STS_TYP_NM 
		, upper( TRIM( CASE_STS_TYP_CD ) )                   AS                                    CASE_STS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_STS
            ),
LOGIC_RSN as ( SELECT 
		  upper( TRIM( CASE_STS_RSN_TYP_NM ) )               AS                                CASE_STS_RSN_TYP_NM 
		, upper( TRIM( CASE_STS_RSN_TYP_CD ) )               AS                                CASE_STS_RSN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_RSN
            )

---- RENAME LAYER ----
,

RENAME_CSSH as ( SELECT 
		  HIST_ID                                            as                             CASE_STATUS_HISTORY_ID
		, CASE_STT_STS_ID                                    as                                    CASE_STT_STS_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_STT_STS_STT_EFF_DT                            as                            CASE_STT_STS_STT_EFF_DT
		, CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD
		, CASE_STT_STS_STS_EFF_DT                            as                            CASE_STT_STS_STS_EFF_DT
		, CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD
		, CASE_STT_STS_STS_RSN_EFF_DT                        as                        CASE_STT_STS_STS_RSN_EFF_DT
		, CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD
		, CASE_STT_STS_COMT_TXT                              as                              CASE_STT_STS_COMT_TXT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CSSH   ), 
RENAME_CS as ( SELECT 
		  CASE_NO                                            as                                            CASE_NO
		, CASE_ID                                            as                                         CS_CASE_ID 
				FROM     LOGIC_CS   ), 
RENAME_STT as ( SELECT 
		  CASE_STT_TYP_NM                                    as                                    CASE_STT_TYP_NM
		, CASE_STT_TYP_CD                                    as                                STT_CASE_STT_TYP_CD
		, VOID_IND                                           as                                       STT_VOID_IND 
				FROM     LOGIC_STT   ), 
RENAME_STS as ( SELECT 
		  CASE_STS_TYP_NM                                    as                                    CASE_STS_TYP_NM
		, CASE_STS_TYP_CD                                    as                                STS_CASE_STS_TYP_CD
		, VOID_IND                                           as                                       STS_VOID_IND 
				FROM     LOGIC_STS   ), 
RENAME_RSN as ( SELECT 
		  CASE_STS_RSN_TYP_NM                                as                                CASE_STS_RSN_TYP_NM
		, CASE_STS_RSN_TYP_CD                                as                            RSN_CASE_STS_RSN_TYP_CD
		, VOID_IND                                           as                                       RSN_VOID_IND 
				FROM     LOGIC_RSN   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CSSH                           as ( SELECT * from    RENAME_CSSH   ),
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_STT                            as ( SELECT * from    RENAME_STT 
				WHERE STT_VOID_IND = 'N'  ),
FILTER_STS                            as ( SELECT * from    RENAME_STS 
				WHERE STS_VOID_IND = 'N'  ),
FILTER_RSN                            as ( SELECT * from    RENAME_RSN 
				WHERE RSN_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CSSH as ( SELECT * 
				FROM  FILTER_CSSH
				INNER JOIN FILTER_CS ON  FILTER_CSSH.CASE_ID =  FILTER_CS.CS_CASE_ID 
						LEFT JOIN FILTER_STT ON  FILTER_CSSH.CASE_STT_TYP_CD =  FILTER_STT.STT_CASE_STT_TYP_CD 
						LEFT JOIN FILTER_STS ON  FILTER_CSSH.CASE_STS_TYP_CD =  FILTER_STS.STS_CASE_STS_TYP_CD 
						LEFT JOIN FILTER_RSN ON  FILTER_CSSH.CASE_STS_RSN_TYP_CD =  FILTER_RSN.RSN_CASE_STS_RSN_TYP_CD  )
SELECT * 
from CSSH
      );
    