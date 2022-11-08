

      create or replace  table DEV_EDW.STAGING.STG_CASE_DETAIL_FILE_REVIEW  as
      (---- SRC LAYER ----
WITH
SRC_CDFR as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_FILE_RVW ),
SRC_CDART as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_ADNDM_REQS_TYPE ),
//SRC_CDFR as ( SELECT *     from     CASE_DETAIL_FILE_RVW) ,
//SRC_CDART as ( SELECT *     from     CASE_DETAIL_ADNDM_REQS_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CDFR as ( SELECT 
		  CDFR_ID                                            AS                                            CDFR_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, cast( CDFR_RVW_RPT_RECV_DT as DATE )               AS                               CDFR_RVW_RPT_RECV_DT 
		, CDFR_PHYS_IMPR_RT                                  AS                                  CDFR_PHYS_IMPR_RT 
		, CDFR_FNL_IMPR_RT                                   AS                                   CDFR_FNL_IMPR_RT 
		, upper( CDFR_ADNDM_REQS_IND )                       AS                                CDFR_ADNDM_REQS_IND 
		, upper( CDFR_EXM_REQS_IND )                         AS                                  CDFR_EXM_REQS_IND 
		, upper( TRIM( CD_ADNDM_REQS_TYP_CD ) )              AS                               CD_ADNDM_REQS_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDFR
            ),
LOGIC_CDART as ( SELECT 
		  upper( TRIM( CD_ADNDM_REQS_TYP_NM ) )              AS                               CD_ADNDM_REQS_TYP_NM 
		, upper( TRIM( CD_ADNDM_REQS_TYP_CD ) )              AS                               CD_ADNDM_REQS_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDART
            )

---- RENAME LAYER ----
,

RENAME_CDFR as ( SELECT 
		  CDFR_ID                                            as                                            CDFR_ID
		, CASE_ID                                            as                                            CASE_ID
		, CDFR_RVW_RPT_RECV_DT                               as                               CDFR_RVW_RPT_RECV_DT
		, CDFR_PHYS_IMPR_RT                                  as                                  CDFR_PHYS_IMPR_RT
		, CDFR_FNL_IMPR_RT                                   as                                   CDFR_FNL_IMPR_RT
		, CDFR_ADNDM_REQS_IND                                as                                CDFR_ADNDM_REQS_IND
		, CDFR_EXM_REQS_IND                                  as                                  CDFR_EXM_REQS_IND
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CDFR   ), 
RENAME_CDART as ( SELECT 
		  CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM
		, CD_ADNDM_REQS_TYP_CD                               as                         CDART_CD_ADNDM_REQS_TYP_CD
		, VOID_IND                                           as                                     CDART_VOID_IND 
				FROM     LOGIC_CDART   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDFR                           as ( SELECT * from    RENAME_CDFR   ),
FILTER_CDART                          as ( SELECT * from    RENAME_CDART 
				WHERE CDART_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CDFR as ( SELECT * 
				FROM  FILTER_CDFR
				LEFT JOIN FILTER_CDART ON  FILTER_CDFR.CD_ADNDM_REQS_TYP_CD =  FILTER_CDART.CDART_CD_ADNDM_REQS_TYP_CD  )
SELECT * 
from CDFR
      );
    