

      create or replace  table DEV_EDW.STAGING.STG_CASE_DETAIL_VOCATIONAL_REHAB  as
      (---- SRC LAYER ----
WITH
SRC_CDVR as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_DETAIL_VOCATIONAL_REHAB ),
//SRC_CDVR as ( SELECT *     from     CASE_DETAIL_VOCATIONAL_REHAB) ,

---- LOGIC LAYER ----

LOGIC_CDVR as ( SELECT 
		  CDVR_ID                                            AS                                            CDVR_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, cast( CDVR_INT_INTVW_DTM as DATE )                 AS                                 CDVR_INT_INTVW_DTM 
		, upper( CDVR_JOB_PLC_SERV_IND )                     AS                              CDVR_JOB_PLC_SERV_IND 
		, cast( CDVR_EMPL_OBTN_DTM as DATE )                 AS                                 CDVR_EMPL_OBTN_DTM 
		, cast( CDVR_RTN_TO_WK_DTM as DATE )                 AS                                 CDVR_RTN_TO_WK_DTM 
		, upper( TRIM( CDVR_TYP_OF_EMPL_OBTN_DESC ) )        AS                         CDVR_TYP_OF_EMPL_OBTN_DESC 
		, upper( TRIM( CDVR_WGS_FRM_EMPL_OBTN_DESC ) )       AS                        CDVR_WGS_FRM_EMPL_OBTN_DESC 
		, upper( CDVR_REG_COST_SH_IND )                      AS                               CDVR_REG_COST_SH_IND 
		, cast( CDVR_LMC_COST_ANL_DTM as DATE )              AS                              CDVR_LMC_COST_ANL_DTM 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CDVR
            )

---- RENAME LAYER ----
,

RENAME_CDVR as ( SELECT 
		  CDVR_ID                                            as                                            CDVR_ID
		, CASE_ID                                            as                                            CASE_ID
		, CDVR_INT_INTVW_DTM                                 as                                 CDVR_INT_INTVW_DTM
		, CDVR_JOB_PLC_SERV_IND                              as                              CDVR_JOB_PLC_SERV_IND
		, CDVR_EMPL_OBTN_DTM                                 as                                 CDVR_EMPL_OBTN_DTM
		, CDVR_RTN_TO_WK_DTM                                 as                                 CDVR_RTN_TO_WK_DTM
		, CDVR_TYP_OF_EMPL_OBTN_DESC                         as                         CDVR_TYP_OF_EMPL_OBTN_DESC
		, CDVR_WGS_FRM_EMPL_OBTN_DESC                        as                        CDVR_WGS_FRM_EMPL_OBTN_DESC
		, CDVR_REG_COST_SH_IND                               as                               CDVR_REG_COST_SH_IND
		, CDVR_LMC_COST_ANL_DTM                              as                              CDVR_LMC_COST_ANL_DTM
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CDVR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDVR                           as ( SELECT * from    RENAME_CDVR   ),

---- JOIN LAYER ----

 JOIN_CDVR  as  ( SELECT * 
				FROM  FILTER_CDVR )
 SELECT * FROM  JOIN_CDVR
      );
    