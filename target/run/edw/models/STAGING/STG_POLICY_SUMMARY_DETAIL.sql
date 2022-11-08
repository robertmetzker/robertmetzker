

      create or replace  table DEV_EDW.STAGING.STG_POLICY_SUMMARY_DETAIL  as
      (---- SRC LAYER ----
WITH
SRC_PSD            as ( SELECT *     FROM      DEV_VIEWS.PCMP.POLICY_SUMMARY_DETAIL ),
//SRC_PSD            as ( SELECT *     FROM     POLICY_SUMMARY_DETAIL) ,

---- LOGIC LAYER ----


LOGIC_PSD as ( SELECT 
		  PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID 
		, PLCY_SUM_DTL_CUST_ID_ACCT_HLDR                     as                     PLCY_SUM_DTL_CUST_ID_ACCT_HLDR 
		, upper( TRIM( LOB_TYP_CD ) )                        as                                         LOB_TYP_CD 
		, upper( TRIM( PLCY_SUM_DTL_PLCY_NO ) )              as                               PLCY_SUM_DTL_PLCY_NO 
		, cast( PLCY_SUM_DTL_EFF_DT as DATE )                as                                PLCY_SUM_DTL_EFF_DT 
		, cast( PLCY_SUM_DTL_END_DT as DATE )                as                                PLCY_SUM_DTL_END_DT 
		, upper( TRIM( PLCY_SUM_DTL_PLCY_TYP_CD ) )          as                           PLCY_SUM_DTL_PLCY_TYP_CD 
		, upper( TRIM( PLCY_SUM_DTL_DEPT_NO ) )              as                               PLCY_SUM_DTL_DEPT_NO 
		, upper( TRIM( PLCY_SUM_DTL_CUST_EXTRNL_NO ) )       as                        PLCY_SUM_DTL_CUST_EXTRNL_NO 
		, upper( TRIM( MRKT_TYP_CD ) )                       as                                        MRKT_TYP_CD 
		, INS_CMPY_TYP_ID                                    as                                    INS_CMPY_TYP_ID 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		, upper( TRIM( PLCY_SUM_DTL_PLCY_STS ) )             as                              PLCY_SUM_DTL_PLCY_STS 
		, upper( TRIM( PLCY_SUM_DTL_CUST_AGY_NO ) )          as                           PLCY_SUM_DTL_CUST_AGY_NO 
		FROM SRC_PSD
            )

---- RENAME LAYER ----
,

RENAME_PSD        as ( SELECT 
		  PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID
		, PLCY_SUM_DTL_CUST_ID_ACCT_HLDR                     as                     PLCY_SUM_DTL_CUST_ID_ACCT_HLDR
		, LOB_TYP_CD                                         as                                         LOB_TYP_CD
		, PLCY_SUM_DTL_PLCY_NO                               as                               PLCY_SUM_DTL_PLCY_NO
		, PLCY_SUM_DTL_EFF_DT                                as                              PLCY_SUM_DTL_EFF_DATE
		, PLCY_SUM_DTL_END_DT                                as                              PLCY_SUM_DTL_END_DATE
		, PLCY_SUM_DTL_PLCY_TYP_CD                           as                           PLCY_SUM_DTL_PLCY_TYP_CD
		, PLCY_SUM_DTL_DEPT_NO                               as                               PLCY_SUM_DTL_DEPT_NO
		, PLCY_SUM_DTL_CUST_EXTRNL_NO                        as                        PLCY_SUM_DTL_CUST_EXTRNL_NO
		, MRKT_TYP_CD                                        as                                        MRKT_TYP_CD
		, INS_CMPY_TYP_ID                                    as                                    INS_CMPY_TYP_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, PLCY_SUM_DTL_PLCY_STS                              as                              PLCY_SUM_DTL_PLCY_STS
		, PLCY_SUM_DTL_CUST_AGY_NO                           as                           PLCY_SUM_DTL_CUST_AGY_NO 
				FROM     LOGIC_PSD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PSD                            as ( SELECT * FROM    RENAME_PSD   ),

---- JOIN LAYER ----

 JOIN_PSD         as  ( SELECT * 
				FROM  FILTER_PSD )
 SELECT * FROM  JOIN_PSD
      );
    