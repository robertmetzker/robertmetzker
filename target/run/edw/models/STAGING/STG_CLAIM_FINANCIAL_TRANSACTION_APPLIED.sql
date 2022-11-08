

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_FINANCIAL_TRANSACTION_APPLIED  as
      (---- SRC LAYER ----
WITH
SRC_CFTA as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_FINANCIAL_TRAN_APP ),
SRC_FTAT as ( SELECT *     from     DEV_VIEWS.PCMP.FINANCIAL_TRANSACTION_APP_TYP ),
//SRC_CFTA as ( SELECT *     from     CLAIM_FINANCIAL_TRAN_APP) ,
//SRC_FTAT as ( SELECT *     from     FINANCIAL_TRANSACTION_APP_TYP) ,

---- LOGIC LAYER ----

LOGIC_CFTA as ( SELECT 
		  CFTA_ID                                            AS                                            CFTA_ID 
		, FTAT_ID                                            AS                                            FTAT_ID 
		, CFTA_AMT                                           AS                                           CFTA_AMT 
		, cast( CFTA_DT as DATE )                            AS                                            CFTA_DT 
		, CFT_ID_APLD_FR                                     AS                                     CFT_ID_APLD_FR 
		, CFT_ID_APLD_TO                                     AS                                     CFT_ID_APLD_TO 
		, CFT_ID_RVRS                                        AS                                        CFT_ID_RVRS 
		, cast( CFTA_SENT_DT as DATE )                       AS                                       CFTA_SENT_DT 
		, upper( CFTA_GNRL_LDGR_SENT_IND )                   AS                            CFTA_GNRL_LDGR_SENT_IND 
		, upper( CFTA_ORIG_APP_IND )                         AS                                  CFTA_ORIG_APP_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		from SRC_CFTA
            ),
LOGIC_FTAT as ( SELECT 
		  upper( TRIM( AGRE_TYP_CD ) )                       AS                                        AGRE_TYP_CD 
		, upper( TRIM( FTAT_CD ) )                           AS                                            FTAT_CD 
		, upper( TRIM( FTAT_NM ) )                           AS                                            FTAT_NM 
		, upper( FTAT_GNRL_LDGR_IND )                        AS                                 FTAT_GNRL_LDGR_IND 
		, upper( FTAT_MNL_UNAPLD_IND )                       AS                                FTAT_MNL_UNAPLD_IND 
		, FTAT_ID                                            AS                                            FTAT_ID 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_FTAT
            )

---- RENAME LAYER ----
,

RENAME_CFTA as ( SELECT 
		  CFTA_ID                                            as                                            CFTA_ID
		, FTAT_ID                                            as                                            FTAT_ID
		, CFTA_AMT                                           as                                           CFTA_AMT
		, CFTA_DT                                            as                                            CFTA_DT
		, CFT_ID_APLD_FR                                     as                                     CFT_ID_APLD_FR
		, CFT_ID_APLD_TO                                     as                                     CFT_ID_APLD_TO
		, CFT_ID_RVRS                                        as                                        CFT_ID_RVRS
		, CFTA_SENT_DT                                       as                                       CFTA_SENT_DT
		, CFTA_GNRL_LDGR_SENT_IND                            as                            CFTA_GNRL_LDGR_SENT_IND
		, CFTA_ORIG_APP_IND                                  as                                  CFTA_ORIG_APP_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_CFTA   ), 
RENAME_FTAT as ( SELECT 
		  AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, FTAT_CD                                            as                                            FTAT_CD
		, FTAT_NM                                            as                                            FTAT_NM
		, FTAT_GNRL_LDGR_IND                                 as                                 FTAT_GNRL_LDGR_IND
		, FTAT_MNL_UNAPLD_IND                                as                                FTAT_MNL_UNAPLD_IND
		, FTAT_ID                                            as                                       FTAT_FTAT_ID
		, VOID_IND                                           as                                      FTAT_VOID_IND 
				FROM     LOGIC_FTAT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CFTA                           as ( SELECT * from    RENAME_CFTA   ),
FILTER_FTAT                           as ( SELECT * from    RENAME_FTAT 
				WHERE FTAT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CFTA as ( SELECT * 
				FROM  FILTER_CFTA
				LEFT JOIN FILTER_FTAT ON  FILTER_CFTA.FTAT_ID =  FILTER_FTAT.FTAT_FTAT_ID  )
SELECT * 
from CFTA
      );
    