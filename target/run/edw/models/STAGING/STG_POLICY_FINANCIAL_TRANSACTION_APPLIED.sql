

      create or replace  table DEV_EDW.STAGING.STG_POLICY_FINANCIAL_TRANSACTION_APPLIED  as
      (---- SRC LAYER ----
WITH
SRC_PFTA as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_FINANCIAL_TRAN_APP ),
SRC_FTAT as ( SELECT *     from     DEV_VIEWS.PCMP.FINANCIAL_TRANSACTION_APP_TYP ),
//SRC_PFTA as ( SELECT *     from     POLICY_FINANCIAL_TRAN_APP) ,
//SRC_FTAT as ( SELECT *     from     FINANCIAL_TRANSACTION_APP_TYP) ,

---- LOGIC LAYER ----

LOGIC_PFTA as ( SELECT 
		  PFTA_ID                                            AS                                            PFTA_ID 
		, FTAT_ID                                            AS                                            FTAT_ID 
		, PFTA_AMT                                           AS                                           PFTA_AMT 
		, cast( PFTA_DT as DATE )                            AS                                            PFTA_DT 
		, PFT_ID_APLD_FR                                     AS                                     PFT_ID_APLD_FR 
		, PFT_ID_APLD_TO                                     AS                                     PFT_ID_APLD_TO 
		, PFT_ID_RVRS                                        AS                                        PFT_ID_RVRS 
		, cast( PFTA_SENT_DT as DATE )                       AS                                       PFTA_SENT_DT 
		, upper( PFTA_GNRL_LDGR_SENT_IND )                   AS                            PFTA_GNRL_LDGR_SENT_IND 
		, upper( PFTA_ACCT_PYBL_SENT_IND )                   AS                            PFTA_ACCT_PYBL_SENT_IND 
		, upper( PFTA_COMS_FR_IND )                          AS                                   PFTA_COMS_FR_IND 
		, upper( PFTA_COMS_TO_IND )                          AS                                   PFTA_COMS_TO_IND 
		, upper( PFTA_ORIG_APP_IND )                         AS                                  PFTA_ORIG_APP_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		from SRC_PFTA
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

RENAME_PFTA as ( SELECT 
		  PFTA_ID                                            as                                            PFTA_ID
		, FTAT_ID                                            as                                            FTAT_ID
		, PFTA_AMT                                           as                                           PFTA_AMT
		, PFTA_DT                                            as                                            PFTA_DT
		, PFT_ID_APLD_FR                                     as                                     PFT_ID_APLD_FR
		, PFT_ID_APLD_TO                                     as                                     PFT_ID_APLD_TO
		, PFT_ID_RVRS                                        as                                        PFT_ID_RVRS
		, PFTA_SENT_DT                                       as                                       PFTA_SENT_DT
		, PFTA_GNRL_LDGR_SENT_IND                            as                            PFTA_GNRL_LDGR_SENT_IND
		, PFTA_ACCT_PYBL_SENT_IND                            as                            PFTA_ACCT_PYBL_SENT_IND
		, PFTA_COMS_FR_IND                                   as                                   PFTA_COMS_FR_IND
		, PFTA_COMS_TO_IND                                   as                                   PFTA_COMS_TO_IND
		, PFTA_ORIG_APP_IND                                  as                                  PFTA_ORIG_APP_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_PFTA   ), 
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
FILTER_PFTA                           as ( SELECT * from    RENAME_PFTA   ),
FILTER_FTAT                           as ( SELECT * from    RENAME_FTAT 
				WHERE FTAT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PFTA as ( SELECT * 
				FROM  FILTER_PFTA
				LEFT JOIN FILTER_FTAT ON  FILTER_PFTA.FTAT_ID =  FILTER_FTAT.FTAT_FTAT_ID  )
SELECT * 
from PFTA
      );
    