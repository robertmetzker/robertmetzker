

      create or replace  table DEV_EDW.STAGING.STG_POLICY_INVOICE  as
      (---- SRC LAYER ----
WITH
SRC_PINV as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_INVOICE ),
SRC_PIT as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_INVOICE_TYPE ),
SRC_BPI as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_POLICY_INVOICE ),
//SRC_PINV as ( SELECT *     from     POLICY_INVOICE) ,
//SRC_PIT as ( SELECT *     from     POLICY_INVOICE_TYPE) ,
//SRC_BPI as ( SELECT *     from     BWC_POLICY_INVOICE) ,

---- LOGIC LAYER ----

LOGIC_PINV as ( SELECT 
		  PLCY_INVC_ID                                       AS                                       PLCY_INVC_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( PLCY_INVC_TYP_CD ) )                  AS                                   PLCY_INVC_TYP_CD 
		, cast( PLCY_INVC_ISS_DT as DATE )                   AS                                   PLCY_INVC_ISS_DT 
		, CUST_ID_PYR                                        AS                                        CUST_ID_PYR 
		, PLCY_INVC_DRV_BAL_AMT                              AS                              PLCY_INVC_DRV_BAL_AMT 
		, PLCY_INVC_DTL_DRV_BAL_AMT                          AS                          PLCY_INVC_DTL_DRV_BAL_AMT 
		, cast( PLCY_INVC_RUN_DT as DATE )                   AS                                   PLCY_INVC_RUN_DT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PINV
            ),
LOGIC_PIT as ( SELECT 
		  upper( TRIM( PLCY_INVC_TYP_NM ) )                  AS                                   PLCY_INVC_TYP_NM 
		, upper( TRIM( PLCY_INVC_TYP_CD ) )                  AS                                   PLCY_INVC_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PIT
            ),
LOGIC_BPI as ( SELECT 
		  PLCY_INVC_CERT_AG_BAL_AMT                          AS                          PLCY_INVC_CERT_AG_BAL_AMT 
		, PLCY_INVC_DSP_BAL_AMT                              AS                              PLCY_INVC_DSP_BAL_AMT 
		, PLCY_INVC_ID                                       AS                                       PLCY_INVC_ID 
		from SRC_BPI
            )

---- RENAME LAYER ----
,

RENAME_PINV as ( SELECT 
		  PLCY_INVC_ID                                       as                                       PLCY_INVC_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, PLCY_INVC_TYP_CD                                   as                                   PLCY_INVC_TYP_CD
		, PLCY_INVC_ISS_DT                                   as                                   PLCY_INVC_ISS_DT
		, CUST_ID_PYR                                        as                                        CUST_ID_PYR
		, PLCY_INVC_DRV_BAL_AMT                              as                              PLCY_INVC_DRV_BAL_AMT
		, PLCY_INVC_DTL_DRV_BAL_AMT                          as                          PLCY_INVC_DTL_DRV_BAL_AMT
		, PLCY_INVC_RUN_DT                                   as                                   PLCY_INVC_RUN_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PINV   ), 
RENAME_PIT as ( SELECT 
		  PLCY_INVC_TYP_NM                                   as                                   PLCY_INVC_TYP_NM
		, PLCY_INVC_TYP_CD                                   as                               PIT_PLCY_INVC_TYP_CD
		, VOID_IND                                           as                                       PIT_VOID_IND 
				FROM     LOGIC_PIT   ), 
RENAME_BPI as ( SELECT 
		  PLCY_INVC_CERT_AG_BAL_AMT                          as                          PLCY_INVC_CERT_AG_BAL_AMT
		, PLCY_INVC_DSP_BAL_AMT                              as                              PLCY_INVC_DSP_BAL_AMT
		, PLCY_INVC_ID                                       as                                   BPI_PLCY_INVC_ID 
				FROM     LOGIC_BPI   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PINV                           as ( SELECT * from    RENAME_PINV   ),
FILTER_BPI                            as ( SELECT * from    RENAME_BPI   ),
FILTER_PIT                            as ( SELECT * from    RENAME_PIT 
				WHERE PIT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PINV as ( SELECT * 
				FROM  FILTER_PINV
				LEFT JOIN FILTER_BPI ON  FILTER_PINV.PLCY_INVC_ID =  FILTER_BPI.BPI_PLCY_INVC_ID 
								LEFT JOIN FILTER_PIT ON  FILTER_PINV.PLCY_INVC_TYP_CD =  FILTER_PIT.PIT_PLCY_INVC_TYP_CD  )
SELECT * 
from PINV
      );
    