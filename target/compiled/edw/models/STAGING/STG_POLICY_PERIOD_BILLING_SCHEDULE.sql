---- SRC LAYER ----
WITH
SRC_PPBS as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD_BILLING_SCHEDULE ),
SRC_BPPBS as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_PLCY_PRD_BILLING_SCH ),
//SRC_PPBS as ( SELECT *     from     POLICY_PERIOD_BILLING_SCHEDULE) ,
//SRC_BPPBS as ( SELECT *     from     BWC_PLCY_PRD_BILLING_SCH) ,

---- LOGIC LAYER ----

LOGIC_PPBS as ( SELECT 
		  PLCY_PRD_BILL_SCH_ID                               AS                               PLCY_PRD_BILL_SCH_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, PLCY_PRD_BILL_SCH_DRV_SCH_AMT                      AS                      PLCY_PRD_BILL_SCH_DRV_SCH_AMT 
		, PLCY_PRD_BILL_SCH_DRV_ADJ_AMT                      AS                      PLCY_PRD_BILL_SCH_DRV_ADJ_AMT 
		, PLCY_PRD_BILL_SCH_DRV_DFR_AMT                      AS                      PLCY_PRD_BILL_SCH_DRV_DFR_AMT 
		, PLCY_PRD_BILL_SCH_ASMT_AMT                         AS                         PLCY_PRD_BILL_SCH_ASMT_AMT 
		, upper( PLCY_PRD_BILL_SCH_CSTM_IND )                AS                         PLCY_PRD_BILL_SCH_CSTM_IND 
		, upper( PLCY_PRD_BILL_SCH_NOTE_IND )                AS                         PLCY_PRD_BILL_SCH_NOTE_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PPBS
            ),
LOGIC_BPPBS as ( SELECT 
		  upper( PLCY_PRD_BILL_SCH_TRU_UP_IND )              AS                       PLCY_PRD_BILL_SCH_TRU_UP_IND 
		, PLCY_PRD_BILL_SCH_ID                               AS                               PLCY_PRD_BILL_SCH_ID 
		from SRC_BPPBS
            )

---- RENAME LAYER ----
,

RENAME_PPBS as ( SELECT 
		  PLCY_PRD_BILL_SCH_ID                               as                               PLCY_PRD_BILL_SCH_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_PRD_BILL_SCH_DRV_SCH_AMT                      as                      PLCY_PRD_BILL_SCH_DRV_SCH_AMT
		, PLCY_PRD_BILL_SCH_DRV_ADJ_AMT                      as                      PLCY_PRD_BILL_SCH_DRV_ADJ_AMT
		, PLCY_PRD_BILL_SCH_DRV_DFR_AMT                      as                      PLCY_PRD_BILL_SCH_DRV_DFR_AMT
		, PLCY_PRD_BILL_SCH_ASMT_AMT                         as                         PLCY_PRD_BILL_SCH_ASMT_AMT
		, PLCY_PRD_BILL_SCH_CSTM_IND                         as                         PLCY_PRD_BILL_SCH_CSTM_IND
		, PLCY_PRD_BILL_SCH_NOTE_IND                         as                         PLCY_PRD_BILL_SCH_NOTE_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                      PPBS_VOID_IND 
				FROM     LOGIC_PPBS   ), 
RENAME_BPPBS as ( SELECT 
		  PLCY_PRD_BILL_SCH_TRU_UP_IND                       as                       PLCY_PRD_BILL_SCH_TRU_UP_IND
		, PLCY_PRD_BILL_SCH_ID                               as                         BPPBS_PLCY_PRD_BILL_SCH_ID 
				FROM     LOGIC_BPPBS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPBS                           as ( SELECT * from    RENAME_PPBS 
				WHERE PPBS_VOID_IND = 'N'  ),
FILTER_BPPBS                          as ( SELECT * from    RENAME_BPPBS   ),

---- JOIN LAYER ----

PPBS as ( SELECT * 
				FROM  FILTER_PPBS
				LEFT JOIN FILTER_BPPBS ON  FILTER_PPBS.PLCY_PRD_BILL_SCH_ID =  FILTER_BPPBS.BPPBS_PLCY_PRD_BILL_SCH_ID  )
SELECT * 
from PPBS