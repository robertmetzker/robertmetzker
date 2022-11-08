---- SRC LAYER ----
WITH
SRC_BSD as ( SELECT *     from     DEV_VIEWS.PCMP.BILLING_SCHEDULE_DETAIL ),
SRC_BBSD as ( SELECT *     from     DEV_VIEWS.PCMP.BWC_BILLING_SCHEDULE_DETAIL ),
//SRC_BSD as ( SELECT *     from     BILLING_SCHEDULE_DETAIL) ,
//SRC_BBSD as ( SELECT *     from     BWC_BILLING_SCHEDULE_DETAIL) ,

---- LOGIC LAYER ----

LOGIC_BSD as ( SELECT 
		  BILL_SCH_DTL_ID                                    AS                                    BILL_SCH_DTL_ID 
		, PLCY_PRD_BILL_SCH_ID                               AS                               PLCY_PRD_BILL_SCH_ID 
		, BILL_SCH_DTL_NO                                    AS                                    BILL_SCH_DTL_NO 
		, cast( BILL_SCH_DTL_BILL_DT as DATE )               AS                               BILL_SCH_DTL_BILL_DT 
		, BILL_SCH_DTL_DRV_AMT                               AS                               BILL_SCH_DTL_DRV_AMT 
		, BILL_SCH_DTL_DRV_ADJ_AMT                           AS                           BILL_SCH_DTL_DRV_ADJ_AMT 
		, upper( BILL_SCH_DTL_ADV_BTCH_IND )                 AS                          BILL_SCH_DTL_ADV_BTCH_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BSD
            ),
LOGIC_BBSD as ( SELECT 
		  cast( BILL_SCH_DTL_LPS_EFF_DT as DATE )            AS                            BILL_SCH_DTL_LPS_EFF_DT 
		, BILL_SCH_DTL_ID                                    AS                                    BILL_SCH_DTL_ID 
		from SRC_BBSD
            )

---- RENAME LAYER ----
,

RENAME_BSD as ( SELECT 
		  BILL_SCH_DTL_ID                                    as                                    BILL_SCH_DTL_ID
		, PLCY_PRD_BILL_SCH_ID                               as                               PLCY_PRD_BILL_SCH_ID
		, BILL_SCH_DTL_NO                                    as                                    BILL_SCH_DTL_NO
		, BILL_SCH_DTL_BILL_DT                               as                               BILL_SCH_DTL_BILL_DT
		, BILL_SCH_DTL_DRV_AMT                               as                               BILL_SCH_DTL_DRV_AMT
		, BILL_SCH_DTL_DRV_ADJ_AMT                           as                           BILL_SCH_DTL_DRV_ADJ_AMT
		, BILL_SCH_DTL_ADV_BTCH_IND                          as                          BILL_SCH_DTL_ADV_BTCH_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                       BSD_VOID_IND 
				FROM     LOGIC_BSD   ), 
RENAME_BBSD as ( SELECT 
		  BILL_SCH_DTL_LPS_EFF_DT                            as                            BILL_SCH_DTL_LPS_EFF_DT
		, BILL_SCH_DTL_ID                                    as                               BBSD_BILL_SCH_DTL_ID 
				FROM     LOGIC_BBSD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_BSD                            as ( SELECT * from    RENAME_BSD 
				WHERE BSD_VOID_IND = 'N'  ),
FILTER_BBSD                           as ( SELECT * from    RENAME_BBSD   ),

---- JOIN LAYER ----

BSD as ( SELECT * 
				FROM  FILTER_BSD
				LEFT JOIN FILTER_BBSD ON  FILTER_BSD.BILL_SCH_DTL_ID =  FILTER_BBSD.BBSD_BILL_SCH_DTL_ID  )
SELECT * 
from BSD