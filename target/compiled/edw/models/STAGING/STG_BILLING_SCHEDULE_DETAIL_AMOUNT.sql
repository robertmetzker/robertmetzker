---- SRC LAYER ----
WITH
SRC_BSDA as ( SELECT *     from     DEV_VIEWS.PCMP.BILLING_SCHEDULE_DETAIL_AMOUNT ),
SRC_SAT as ( SELECT *     from     DEV_VIEWS.PCMP.SCHEDULE_AMOUNT_TYPE ),
SRC_SAFTX as ( SELECT *     from     DEV_VIEWS.PCMP.SCHEDULE_AMOUNT_FNCL_TRAN_XREF ),
SRC_FTT as ( SELECT *     from     DEV_VIEWS.PCMP.FINANCIAL_TRANSACTION_TYPE ),
//SRC_BSDA as ( SELECT *     from     BILLING_SCHEDULE_DETAIL_AMOUNT) ,
//SRC_SAT as ( SELECT *     from     SCHEDULE_AMOUNT_TYPE) ,
//SRC_SAFTX as ( SELECT *     from     SCHEDULE_AMOUNT_FNCL_TRAN_XREF) ,
//SRC_FTT as ( SELECT *     from     FINANCIAL_TRANSACTION_TYPE) ,

---- LOGIC LAYER ----

LOGIC_BSDA as ( SELECT 
		  BILL_SCH_DTL_AMT_ID                                AS                                BILL_SCH_DTL_AMT_ID 
		, BILL_SCH_DTL_ID                                    AS                                    BILL_SCH_DTL_ID 
		, BILL_SCH_DTL_AMT_NO                                AS                                BILL_SCH_DTL_AMT_NO 
		, upper( TRIM( SCH_AMT_TYP_CD ) )                    AS                                     SCH_AMT_TYP_CD 
		, BILL_SCH_DTL_AMT_AMT                               AS                               BILL_SCH_DTL_AMT_AMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_BSDA
            ),
LOGIC_SAT as ( SELECT 
		  upper( TRIM( SCH_AMT_TYP_NM ) )                    AS                                     SCH_AMT_TYP_NM 
		, upper( TRIM( SCH_AMT_TYP_CD ) )                    AS                                     SCH_AMT_TYP_CD 
		, upper( SCH_AMT_TYP_VOID_IND )                      AS                               SCH_AMT_TYP_VOID_IND 
		from SRC_SAT
            ),
LOGIC_SAFTX as ( SELECT 
		  upper( TRIM( FNCL_TRAN_TYP_CD ) )                  AS                                   FNCL_TRAN_TYP_CD 
		, upper( TRIM( SCH_AMT_TYP_CD ) )                    AS                                     SCH_AMT_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_SAFTX
            ),
LOGIC_FTT as ( SELECT 
		  upper( TRIM( FNCL_TRAN_TYP_NM ) )                  AS                                   FNCL_TRAN_TYP_NM 
		, upper( TRIM( FNCL_TRAN_TYP_CD ) )                  AS                                   FNCL_TRAN_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_FTT
            )

---- RENAME LAYER ----
,

RENAME_BSDA as ( SELECT 
		  BILL_SCH_DTL_AMT_ID                                as                                BILL_SCH_DTL_AMT_ID
		, BILL_SCH_DTL_ID                                    as                                    BILL_SCH_DTL_ID
		, BILL_SCH_DTL_AMT_NO                                as                                BILL_SCH_DTL_AMT_NO
		, SCH_AMT_TYP_CD                                     as                                     SCH_AMT_TYP_CD
		, BILL_SCH_DTL_AMT_AMT                               as                               BILL_SCH_DTL_AMT_AMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_BSDA   ), 
RENAME_SAT as ( SELECT 
		  SCH_AMT_TYP_NM                                     as                                     SCH_AMT_TYP_NM
		, SCH_AMT_TYP_CD                                     as                                 SAT_SCH_AMT_TYP_CD
		, SCH_AMT_TYP_VOID_IND                               as                               SCH_AMT_TYP_VOID_IND 
				FROM     LOGIC_SAT   ), 
RENAME_SAFTX as ( SELECT 
		  FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD
		, SCH_AMT_TYP_CD                                     as                               SAFTX_SCH_AMT_TYP_CD
		, VOID_IND                                           as                                     SAFTX_VOID_IND 
				FROM     LOGIC_SAFTX   ), 
RENAME_FTT as ( SELECT 
		  FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
		, FNCL_TRAN_TYP_CD                                   as                               FTT_FNCL_TRAN_TYP_CD
		, VOID_IND                                           as                                       FTT_VOID_IND 
				FROM     LOGIC_FTT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_BSDA                           as ( SELECT * from    RENAME_BSDA   ),
FILTER_SAT                            as ( SELECT * from    RENAME_SAT 
				WHERE SCH_AMT_TYP_VOID_IND = 'N'  ),
FILTER_SAFTX                          as ( SELECT * from    RENAME_SAFTX 
				WHERE SAFTX_VOID_IND = 'N'  ),
FILTER_FTT                            as ( SELECT * from    RENAME_FTT 
				WHERE FTT_VOID_IND = 'N'  ),

---- JOIN LAYER ----

SAFTX as ( SELECT * 
				FROM  FILTER_SAFTX
				LEFT JOIN FILTER_FTT ON  FILTER_SAFTX.FNCL_TRAN_TYP_CD =  FILTER_FTT.FTT_FNCL_TRAN_TYP_CD  ),
BSDA as ( SELECT * 
				FROM  FILTER_BSDA
				LEFT JOIN FILTER_SAT ON  FILTER_BSDA.SCH_AMT_TYP_CD =  FILTER_SAT.SAT_SCH_AMT_TYP_CD 
						LEFT JOIN SAFTX ON  FILTER_BSDA.SCH_AMT_TYP_CD = SAFTX.SAFTX_SCH_AMT_TYP_CD  )
SELECT * 
from BSDA