---- SRC LAYER ----
WITH
SRC_CFTPR as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_FINANCIAL_TRAN_PAY_REQS ),
//SRC_CFTPR as ( SELECT *     from     CLAIM_FINANCIAL_TRAN_PAY_REQS) ,

---- LOGIC LAYER ----


LOGIC_CFTPR as ( SELECT 
		  CLM_FNCL_TRAN_PAY_REQS_ID                          as                          CLM_FNCL_TRAN_PAY_REQS_ID 
		, CFT_ID                                             as                                             CFT_ID 
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID 
		, CLM_FNCL_TRAN_PAY_REQS_BTCH_ID                     as                     CLM_FNCL_TRAN_PAY_REQS_BTCH_ID 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CFTPR
            )

---- RENAME LAYER ----
,

RENAME_CFTPR as ( SELECT 
		  CLM_FNCL_TRAN_PAY_REQS_ID                          as                          CLM_FNCL_TRAN_PAY_REQS_ID
		, CFT_ID                                             as                                             CFT_ID
		, PAY_REQS_ID                                        as                                        PAY_REQS_ID
		, CLM_FNCL_TRAN_PAY_REQS_BTCH_ID                     as                     CLM_FNCL_TRAN_PAY_REQS_BTCH_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CFTPR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CFTPR                          as ( SELECT * from    RENAME_CFTPR   ),

---- JOIN LAYER ----

 JOIN_CFTPR  as  ( SELECT * 
				FROM  FILTER_CFTPR )
 SELECT * FROM  JOIN_CFTPR