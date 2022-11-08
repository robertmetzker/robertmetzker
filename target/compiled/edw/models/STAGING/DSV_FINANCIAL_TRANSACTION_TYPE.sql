

---- SRC LAYER ----
WITH
SRC_FTT as ( SELECT *     from     STAGING.DST_FINANCIAL_TRANSACTION_TYPE ),
//SRC_FTT as ( SELECT *     from     DST_FINANCIAL_TRANSACTION_TYPE) ,

---- LOGIC LAYER ----

LOGIC_FTT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID 
		, FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD 
		, FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM 
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD 
		, AGRE_TYP_NM                                        as                                        AGRE_TYP_NM 
		, FNCL_TRAN_TYP_GNRL_LDGR_IND                        as                        FNCL_TRAN_TYP_GNRL_LDGR_IND 
		, FNCL_TRAN_TYP_ACCT_PYBL_IND                        as                        FNCL_TRAN_TYP_ACCT_PYBL_IND 
		from SRC_FTT
            )

---- RENAME LAYER ----
,

RENAME_FTT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, FNCL_TRAN_TYP_ID                                   as                      FINANCIAL_TRANSACTION_TYPE_ID
		, FNCL_TRAN_TYP_CD                                   as                    FINANCIAL_TRANSACTION_TYPE_CODE
		, FNCL_TRAN_TYP_NM                                   as                    FINANCIAL_TRANSACTION_TYPE_DESC
		, AGRE_TYP_CD                                        as                                AGREEMENT_TYPE_CODE
		, AGRE_TYP_NM                                        as                                AGREEMENT_TYPE_DESC
		, FNCL_TRAN_TYP_GNRL_LDGR_IND                        as      FINANCIAL_TRANSACTION_TYPE_GENERAL_LEDGER_IND
		, FNCL_TRAN_TYP_ACCT_PYBL_IND                        as     FINANCIAL_TRANSACTION_TYPE_ACCOUNT_PAYABLE_IND 
				FROM     LOGIC_FTT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FTT                            as ( SELECT * from    RENAME_FTT   ),

---- JOIN LAYER ----

 JOIN_FTT  as  ( SELECT * 
				FROM  FILTER_FTT )
 SELECT 
  UNIQUE_ID_KEY
, FINANCIAL_TRANSACTION_TYPE_ID
, FINANCIAL_TRANSACTION_TYPE_CODE
, FINANCIAL_TRANSACTION_TYPE_DESC
, AGREEMENT_TYPE_CODE
, AGREEMENT_TYPE_DESC
, FINANCIAL_TRANSACTION_TYPE_GENERAL_LEDGER_IND
, FINANCIAL_TRANSACTION_TYPE_ACCOUNT_PAYABLE_IND
 FROM  JOIN_FTT