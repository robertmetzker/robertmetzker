---- SRC LAYER ----
WITH
SRC_FTT as ( SELECT *     from    DEV_VIEWS.PCMP.FINANCIAL_TRANSACTION_TYPE ),
SRC_A as ( SELECT *     from   DEV_VIEWS.PCMP.AGREEMENT_TYPE ),
//SRC_FTT as ( SELECT *     from     FINANCIAL_TRANSACTION_TYPE) ,
//SRC_A as ( SELECT *     from     AGREEMENT_TYPE) ,

----- LOGIC LAYER ----

LOGIC_FTT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID 
		, upper( TRIM( FNCL_TRAN_TYP_CD ) )                  as                                   FNCL_TRAN_TYP_CD 
		, upper( TRIM( FNCL_TRAN_TYP_NM ) )                  as                                   FNCL_TRAN_TYP_NM 
		, upper( TRIM( AGRE_TYP_CD ) )                       as                                        AGRE_TYP_CD 
		, upper( FNCL_TRAN_TYP_GNRL_LDGR_IND )               as                        FNCL_TRAN_TYP_GNRL_LDGR_IND 
		, upper( FNCL_TRAN_TYP_ACCT_PYBL_IND )               as                        FNCL_TRAN_TYP_ACCT_PYBL_IND 
		from SRC_FTT
            ),
LOGIC_A as ( SELECT 
		  upper( TRIM( AGRE_TYP_NM ) )                       as                                        AGRE_TYP_NM 
		, upper( TRIM( AGRE_TYP_CD ) )                       as                                        AGRE_TYP_CD 
		from SRC_A
            )

---- RENAME LAYER ----
,

RENAME_FTT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID
		, FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD
		, FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, FNCL_TRAN_TYP_GNRL_LDGR_IND                        as                        FNCL_TRAN_TYP_GNRL_LDGR_IND
		, FNCL_TRAN_TYP_ACCT_PYBL_IND                        as                        FNCL_TRAN_TYP_ACCT_PYBL_IND 
				FROM     LOGIC_FTT   ), 
RENAME_A as ( SELECT 
		  AGRE_TYP_NM                                        as                                        AGRE_TYP_NM
		, AGRE_TYP_CD                                        as                                      A_AGRE_TYP_CD 
				FROM     LOGIC_A   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FTT                            as ( SELECT * from    RENAME_FTT   ),
FILTER_A                              as ( SELECT * from    RENAME_A   ),

---- JOIN LAYER ----

FTT as ( SELECT * 
				FROM  FILTER_FTT
				LEFT JOIN FILTER_A ON  FILTER_FTT.AGRE_TYP_CD =  FILTER_A.A_AGRE_TYP_CD  )
SELECT 
		  FNCL_TRAN_TYP_ID
		, FNCL_TRAN_TYP_CD
		, FNCL_TRAN_TYP_NM
		, AGRE_TYP_CD
		, AGRE_TYP_NM
		, FNCL_TRAN_TYP_GNRL_LDGR_IND
		, FNCL_TRAN_TYP_ACCT_PYBL_IND 
from FTT