

      create or replace  table DEV_EDW.STAGING.DST_FINANCIAL_TRANSACTION_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_FTT as ( SELECT *     from     STAGING.STG_FINANCIAL_TRANSACTION_TYPE ),
//SRC_FTT as ( SELECT *     from     STG_FINANCIAL_TRANSACTION_TYPE) ,

---- LOGIC LAYER ----

LOGIC_FTT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID 
		, TRIM( FNCL_TRAN_TYP_CD )                           as                                   FNCL_TRAN_TYP_CD 
		, TRIM( FNCL_TRAN_TYP_NM )                           as                                   FNCL_TRAN_TYP_NM 
		, TRIM( AGRE_TYP_CD )                                as                                        AGRE_TYP_CD 
		, TRIM( AGRE_TYP_NM )                                as                                        AGRE_TYP_NM 
		, TRIM( FNCL_TRAN_TYP_GNRL_LDGR_IND )                as                        FNCL_TRAN_TYP_GNRL_LDGR_IND 
		, TRIM( FNCL_TRAN_TYP_ACCT_PYBL_IND )                as                        FNCL_TRAN_TYP_ACCT_PYBL_IND 
		from SRC_FTT
            )

---- RENAME LAYER ----
,

RENAME_FTT as ( SELECT 
		  FNCL_TRAN_TYP_ID                                   as                                   FNCL_TRAN_TYP_ID
		, FNCL_TRAN_TYP_CD                                   as                                   FNCL_TRAN_TYP_CD
		, FNCL_TRAN_TYP_NM                                   as                                   FNCL_TRAN_TYP_NM
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, AGRE_TYP_NM                                        as                                        AGRE_TYP_NM
		, FNCL_TRAN_TYP_GNRL_LDGR_IND                        as                        FNCL_TRAN_TYP_GNRL_LDGR_IND
		, FNCL_TRAN_TYP_ACCT_PYBL_IND                        as                        FNCL_TRAN_TYP_ACCT_PYBL_IND 
				FROM     LOGIC_FTT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FTT                            as ( SELECT * from    RENAME_FTT   ),

---- JOIN LAYER ----

 JOIN_FTT  as 
  ( SELECT 
 md5(cast(
    
    coalesce(cast(FNCL_TRAN_TYP_ID as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
,FNCL_TRAN_TYP_ID
,FNCL_TRAN_TYP_CD
,FNCL_TRAN_TYP_NM
,AGRE_TYP_CD
,AGRE_TYP_NM
,FNCL_TRAN_TYP_GNRL_LDGR_IND
,FNCL_TRAN_TYP_ACCT_PYBL_IND

				FROM  FILTER_FTT )
 SELECT * FROM  JOIN_FTT
      );
    