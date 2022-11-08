

      create or replace  table DEV_EDW.STAGING.DST_FINANCIAL_TRANSACTION_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_FTS as ( SELECT *     from     STAGING.STG_FINANCIAL_TRANSACTION_STATUS ),
//SRC_FTS as ( SELECT *     from     STG_FINANCIAL_TRANSACTION_STATUS) ,

---- LOGIC LAYER ----


LOGIC_FTS as ( SELECT 
		  md5(cast(
    
    coalesce(cast(FNCL_TRAN_SUB_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) as                               UNIQUE_ID_KEY                                   
		, TRIM( FNCL_TRAN_SUB_TYP_CD )                       as                               FNCL_TRAN_SUB_TYP_CD 
		, TRIM( FNCL_TRAN_SUB_TYP_NM )                       as                               FNCL_TRAN_SUB_TYP_NM 
		from SRC_FTS
            )

---- RENAME LAYER ----
,

RENAME_FTS as ( SELECT 
          UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, FNCL_TRAN_SUB_TYP_CD                               as                               FNCL_TRAN_SUB_TYP_CD
		, FNCL_TRAN_SUB_TYP_NM                               as                               FNCL_TRAN_SUB_TYP_NM 
				FROM     LOGIC_FTS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FTS                            as ( SELECT * from    RENAME_FTS   ),

---- JOIN LAYER ----

 JOIN_FTS  as  ( SELECT * 
				FROM  FILTER_FTS )
 SELECT * FROM  JOIN_FTS
      );
    