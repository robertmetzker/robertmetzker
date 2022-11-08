---- SRC LAYER ----
WITH
SRC_STS as ( SELECT *     from    DEV_VIEWS.PCMP.BWC_FINANCIAL_TRAN_SUB_TYP ),
//SRC_STS as ( SELECT *     from     BWC_FINANCIAL_TRAN_SUB_TYP) ,

---- LOGIC LAYER ----


LOGIC_STS as ( SELECT 
		  upper( TRIM( FNCL_TRAN_SUB_TYP_CD ) )              as                               FNCL_TRAN_SUB_TYP_CD 
		, upper( TRIM( FNCL_TRAN_SUB_TYP_NM ) )              as                               FNCL_TRAN_SUB_TYP_NM 
		from SRC_STS
            )

---- RENAME LAYER ----
,

RENAME_STS as ( SELECT 
		  FNCL_TRAN_SUB_TYP_CD                               as                               FNCL_TRAN_SUB_TYP_CD
		, FNCL_TRAN_SUB_TYP_NM                               as                               FNCL_TRAN_SUB_TYP_NM 
				FROM     LOGIC_STS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_STS                            as ( SELECT * from    RENAME_STS   ),

---- JOIN LAYER ----

 JOIN_STS  as  ( SELECT * 
				FROM  FILTER_STS )
 SELECT * FROM  JOIN_STS