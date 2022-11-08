

      create or replace  table DEV_EDW.STAGING.DST_POLICY_FINANCIAL_TRANSACTION_COMMENT  as
      (---- SRC LAYER ----
WITH
SRC_PFT            as ( SELECT *     FROM     STAGING.STG_POLICY_FINANCIAL_TRANSACTION ),
//SRC_PFT            as ( SELECT *     FROM     STG_POLICY_FINANCIAL_TRANSACTION) ,

---- LOGIC LAYER ----


LOGIC_PFT as ( SELECT 
		NVL (TRIM( PFT_CMT ),'N/A')                                    as                                            PFT_CMT 
		FROM SRC_PFT
            )

---- RENAME LAYER ----
,

RENAME_PFT        as ( SELECT 
		  PFT_CMT                                            as                                            PFT_CMT 
				FROM     LOGIC_PFT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PFT                            as ( SELECT * FROM    RENAME_PFT   ),

---- JOIN LAYER ----

 JOIN_PFT         as  ( SELECT * 
				FROM  FILTER_PFT )
----ETL LAYER----				
 ,
ETL as (SELECT DISTINCT md5(cast(
    
    coalesce(cast(PFT_CMT as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, 
PFT_CMT
        from JOIN_PFT)

SELECT * FROM ETL
      );
    