

      create or replace  table DEV_EDW.STAGING.DST_PAYEE_NAME  as
      (---- SRC LAYER ----
WITH
SRC_DPC as ( SELECT *     from     STAGING.STG_DETAIL_PAYMENT_CODING ),
//SRC_DPC as ( SELECT *     from     STG_DETAIL_PAYMENT_CODING) ,

---- LOGIC LAYER ----

LOGIC_DPC as ( SELECT 
		  TRIM( PAYEE_NAME )                                 AS                                         PAYEE_NAME 
		from SRC_DPC
            )

---- RENAME LAYER ----
,

RENAME_DPC as ( SELECT 
		  PAYEE_NAME                                         as                                         PAYEE_NAME 
				FROM     LOGIC_DPC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DPC                            as ( SELECT * from    RENAME_DPC   ),

---- JOIN LAYER ----

 JOIN_DPC  as  ( SELECT * 
				FROM  FILTER_DPC ),

------ ETL LAYER ----------------

ETL AS ( SELECT DISTINCT md5(cast(
    
    coalesce(cast(PAYEE_NAME as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY, * FROM JOIN_DPC)
SELECT * FROM ETL
      );
    