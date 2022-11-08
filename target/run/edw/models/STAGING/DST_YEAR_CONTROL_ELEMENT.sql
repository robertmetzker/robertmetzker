

      create or replace  table DEV_EDW.STAGING.DST_YEAR_CONTROL_ELEMENT  as
      (---- SRC LAYER ----
WITH
SRC_CE             as ( SELECT *     FROM     STAGING.STG_YEAR_CONTROL_ELEMENT ),
//SRC_CE             as ( SELECT *     FROM     STG_YEAR_CONTROL_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_CE as ( SELECT 
		  TRIM( POLICY_TYPE_CODE )                           as                                   POLICY_TYPE_CODE 
		, TRIM( POLICY_TYPE_DESC )                           as                                   POLICY_TYPE_DESC 
		, TRIM( PAYMENT_PLAN_TYPE_CODE )                     as                             PAYMENT_PLAN_TYPE_CODE 
		, TRIM( PAYMENT_PLAN_TYPE_DESC )                     as                             PAYMENT_PLAN_TYPE_DESC 
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE 
		, TRIM( LEASE_TYPE_DESC )                            as                                    LEASE_TYPE_DESC 
		FROM SRC_CE
            )

---- RENAME LAYER ----
,

RENAME_CE         as ( SELECT 
		  POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE
		, LEASE_TYPE_DESC                                    as                                    LEASE_TYPE_DESC 
				FROM     LOGIC_CE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CE                             as ( SELECT * FROM    RENAME_CE   ),

---- JOIN LAYER ----

 JOIN_CE          as  ( SELECT * 
				FROM  FILTER_CE ),

------ETL LAYER------------
ETL AS(SELECT 
md5(cast(
    
    coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_PLAN_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(LEASE_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,POLICY_TYPE_CODE
,POLICY_TYPE_DESC
,PAYMENT_PLAN_TYPE_CODE
,PAYMENT_PLAN_TYPE_DESC
,LEASE_TYPE_CODE
,LEASE_TYPE_DESC
FROM JOIN_CE)

SELECT DISTINCT * FROM ETL
      );
    