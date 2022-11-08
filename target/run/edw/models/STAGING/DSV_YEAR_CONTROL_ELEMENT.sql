
  create or replace  view DEV_EDW.STAGING.DSV_YEAR_CONTROL_ELEMENT  as (
    

---- SRC LAYER ----
WITH
SRC_DIS            as ( SELECT *     FROM     STAGING.DST_YEAR_CONTROL_ELEMENT ),
//SRC_DIS            as ( SELECT *     FROM     ) ,

---- LOGIC LAYER ----


LOGIC_DIS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC 
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE 
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC 
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE 
		, LEASE_TYPE_DESC                                    as                                    LEASE_TYPE_DESC 
		FROM SRC_DIS
            )

---- RENAME LAYER ----
,

RENAME_DIS        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC
		, PAYMENT_PLAN_TYPE_CODE                             as                             PAYMENT_PLAN_TYPE_CODE
		, PAYMENT_PLAN_TYPE_DESC                             as                             PAYMENT_PLAN_TYPE_DESC
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE
		, LEASE_TYPE_DESC                                    as                                    LEASE_TYPE_DESC 
				FROM     LOGIC_DIS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DIS                             as ( SELECT * FROM    RENAME_DIS   ),

---- JOIN LAYER ----

 JOIN_DIS          as  ( SELECT * 
				FROM  FILTER_DIS )
 SELECT * FROM  JOIN_DIS
  );
