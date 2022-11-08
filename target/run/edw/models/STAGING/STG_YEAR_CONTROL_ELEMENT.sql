

      create or replace  table DEV_EDW.STAGING.STG_YEAR_CONTROL_ELEMENT  as
      (---- SRC LAYER ----
WITH
SRC_PT             as ( SELECT *     FROM     DEV_VIEWS.PCMP.CONTROL_ELEMENT_SUB_TYPE ),
SRC_PAY            as ( SELECT *     FROM     DEV_VIEWS.PCMP.CONTROL_ELEMENT_SUB_TYPE ),
SRC_LEASE          as ( SELECT *     FROM     DEV_VIEWS.PCMP.CONTROL_ELEMENT_SUB_TYPE ),
SRC_PCE          as ( SELECT *     FROM         DEV_VIEWS.PCMP.POLICY_CONTROL_ELEMENT ),
//SRC_PT             as ( SELECT *     FROM     CONTROL_ELEMENT_SUB_TYPE) ,
//SRC_PAY            as ( SELECT *     FROM     CONTROL_ELEMENT_SUB_TYPE) ,
//SRC_LEASE          as ( SELECT *     FROM     CONTROL_ELEMENT_SUB_TYPE) ,
//SRC_PCE            AS (SELECT * FROM POLICY_CONTROL_ELEMENT),

---- LOGIC LAYER ----


LOGIC_PT as ( SELECT 
		  CTL_ELEM_TYP_ID                                    as                                    CTL_ELEM_TYP_ID 
		, UPPER(TRIM( CTL_ELEM_SUB_TYP_CD ))                        as                                CTL_ELEM_SUB_TYP_CD 
		, UPPER(TRIM( CTL_ELEM_SUB_TYP_NM ))                        as                                CTL_ELEM_SUB_TYP_NM 
		, UPPER(CTL_ELEM_SUB_TYP_VOID_IND)                          as                          CTL_ELEM_SUB_TYP_VOID_IND 
		FROM SRC_PT
            ),

LOGIC_PAY as ( SELECT 
		  CTL_ELEM_TYP_ID                                    as                                    CTL_ELEM_TYP_ID 
		, UPPER(TRIM( CTL_ELEM_SUB_TYP_CD ))                        as                                CTL_ELEM_SUB_TYP_CD 
		, UPPER(TRIM( CTL_ELEM_SUB_TYP_NM ))                        as                                CTL_ELEM_SUB_TYP_NM 
		, UPPER(CTL_ELEM_SUB_TYP_VOID_IND)                          as                          CTL_ELEM_SUB_TYP_VOID_IND 
		, CTL_ELEM_SUB_TYP_ID                               AS                                 CTL_ELEM_SUB_TYP_ID
		FROM SRC_PAY
            ),

LOGIC_LEASE as ( SELECT 
		  CTL_ELEM_TYP_ID                                    as                                    CTL_ELEM_TYP_ID 
		, UPPER(TRIM( CTL_ELEM_SUB_TYP_CD ))                        as                                CTL_ELEM_SUB_TYP_CD 
		, UPPER(TRIM( CTL_ELEM_SUB_TYP_NM ))                        as                                CTL_ELEM_SUB_TYP_NM 
		, UPPER(CTL_ELEM_SUB_TYP_VOID_IND)                          as                          CTL_ELEM_SUB_TYP_VOID_IND 
		FROM SRC_LEASE
            ),
LOGIC_PCE AS (SELECT	
         CTL_ELEM_SUB_TYP_ID                               AS                                 CTL_ELEM_SUB_TYP_ID
		 ,UPPER(TRIM(VOID_IND))                            AS                            VOID_IND		
FROM SRC_PCE)
---- RENAME LAYER ----
,

RENAME_PT         as ( SELECT 
		  CTL_ELEM_TYP_ID                                    as                                 PT_CTL_ELEM_TYP_ID
		, CTL_ELEM_SUB_TYP_CD                                as                                   POLICY_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                   POLICY_TYPE_DESC
		, CTL_ELEM_SUB_TYP_VOID_IND                          as                       PT_CTL_ELEM_SUB_TYP_VOID_IND 
				FROM     LOGIC_PT   ), 
RENAME_PAY        as ( SELECT 
		  CTL_ELEM_TYP_ID                                    as                                PAY_CTL_ELEM_TYP_ID
		, CTL_ELEM_SUB_TYP_CD                                as                             PAYMENT_PLAN_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                             PAYMENT_PLAN_TYPE_DESC
		, CTL_ELEM_SUB_TYP_VOID_IND                          as                      PAY_CTL_ELEM_SUB_TYP_VOID_IND
		, CTL_ELEM_SUB_TYP_ID                               AS                                 CTL_ELEM_SUB_TYP_ID 
				FROM     LOGIC_PAY   ), 
RENAME_LEASE      as ( SELECT 
		  CTL_ELEM_TYP_ID                                    as                              LEASE_CTL_ELEM_TYP_ID
		, CTL_ELEM_SUB_TYP_CD                                as                                    LEASE_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                    LEASE_TYPE_DESC
		, CTL_ELEM_SUB_TYP_VOID_IND                          as                    LEASE_CTL_ELEM_SUB_TYP_VOID_IND 
				FROM     LOGIC_LEASE   ),
RENAME_PCE AS (SELECT	
         CTL_ELEM_SUB_TYP_ID                               AS                                 PCE_CTL_ELEM_SUB_TYP_ID
		 ,VOID_IND                                          AS                            PCE_VOID_IND		
FROM LOGIC_PCE)				

---- FILTER LAYER (uses aliases) ----
,
FILTER_PT                             as ( SELECT * FROM    RENAME_PT 
                                            WHERE PT_CTL_ELEM_TYP_ID =6303000 AND PT_CTL_ELEM_SUB_TYP_VOID_IND = 'N'  ),
FILTER_PAY                            as ( SELECT * FROM    RENAME_PAY 
                                            WHERE PAY_CTL_ELEM_TYP_ID =7 AND PAY_CTL_ELEM_SUB_TYP_VOID_IND = 'N'  ),
FILTER_LEASE                          as ( SELECT * FROM    RENAME_LEASE 
                                            WHERE LEASE_CTL_ELEM_TYP_ID =11 AND LEASE_CTL_ELEM_SUB_TYP_VOID_IND = 'N'  ),
FILTER_PCE                             as( select * from RENAME_PCE
                                             WHERE PCE_VOID_IND='N' ),
---- JOIN LAYER ----

JOIN_LEASE       as  ( select * from FILTER_PT
full outer join lateral (select * from FILTER_PAY) T2
INNER JOIN FILTER_PCE ON T2.CTL_ELEM_SUB_TYP_ID=PCE_CTL_ELEM_SUB_TYP_ID
full outer join lateral (select * from FILTER_LEASE )T3
 )

SELECT DISTINCT
 PT_CTL_ELEM_TYP_ID
,POLICY_TYPE_CODE
,POLICY_TYPE_DESC
,PT_CTL_ELEM_SUB_TYP_VOID_IND
,PAY_CTL_ELEM_TYP_ID
,PAYMENT_PLAN_TYPE_CODE
,PAYMENT_PLAN_TYPE_DESC
,PAY_CTL_ELEM_SUB_TYP_VOID_IND
,LEASE_CTL_ELEM_TYP_ID
,LEASE_TYPE_CODE
,LEASE_TYPE_DESC
,LEASE_CTL_ELEM_SUB_TYP_VOID_IND
 FROM  JOIN_LEASE
      );
    