

      create or replace  table DEV_EDW.STAGING.DST_POLICY_PARTICIPATION_INSURED  as
      (---- SRC LAYER ----
WITH
SRC_PP as ( SELECT *     from     STAGING.STG_POLICY_PERIOD_PARTICIPATION ),
SRC_PC as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
//SRC_PP as ( SELECT *     from     STG_POLICY_PERIOD_PARTICIPATION) ,
//SRC_PC as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_PP as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, TRIM( PLCY_PRD_PTCP_INS_PRI_IND )                  as                          PLCY_PRD_PTCP_INS_PRI_IND 
		, PLCY_PRD_PTCP_EFF_DATE                             as                             PLCY_PRD_PTCP_EFF_DATE 
		, PLCY_PRD_PTCP_END_DATE                             as                             PLCY_PRD_PTCP_END_DATE 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, TRIM( AGRE_TYP_CD )                                as                                        AGRE_TYP_CD 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		from SRC_PP
            ),

LOGIC_PC as ( SELECT 
		  TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
		, TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
		, TRIM( CTL_ELEM_TYP_CD )                            as                                    CTL_ELEM_TYP_CD 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		from SRC_PC
            )

---- RENAME LAYER ----
,

RENAME_PP as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
		, AGRE_ID                                            as                                       PLCY_AGRE_ID
		, PLCY_NO                                            as                                            PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, PLCY_PRD_PTCP_INS_PRI_IND                          as                                            PRI_IND
		, PLCY_PRD_PTCP_EFF_DATE                             as                                       PPP_EFF_DATE
		, PLCY_PRD_PTCP_END_DATE                             as                                       PPP_END_DATE
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
				FROM     LOGIC_PP   ), 
RENAME_PC as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM
		, CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
		, VOID_IND                                           as                                           VOID_IND
		, PLCY_PRD_ID                                        as                                     PC_PLCY_PRD_ID 
				FROM     LOGIC_PC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * from    RENAME_PP 
                                            WHERE AGRE_TYP_CD = 'PLCY' AND PTCP_TYP_CD = 'INSRD'  ),
FILTER_PC                             as ( SELECT * from    RENAME_PC 
                                            WHERE CTL_ELEM_TYP_CD  = 'PLCY_TYP' AND VOID_IND = 'N'  ),

---- JOIN LAYER ----

PP as ( SELECT * 
				FROM  FILTER_PP
				INNER JOIN FILTER_PC ON  FILTER_PP.PLCY_PRD_ID =  FILTER_PC.PC_PLCY_PRD_ID  )

----------------------------------------------------------------------------------------------------------------------------------------
--- Row number assigned to records that are flat cancelled and remove them later except first flat cancelled record
-- Flat cancelled --> Both PPP_EFF_DATE & PPP_END_DATE are same.
----------------------------------------------------------------------------------------------------------------------------------------
, ETL AS (
SELECT  CUST_ID, PLCY_AGRE_ID, PLCY_NO, BUSN_SEQ_NO, PRI_IND, CTL_ELEM_SUB_TYP_CD, CTL_ELEM_SUB_TYP_NM, PPP_EFF_DATE, coalesce( PPP_END_DATE, '2099-12-31') AS PPP_END_DATE, PTCP_TYP_CD, AGRE_TYP_CD, PLCY_PRD_ID,
 ROW_NUMBER() OVER (PARTITION BY CUST_ID,PLCY_AGRE_ID, PLCY_NO ORDER BY PPP_EFF_DATE, PPP_END_DATE) AS FLAT_CNCL_SEQ
from PP 
     ) 

-------------------------------------------------------------------------------------------------------------------------------------------------------------
--- Identify the change event occurs for each customer when the PolicyNo changed using CONDITIONAL_CHANGE_EVENT Built-in function 
-------------------------------------------------------------------------------------------------------------------------------------------------------------
, CHANGE_EVENT_ETL AS (
  SELECT *, 
  CONDITIONAL_CHANGE_EVENT(PLCY_NO||BUSN_SEQ_NO) OVER (PARTITION BY CUST_ID ORDER BY PLCY_NO,PPP_EFF_DATE, PPP_END_DATE) AS CCE  
    from ETL
   WHERE (PPP_EFF_DATE <> PPP_END_DATE OR (PPP_EFF_DATE = PPP_END_DATE AND  FLAT_CNCL_SEQ=1 ))
)

-------------------------------------------------------------------------------------------------------------------------------------------------------------
--- pull the MIN & MAX DATES when the policy No changes after the Change Event is defined
-------------------------------------------------------------------------------------------------------------------------------------------------------------
, FINAL_ETL AS (
 SELECT CUST_ID,PLCY_AGRE_ID, PLCY_NO, BUSN_SEQ_NO, PRI_IND, CTL_ELEM_SUB_TYP_CD, CTL_ELEM_SUB_TYP_NM ,MIN(PPP_EFF_DATE) AS PPP_EFF_DATE, MAX(PPP_END_DATE) AS PPP_END_DATE 
   from CHANGE_EVENT_ETL
  GROUP BY CUST_ID,PLCY_AGRE_ID, PLCY_NO, BUSN_SEQ_NO, PRI_IND, CTL_ELEM_SUB_TYP_CD, CTL_ELEM_SUB_TYP_NM , CCE
  )
 
---- FINAL LAYER ----

  SELECT md5(cast(
    
    coalesce(cast(CUST_ID as 
    varchar
), '') || '-' || coalesce(cast(PLCY_AGRE_ID as 
    varchar
), '') || '-' || coalesce(cast(PPP_EFF_DATE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
   , CUST_ID
   , PLCY_AGRE_ID
   , PLCY_NO 
   , BUSN_SEQ_NO
   , CTL_ELEM_SUB_TYP_CD
   , CTL_ELEM_SUB_TYP_NM
   , PRI_IND
   , PPP_EFF_DATE
   , PPP_END_DATE
  FROM FINAL_ETL
      );
    