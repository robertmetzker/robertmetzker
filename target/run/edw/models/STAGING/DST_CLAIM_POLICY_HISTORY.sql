

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_POLICY_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.DST_CLAIM_INSURED_PARTICIPATION ),
SRC_BC as ( SELECT *     from     STAGING.STG_BUSINESS_CUSTOMER ),
SRC_CP as ( SELECT *     from     STAGING.DST_POLICY_PARTICIPATION_INSURED ),
SRC_CPC as ( SELECT *     from     STAGING.DST_POLICY_PARTICIPATION_INSURED ),
SRC_CPD as ( SELECT *     from     STAGING.DST_POLICY_PARTICIPATION_INSURED ),
//SRC_CLM as ( SELECT *     from     DST_CLAIM_INSURED_PARTICIPATION) ,
//SRC_BC as ( SELECT *     from     STG_BUSINESS_CUSTOMER) ,
//SRC_CP as ( SELECT *     from     DST_POLICY_PARTICIPATION_INSURED) ,
//SRC_CPC as ( SELECT *     from     DST_POLICY_PARTICIPATION_INSURED) ,
//SRC_CPD as ( SELECT *     from     DST_POLICY_PARTICIPATION_INSURED) ,

---- LOGIC LAYER ----


---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 
		  CLM_AGRE_ID                                        as                                        CLM_AGRE_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, INSRD_CUST_ID                                      as                                      INSRD_CUST_ID 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, CLM_ENTRY_DATE                                     as                                     CLM_ENTRY_DATE 
		, PSD_PLCY_NO                                        as                                        PSD_PLCY_NO 
		, CP_EFF_DATE                                        as                                        CP_EFF_DATE 
		, CP_END_DATE                                        as                                        CP_END_DATE 
		from SRC_CLM
            ),

LOGIC_BC as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO 
		, CUST_ID                                            as                                            CUST_ID 
		from SRC_BC
            ),

LOGIC_CP as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, PLCY_NO                                            as                                            PLCY_NO 
		, PPP_EFF_DATE                                       as                                       PPP_EFF_DATE 
		, PPP_END_DATE                                       as                                       PPP_END_DATE 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		from SRC_CP
            ),

LOGIC_CPC as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, PPP_EFF_DATE                                       as                                       PPP_EFF_DATE 
		, PPP_END_DATE                                       as                                       PPP_END_DATE 
		, PLCY_NO                                            as                                            PLCY_NO 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		from SRC_CPC
            ),

LOGIC_CPD as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, PPP_EFF_DATE                                       as                                       PPP_EFF_DATE 
		, PPP_END_DATE                                       as                                       PPP_END_DATE 
		, PLCY_NO                                            as                                            PLCY_NO 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		from SRC_CPD
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, INSRD_CUST_ID                                      as                                    INS_PARTICIPANT
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, CLM_ENTRY_DATE                                     as                                  CLAIM_CREATE_DATE
		, PSD_PLCY_NO                                        as                                        PSD_PLCY_NO
		, CP_EFF_DATE                                        as                                        CP_EFF_DATE
		, CP_END_DATE                                        as                                        CP_END_DATE 
				FROM     LOGIC_CLM   ), 
RENAME_BC as ( SELECT 
		  CUST_NO                                            as                                        EMP_CUST_NO
		, CUST_ID                                            as                                         BC_CUST_ID 
				FROM     LOGIC_BC   ), 
RENAME_CP as ( SELECT 
		  CUST_ID                                            as                                         CP_CUST_ID
		, PLCY_NO                                            as                                         CP_PLCY_NO
		, PPP_EFF_DATE                                       as                                    CP_PPP_EFF_DATE
		, PPP_END_DATE                                       as                                    CP_PPP_END_DATE
		, PLCY_AGRE_ID                                       as                                         CP_AGRE_ID
		, BUSN_SEQ_NO                                        as                                     CP_BUSN_SEQ_NO
		, CTL_ELEM_SUB_TYP_CD                                as                             CP_CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                             CP_CTL_ELEM_SUB_TYP_NM 
				FROM     LOGIC_CP   ), 
RENAME_CPC as ( SELECT 
		  CUST_ID                                            as                                        CPC_CUST_ID
		, PPP_EFF_DATE                                       as                                   CPC_PPP_EFF_DATE
		, PPP_END_DATE                                       as                                   CPC_PPP_END_DATE
		, PLCY_NO                                            as                                        CPC_PLCY_NO
		, PLCY_AGRE_ID                                       as                                        CPC_AGRE_ID
		, BUSN_SEQ_NO                                        as                                    CPC_BUSN_SEQ_NO
		, CTL_ELEM_SUB_TYP_CD                                as                            CPC_CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                            CPC_CTL_ELEM_SUB_TYP_NM 
				FROM     LOGIC_CPC   ), 
RENAME_CPD as ( SELECT 
		  CUST_ID                                            as                                        CPD_CUST_ID
		, PPP_EFF_DATE                                       as                                   CPD_PPP_EFF_DATE
		, PPP_END_DATE                                       as                                   CPD_PPP_END_DATE
		, PLCY_NO                                            as                                        CPD_PLCY_NO
		, PLCY_AGRE_ID                                       as                                        CPD_AGRE_ID
		, BUSN_SEQ_NO                                        as                                    CPD_BUSN_SEQ_NO
		, CTL_ELEM_SUB_TYP_CD                                as                            CPD_CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                            CPD_CTL_ELEM_SUB_TYP_NM 
				FROM     LOGIC_CPD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_CP                             as ( SELECT * from    RENAME_CP   ),
FILTER_CPD                            as ( SELECT * from    RENAME_CPD   ),
FILTER_CPC                            as ( SELECT * from    RENAME_CPC   ),
FILTER_BC                             as ( SELECT * from    RENAME_BC   ),

---- JOIN LAYER ----

CLM as ( SELECT * 
				FROM  FILTER_CLM
				LEFT JOIN FILTER_CP ON  FILTER_CLM.INS_PARTICIPANT =  FILTER_CP.CP_CUST_ID AND FILTER_CLM.PSD_PLCY_NO = FILTER_CP.CP_PLCY_NO
								LEFT JOIN FILTER_CPD ON  FILTER_CLM.INS_PARTICIPANT =  FILTER_CPD.CPD_CUST_ID AND FILTER_CP.CP_CUST_ID IS NULL 
						AND ((FILTER_CPD.CPD_PPP_EFF_DATE BETWEEN FILTER_CLM.CP_EFF_DATE AND FILTER_CLM.CP_END_DATE) 
						OR(FILTER_CLM.CP_EFF_DATE BETWEEN FILTER_CPD.CPD_PPP_EFF_DATE AND FILTER_CPD.CPD_PPP_END_DATE)) 
								LEFT JOIN FILTER_CPC ON  FILTER_CLM.INS_PARTICIPANT =  FILTER_CPC.CPC_CUST_ID AND (FILTER_CP.CP_CUST_ID IS NULL AND FILTER_CPD.CPD_CUST_ID IS NULL) 
								LEFT JOIN FILTER_BC ON  FILTER_CLM.INS_PARTICIPANT =  FILTER_BC.BC_CUST_ID  )

----------------------------------------------------------------------------------------------------------------------------------------
--- pull Distinct claims & Policy Insured based on Data Category
----------------------------------------------------------------------------------------------------------------------------------------
, MERGE_ETL AS (
SELECT  DISTINCT CLM_AGRE_ID,  CLM_NO, INS_PARTICIPANT,EMP_CUST_NO,CLM_OCCR_DATE,CLAIM_CREATE_DATE,
coalesce( CP_CUST_ID,CPD_CUST_ID,CPC_CUST_ID ) as CUST_ID,
coalesce( CP_PLCY_NO,CPD_PLCY_NO,CPC_PLCY_NO ) as PLCY_NO,
coalesce( CP_BUSN_SEQ_NO, CPD_BUSN_SEQ_NO, CPC_BUSN_SEQ_NO ) as BUSN_SEQ_NO,
coalesce( CP_AGRE_ID, CPD_AGRE_ID, CPC_AGRE_ID ) as PLCY_AGRE_ID,
coalesce( CP_CTL_ELEM_SUB_TYP_CD, CPD_CTL_ELEM_SUB_TYP_CD, CPC_CTL_ELEM_SUB_TYP_CD ) as CTL_ELEM_SUB_TYP_CD,
coalesce( CP_CTL_ELEM_SUB_TYP_NM, CPD_CTL_ELEM_SUB_TYP_NM, CPC_CTL_ELEM_SUB_TYP_NM ) as CTL_ELEM_SUB_TYP_NM, 
COALESCE( CP_PPP_EFF_DATE, CPD_PPP_EFF_DATE, CPC_PPP_EFF_DATE) as PPP_EFF_DT,
COALESCE( CP_PPP_END_DATE, CPD_PPP_END_DATE, CPC_PPP_END_DATE) as PPP_END_DT,
CP_EFF_DATE, CP_END_DATE, CP_CUST_ID,PSD_PLCY_NO
from CLM
     ) 

-------------------------------------------------------------------------------------------------------------------------------------------------------------
--- Identify the change event occurs for each ClmNo & CP_EFF_DATE changed using CONDITIONAL_CHANGE_EVENT Built-in function and keep latest record
-------------------------------------------------------------------------------------------------------------------------------------------------------------
, CHANGE_EVENT_ETL AS (
  SELECT 
  CLM_AGRE_ID
, CLM_OCCR_DATE
, CLAIM_CREATE_DATE
, CLM_NO
, INS_PARTICIPANT
, EMP_CUST_NO
, CP_EFF_DATE 
, CP_END_DATE
, CUST_ID
, PPP_EFF_DT
, PPP_END_DT
, PLCY_NO 
, PSD_PLCY_NO
, PLCY_AGRE_ID
, BUSN_SEQ_NO
, CTL_ELEM_SUB_TYP_CD
, CTL_ELEM_SUB_TYP_NM
, CONDITIONAL_CHANGE_EVENT(INS_PARTICIPANT||coalesce(PLCY_NO,'0')||coalesce(BUSN_SEQ_NO,'0'))OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DATE, CP_END_DATE, PPP_EFF_DT, PPP_END_DT) ORDRBY
 FROM MERGE_ETL
qualify(ROW_NUMBER() OVER (PARTITION BY CLM_NO, CP_EFF_DATE ORDER BY PPP_EFF_DT DESC, PPP_END_DT DESC, PLCY_AGRE_ID DESC) )  = 1
)

-------------------------------------------------------------------------------------------------------------------------------------------------------------
--- pull the MIN & MAX DATES when the PolicyNo changes after the Change Event is defined
-------------------------------------------------------------------------------------------------------------------------------------------------------------
, AGG_ETL AS (
SELECT 
CLM_NO, INS_PARTICIPANT, EMP_CUST_NO
, MIN(CP_EFF_DATE) as CP_EFF_DT
, MAX(CP_END_DATE) as CP_END_DT
, CUST_ID
, PLCY_NO
, PLCY_AGRE_ID
, BUSN_SEQ_NO 
, CTL_ELEM_SUB_TYP_CD, CTL_ELEM_SUB_TYP_NM
, CLM_AGRE_ID, CLM_OCCR_DATE, CLAIM_CREATE_DATE
FROM CHANGE_EVENT_ETL
GROUP BY CLM_NO, INS_PARTICIPANT,EMP_CUST_NO, CUST_ID,PLCY_NO, PLCY_AGRE_ID, BUSN_SEQ_NO,
  CTL_ELEM_SUB_TYP_CD, CTL_ELEM_SUB_TYP_NM, CLM_AGRE_ID, CLM_OCCR_DATE, CLAIM_CREATE_DATE, ORDRBY 
)

--- UNION LAYER ---

, UNION_ETL AS (
SELECT CLM_AGRE_ID
,CLM_NO
,INS_PARTICIPANT
,EMP_CUST_NO
,CLM_OCCR_DATE
,CLAIM_CREATE_DATE
,CP_EFF_DT as CLM_PLCY_RLTNS_EFF_DT
,CP_END_DT as CLM_PLCY_RLTNS_END_DT
,PLCY_NO
,PLCY_AGRE_ID
,BUSN_SEQ_NO
,CTL_ELEM_SUB_TYP_CD
,CTL_ELEM_SUB_TYP_NM
FROM AGG_ETL
UNION
-- The union here is to insert dummy row where CP_EFF_DT> CLM_ENTRY_DATE
select
CLM_AGRE_ID
,CLM_NO
,CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT then 1000042034 
ELSE  INS_PARTICIPANT END AS INS_PARTICIPANT1
, case when INS_PARTICIPANT1 = 1000042034 THEN 1000042034::TEXT ELSE EMP_CUST_NO END AS EMP_CUST_NO
, CLM_OCCR_DATE
, CLAIM_CREATE_DATE
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT 
then CLAIM_CREATE_DATE 
else CP_EFF_DT END AS CLM_PLCY_RLTNS_EFF_DT
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT then CP_EFF_DT -1 
else CP_END_DT END AS CLM_PLCY_RLTNS_END_DT
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT THEN NULL
ELSE PLCY_NO END AS PLCY_NO
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT THEN NULL
ELSE PLCY_AGRE_ID END AS PLCY_AGRE_ID
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT THEN NULL
ELSE BUSN_SEQ_NO END AS BUSN_SEQ_NO
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT THEN NULL
ELSE CTL_ELEM_SUB_TYP_CD END AS CTL_ELEM_SUB_TYP_CD
, CASE WHEN ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) = 1 
and  CLAIM_CREATE_DATE < CP_EFF_DT THEN NULL
ELSE CTL_ELEM_SUB_TYP_NM END AS CTL_ELEM_SUB_TYP_NM
FROM
AGG_ETL
UNION
-- The union here is to insert dummy row to define the "No Insured Found"
SELECT 
CLM_AGRE_ID
,CLM_NO
,CASE WHEN  CP_EFF_DT-1 != lag(CP_END_DT) over (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
then 1000042034 
ELSE  INS_PARTICIPANT END AS INS_PARTICIPANT1
, case when INS_PARTICIPANT1 = 1000042034 THEN 1000042034::TEXT ELSE EMP_CUST_NO END AS EMP_CUST_NO
, CLM_OCCR_DATE
, CLAIM_CREATE_DATE
, CASE WHEN  CP_EFF_DT -1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT) +1
ELSE CP_EFF_DT END AS CLM_PLCY_RLTNS_EFF_DT
, CASE WHEN  CP_EFF_DT-1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN CP_EFF_DT -1 
ELSE CP_END_DT END AS CLM_PLCY_RLTNS_END_DT
, CASE WHEN  CP_EFF_DT-1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN NULL
ELSE PLCY_NO END AS PLCY_NO
, CASE WHEN  CP_EFF_DT-1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN NULL
ELSE PLCY_AGRE_ID END AS PLCY_AGRE_ID
, CASE WHEN  CP_EFF_DT-1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN NULL
ELSE BUSN_SEQ_NO END AS BUSN_SEQ_NO
, CASE WHEN  CP_EFF_DT-1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN NULL
ELSE CTL_ELEM_SUB_TYP_CD END AS CTL_ELEM_SUB_TYP_CD
, CASE WHEN  CP_EFF_DT-1 != LAG(CP_END_DT) OVER (PARTITION BY CLM_NO ORDER BY CP_EFF_DT, CP_END_DT)
THEN NULL
ELSE CTL_ELEM_SUB_TYP_NM END AS CTL_ELEM_SUB_TYP_NM
 FROM AGG_ETL 
 )

---- FINAL LAYER ----

, ETL AS (
  SELECT md5(cast(
    
    coalesce(cast(CLM_NO as 
    varchar
), '') || '-' || coalesce(cast(CLM_PLCY_RLTNS_EFF_DT as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
, CLM_AGRE_ID
, CLM_NO
, INS_PARTICIPANT
, EMP_CUST_NO
, CLM_OCCR_DATE
, CLAIM_CREATE_DATE
, CLM_PLCY_RLTNS_EFF_DT as CLM_PLCY_RLTNS_EFF_DATE
, CASE WHEN CLM_PLCY_RLTNS_END_DT= '2099-12-31' THEN NULL ELSE CLM_PLCY_RLTNS_END_DT END as CLM_PLCY_RLTNS_END_DATE
, PLCY_NO
, BUSN_SEQ_NO
, PLCY_AGRE_ID
, CTL_ELEM_SUB_TYP_CD
, CTL_ELEM_SUB_TYP_NM
, CASE WHEN CLM_PLCY_RLTNS_END_DT= '2099-12-31' then 'Y' ELSE 'N' END as CRNT_PLCY_IND
 FROM UNION_ETL 
)

SELECT * FROM ETL
      );
    