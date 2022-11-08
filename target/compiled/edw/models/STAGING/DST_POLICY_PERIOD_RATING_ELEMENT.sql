---- SRC LAYER ----
WITH
SRC_RATING as ( SELECT *     from     STAGING.STG_POLICY_PERIOD_RATING_ELEMENT ),
SRC_PPER as ( SELECT *     from     STAGING.STG_POLICY_PERIOD ),
SRC_ELE as ( SELECT *     from     STAGING.STG_RATING_ELEMENT ),
SRC_POL as ( SELECT *     from     STAGING.STG_POLICY ),
SRC_PT as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
//SRC_RATING as ( SELECT *     from     STG_POLICY_PERIOD_RATING_ELEMENT) ,
//SRC_PPER as ( SELECT *     from     STG_POLICY_PERIOD) ,
//SRC_ELE as ( SELECT *     from     STG_RATING_ELEMENT) ,
//SRC_POL as ( SELECT *     from     STG_POLICY) ,
//SRC_PT as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,

---- LOGIC LAYER ----

LOGIC_RATING as ( SELECT 
		  PPRE_ID                                            as                                            PPRE_ID 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, PPRE_EFF_DT                                        as                                        PPRE_EFF_DT 
		, PPRE_END_DT                                        as                                        PPRE_END_DT 
		, TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD 
		, TRIM( RT_ELEM_TYP_NM )                             as                                     RT_ELEM_TYP_NM 
		, TRIM( RT_ELEM_USAGE_TYP_CD )                       as                               RT_ELEM_USAGE_TYP_CD 
		, TRIM( RT_ELEM_USAGE_TYP_DESC )                     as                             RT_ELEM_USAGE_TYP_DESC 
		, TRIM( RT_ELEM_DSPLY_TYP_CD )                       as                               RT_ELEM_DSPLY_TYP_CD 
		, TRIM( RT_ELEM_DSPLY_TYP_NM )                       as                               RT_ELEM_DSPLY_TYP_NM 
		, PPRE_RT                                            as                                            PPRE_RT 
		, TRIM( EXPRN_MOD_TYP_CD )                           as                                   EXPRN_MOD_TYP_CD 
		, TRIM( EXPRN_MOD_TYP_NM )                           as                                   EXPRN_MOD_TYP_NM 
		, EXPRN_MOD_FCTR                                     as                                     EXPRN_MOD_FCTR 
		, EXPRN_MOD_EFF_DT                                   as                                   EXPRN_MOD_EFF_DT 
		, EXPRN_MOD_END_DT                                   as                                   EXPRN_MOD_END_DT 
		, EXPRN_MOD_ANV_RT_DT                                as                                EXPRN_MOD_ANV_RT_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_RATING
            ),
LOGIC_PPER as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
        , PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
		, VOID_IND                                           as                                           VOID_IND
		, CAST((PLCY_PRD_EFF_DT || ' - ' || PLCY_PRD_END_DT) as TEXT)                           POLICY_PERIOD_DESC
		, case when PLCY_PRD_EFF_DT = min(PLCY_PRD_EFF_DT) 
		   over (partition by PLCY_NO, AGRE_ID) then 'Y' else 'N' end as                            NEW_POLICY_IND
  
		 from SRC_PPER
            ),			
LOGIC_ELE as ( SELECT 
		  TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD 
		, TRIM( RT_ELEM_TYP_NM )                             as                                     RT_ELEM_TYP_NM 
		, RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT 
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT 
		, TRIM( RT_ELEM_USAGE_TYP_CD )                       as                               RT_ELEM_USAGE_TYP_CD 
		, TRIM( RT_ELEM_USAGE_TYP_DESC )                     as                               RT_ELEM_USAGE_TYP_NM 
		, TRIM( RT_ELEM_TYP_RLT )                            as                                    RT_ELEM_TYP_RLT 
		from SRC_ELE
            ),

LOGIC_POL as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT 
		from SRC_POL
            ),
LOGIC_PT as ( SELECT 
		  TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
		, TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( CTL_ELEM_TYP_CD )                            as                                    CTL_ELEM_TYP_CD 
		, PLCY_CTL_ELEM_EFF_DT                               as                               PLCY_CTL_ELEM_EFF_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                     CTL_ELEM_SUB_TYP_WEB_DSPLY_IND
		, PLCY_CTL_ELEM_END_DT                               as                               PLCY_CTL_ELEM_END_DT 	
		, case when  CTL_ELEM_SUB_TYP_CD = 'PEC' THEN 'Y' ELSE 'N'    end   as                      PEC_POLICY_IND					
				from SRC_PT

            ),
LOGIC_PP as ( SELECT 
		  TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
		, TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( CTL_ELEM_TYP_CD )                            as                                    CTL_ELEM_TYP_CD 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PLCY_CTL_ELEM_EFF_DT                               as                               PLCY_CTL_ELEM_EFF_DT 
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                     CTL_ELEM_SUB_TYP_WEB_DSPLY_IND
		, PLCY_CTL_ELEM_END_DT                               as                               PLCY_CTL_ELEM_END_DT 	


		from SRC_PT
            ),
LOGIC_LS as ( SELECT 
		  TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
		, TRIM( CTL_ELEM_SUB_TYP_NM )                        as                                CTL_ELEM_SUB_TYP_NM 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, TRIM( CTL_ELEM_TYP_CD )                            as                                    CTL_ELEM_TYP_CD 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, PLCY_CTL_ELEM_EFF_DT                               as                               PLCY_CTL_ELEM_EFF_DT 
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                     CTL_ELEM_SUB_TYP_WEB_DSPLY_IND
		, MAX(PLCY_CTL_ELEM_END_DT)                          as                               PLCY_CTL_ELEM_END_DT 
				from SRC_PT
		group by 1,2,3,4,5,6,7
            )

---- RENAME LAYER ----
,

RENAME_RATING as ( SELECT 
		  PPRE_ID                                            as                                            PPRE_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_NO                                            as                                            PLCY_NO
		, PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
		, PPRE_END_DT                                        as                                        PPRE_END_DT
		, RT_ELEM_TYP_CD                                     as                                     RT_ELEM_TYP_CD
		, RT_ELEM_TYP_NM                                     as                                     RT_ELEM_TYP_NM
		, RT_ELEM_USAGE_TYP_CD                               as                               RT_ELEM_USAGE_TYP_CD
		, RT_ELEM_USAGE_TYP_DESC                             as                             RT_ELEM_USAGE_TYP_DESC
		, RT_ELEM_DSPLY_TYP_CD                               as                               RT_ELEM_DSPLY_TYP_CD
		, RT_ELEM_DSPLY_TYP_NM                               as                               RT_ELEM_DSPLY_TYP_NM
		, PPRE_RT                                            as                                            PPRE_RT
		, EXPRN_MOD_TYP_CD                                   as                                   EXPRN_MOD_TYP_CD
		, EXPRN_MOD_TYP_NM                                   as                                   EXPRN_MOD_TYP_NM
		, EXPRN_MOD_FCTR                                     as                                     EXPRN_MOD_FCTR
		, EXPRN_MOD_EFF_DT                                   as                                   EXPRN_MOD_EFF_DT
		, EXPRN_MOD_END_DT                                   as                                   EXPRN_MOD_END_DT
		, EXPRN_MOD_ANV_RT_DT                                as                                EXPRN_MOD_ANV_RT_DT
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_RATING   ), 
RENAME_PPER as ( SELECT 
		  PLCY_PRD_ID                                        as                                 PPER_PLCY_PRD_ID
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, VOID_IND                                           as                                      PPER_VOID_IND			  
				FROM     LOGIC_PPER   ), 
RENAME_ELE as ( SELECT 
		  RT_ELEM_TYP_CD                                     as                                 ELE_RT_ELEM_TYP_CD
		, RT_ELEM_TYP_NM                                     as                                 ELE_RT_ELEM_TYP_NM
		, RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT
		, RT_ELEM_USAGE_TYP_CD                               as                           ELE_RT_ELEM_USAGE_TYP_CD
		, RT_ELEM_USAGE_TYP_NM                               as                               RT_ELEM_USAGE_TYP_NM
		, RT_ELEM_TYP_RLT                                    as                                    RT_ELEM_TYP_RLT 
				FROM     LOGIC_ELE   ), 
RENAME_POL as ( SELECT 
		  PLCY_NO                                            as                                        POL_PLCY_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, CUST_NO                                            as                                            CUST_NO
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT 
				FROM     LOGIC_POL   ), 
RENAME_PT as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                   POLICY_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                   POLICY_TYPE_DESC
		, PLCY_PRD_ID                                        as                                     PT_PLCY_PRD_ID
		, CTL_ELEM_TYP_CD                                    as                                 PT_CTL_ELEM_TYP_CD
		, PLCY_CTL_ELEM_EFF_DT                               as                            PT_PLCY_CTL_ELEM_EFF_DT
		, PLCY_CTL_ELEM_END_DT                               as                            PT_PLCY_CTL_ELEM_END_DT
		, VOID_IND                                           as                                        PT_VOID_IND 
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                  PT_CTL_ELEM_SUB_TYP_WEB_DSPLY_IND
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND
				FROM     LOGIC_PT   ), 
RENAME_PP as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                  PAYMENT_PLAN_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                  PAYMENT_PLAN_DESC
		, PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID
		, CTL_ELEM_TYP_CD                                    as                                 PP_CTL_ELEM_TYP_CD
		, VOID_IND                                           as                                        PP_VOID_IND
		, PLCY_CTL_ELEM_EFF_DT                               as                            PP_PLCY_CTL_ELEM_EFF_DT
		, PLCY_CTL_ELEM_END_DT                               as                            PP_PLCY_CTL_ELEM_END_DT 
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                  PP_CTL_ELEM_SUB_TYP_WEB_DSPLY_IND


				FROM     LOGIC_PP   ), 
RENAME_LS as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                    LEASE_TYPE_CODE
		, CTL_ELEM_SUB_TYP_NM                                as                                    LEASE_TYPE_DESC
		, PLCY_PRD_ID                                        as                                     LS_PLCY_PRD_ID
		, CTL_ELEM_TYP_CD                                    as                                 LS_CTL_ELEM_TYP_CD
		, VOID_IND                                           as                                        LS_VOID_IND
		, PLCY_CTL_ELEM_EFF_DT                               as                            LS_PLCY_CTL_ELEM_EFF_DT
		, PLCY_CTL_ELEM_END_DT                               as                            LS_PLCY_CTL_ELEM_END_DT 
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                  LS_CTL_ELEM_SUB_TYP_WEB_DSPLY_IND
				FROM     LOGIC_LS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_RATING                         as ( SELECT * from    RENAME_RATING 
                                            WHERE RENAME_RATING.RT_ELEM_TYP_CD NOT IN ('ARD','ELILP') and RENAME_RATING.VOID_IND='N' 
											and RENAME_RATING.PLCY_NO is not null
											 ),
FILTER_PPER	                          as ( SELECT * from    RENAME_PPER  
                                           where PPER_VOID_IND = 'N' ),										 
FILTER_ELE                            as ( SELECT * from    RENAME_ELE   ),
FILTER_POL                            as ( SELECT * from    RENAME_POL   ),
FILTER_PT                             as ( SELECT * from    RENAME_PT 
                                            WHERE 
											PT_VOID_IND = 'N' AND PT_CTL_ELEM_TYP_CD = 'PLCY_TYP'  ),
FILTER_PP                             as ( SELECT * from    RENAME_PP 
                                            WHERE  
											PP_VOID_IND = 'N' AND PP_CTL_ELEM_TYP_CD = 'PYMT_PLN'  ),
FILTER_LS                             as ( SELECT * from    RENAME_LS 
                                            WHERE LS_CTL_ELEM_SUB_TYP_WEB_DSPLY_IND = 'Y' AND LS_VOID_IND = 'N' AND LS_CTL_ELEM_TYP_CD = 'EMP_LS_PLCY_TYP'  ),

---- JOIN LAYER ----

RATING as ( SELECT * 
				FROM  FILTER_RATING
				INNER JOIN FILTER_PPER ON FILTER_RATING.PLCY_PRD_ID = FILTER_PPER.PPER_PLCY_PRD_ID
				LEFT JOIN FILTER_ELE ON  FILTER_RATING.RT_ELEM_TYP_CD =  FILTER_ELE.ELE_RT_ELEM_TYP_CD and FILTER_RATING.RT_ELEM_USAGE_TYP_CD = FILTER_ELE.ELE_RT_ELEM_USAGE_TYP_CD
				LEFT JOIN FILTER_POL ON  FILTER_RATING.PLCY_NO =  FILTER_POL.POL_PLCY_NO 
				LEFT JOIN FILTER_PT ON  FILTER_RATING.PLCY_PRD_ID =  FILTER_PT.PT_PLCY_PRD_ID AND PPRE_END_DT BETWEEN FILTER_PT.PT_PLCY_CTL_ELEM_EFF_DT AND FILTER_PT.PT_PLCY_CTL_ELEM_END_DT    
				LEFT JOIN FILTER_PP ON  FILTER_RATING.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID AND PPRE_END_DT BETWEEN FILTER_PP.PP_PLCY_CTL_ELEM_EFF_DT AND FILTER_PP.PP_PLCY_CTL_ELEM_END_DT    
				LEFT JOIN FILTER_LS ON  FILTER_RATING.PLCY_PRD_ID =  FILTER_LS.LS_PLCY_PRD_ID  AND PPRE_END_DT BETWEEN FILTER_LS.LS_PLCY_CTL_ELEM_EFF_DT AND FILTER_LS.LS_PLCY_CTL_ELEM_END_DT     )
SELECT 
	PPRE_ID
,	PLCY_PRD_ID
,	PLCY_NO
,	PPRE_EFF_DT
,	PPRE_END_DT
,	RT_ELEM_TYP_CD
,	RT_ELEM_TYP_NM
,	RT_ELEM_USAGE_TYP_CD
,	RT_ELEM_USAGE_TYP_DESC
,	RT_ELEM_DSPLY_TYP_CD
,	RT_ELEM_DSPLY_TYP_NM
,	PPRE_RT
,	EXPRN_MOD_TYP_CD
,	EXPRN_MOD_TYP_NM
,	EXPRN_MOD_FCTR
,	EXPRN_MOD_EFF_DT
,	EXPRN_MOD_END_DT
,	EXPRN_MOD_ANV_RT_DT
,	VOID_IND
,	ELE_RT_ELEM_TYP_CD
,	ELE_RT_ELEM_TYP_NM
,	RT_ELEM_TYP_NM_EFF_DT
,	RT_ELEM_TYP_NM_END_DT
,	ELE_RT_ELEM_USAGE_TYP_CD
,	RT_ELEM_USAGE_TYP_NM
,	RT_ELEM_TYP_RLT
,	POL_PLCY_NO
,	AGRE_ID
,	CUST_NO
,	PLCY_ORIG_DT
,	POLICY_TYPE_CODE
,	POLICY_TYPE_DESC
,	PT_PLCY_PRD_ID
,	PT_CTL_ELEM_TYP_CD
,	PT_PLCY_CTL_ELEM_EFF_DT
,	PT_PLCY_CTL_ELEM_END_DT
,	PT_VOID_IND
,	PAYMENT_PLAN_CODE
,	PAYMENT_PLAN_DESC
,	PP_PLCY_PRD_ID
,	PP_CTL_ELEM_TYP_CD
,	PP_VOID_IND
,	PP_PLCY_CTL_ELEM_EFF_DT
,	PP_PLCY_CTL_ELEM_END_DT
,   PLCY_PRD_EFF_DT
,	PLCY_PRD_END_DT
,	POLICY_PERIOD_DESC
,	NEW_POLICY_IND
,	PEC_POLICY_IND
,	LEASE_TYPE_CODE
,	LEASE_TYPE_DESC
,	LS_PLCY_PRD_ID
,	LS_CTL_ELEM_TYP_CD
,	LS_VOID_IND
,	LS_PLCY_CTL_ELEM_EFF_DT
,	LS_PLCY_CTL_ELEM_END_DT
from RATING