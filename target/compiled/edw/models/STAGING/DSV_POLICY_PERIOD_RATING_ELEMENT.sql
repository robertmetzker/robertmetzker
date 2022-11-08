


---- SRC LAYER ----
WITH
SRC_PP as ( SELECT *     from     STAGING.DST_POLICY_PERIOD_RATING_ELEMENT ),
//SRC_PP as ( SELECT *     from     DST_POLICY_PERIOD_RATING_ELEMENT) ,

---- LOGIC LAYER ----

LOGIC_PP as ( SELECT 
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
		, ELE_RT_ELEM_TYP_CD                                 as                                 ELE_RT_ELEM_TYP_CD 
		, ELE_RT_ELEM_TYP_NM                                 as                                 ELE_RT_ELEM_TYP_NM
		, RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT 
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT 
		, ELE_RT_ELEM_USAGE_TYP_CD                           as                           ELE_RT_ELEM_USAGE_TYP_CD 
		, RT_ELEM_USAGE_TYP_NM                               as                               RT_ELEM_USAGE_TYP_NM 
		, RT_ELEM_TYP_RLT                                    as                                    RT_ELEM_TYP_RLT 
		, CUST_NO                                            as                                            CUST_NO 
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC 
		, PAYMENT_PLAN_CODE                                  as                                  PAYMENT_PLAN_CODE 
		, PAYMENT_PLAN_DESC                                  as                                  PAYMENT_PLAN_DESC 
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE 
		, LEASE_TYPE_DESC                                    as                                    LEASE_TYPE_DESC 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC 
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND 
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
				from SRC_PP
            )

---- RENAME LAYER ----
,

RENAME_PP as ( SELECT 
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
		, ELE_RT_ELEM_TYP_CD                                 as                                 ELE_RT_ELEM_TYP_CD
		, ELE_RT_ELEM_TYP_NM                                 as                                 ELE_RT_ELEM_TYP_NM
		, RT_ELEM_TYP_NM_EFF_DT                              as                              RT_ELEM_TYP_NM_EFF_DT
		, RT_ELEM_TYP_NM_END_DT                              as                              RT_ELEM_TYP_NM_END_DT
		, ELE_RT_ELEM_USAGE_TYP_CD                           as                           ELE_RT_ELEM_USAGE_TYP_CD
		, RT_ELEM_USAGE_TYP_NM                               as                               RT_ELEM_USAGE_TYP_NM
		, RT_ELEM_TYP_RLT                                    as                                    RT_ELEM_TYP_RLT
		, CUST_NO                                            as                                            CUST_NO
		, PLCY_ORIG_DT                                       as                                       PLCY_ORIG_DT
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_TYPE_DESC                                   as                                   POLICY_TYPE_DESC
		, PAYMENT_PLAN_CODE                                  as                                  PAYMENT_PLAN_CODE
		, PAYMENT_PLAN_DESC                                  as                                  PAYMENT_PLAN_DESC
		, LEASE_TYPE_CODE                                    as                                    LEASE_TYPE_CODE
		, LEASE_TYPE_DESC                                    as                                    LEASE_TYPE_DESC 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT
		, POLICY_PERIOD_DESC                                 as                                 POLICY_PERIOD_DESC
		, NEW_POLICY_IND                                     as                                     NEW_POLICY_IND
		, PEC_POLICY_IND                                     as                                     PEC_POLICY_IND 
						FROM     LOGIC_PP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * from    RENAME_PP   ),

---- JOIN LAYER ----

 JOIN_PP  as  ( SELECT * 
				FROM  FILTER_PP )
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
,	CUST_NO
,	PLCY_ORIG_DT
,	POLICY_TYPE_CODE
,	POLICY_TYPE_DESC
,	PAYMENT_PLAN_CODE
,	PAYMENT_PLAN_DESC
,	LEASE_TYPE_CODE
,	LEASE_TYPE_DESC
,	PLCY_PRD_EFF_DT
,   PLCY_PRD_END_DT
,	POLICY_PERIOD_DESC
,	NEW_POLICY_IND
,	PEC_POLICY_IND 
  FROM  JOIN_PP