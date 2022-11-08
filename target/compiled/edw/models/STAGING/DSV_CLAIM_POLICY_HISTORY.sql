

---- SRC LAYER ----
WITH
SRC_CPH            as ( SELECT *     FROM     STAGING.DST_CLAIM_POLICY_HISTORY ),
//SRC_CPH            as ( SELECT *     FROM     DST_CLAIM_POLICY_HISTORY) ,

---- LOGIC LAYER ----


LOGIC_CPH as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT 
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, CLAIM_CREATE_DATE                                  as                                  CLAIM_CREATE_DATE 
		, CLM_PLCY_RLTNS_EFF_DATE                            as                            CLM_PLCY_RLTNS_EFF_DATE 
		, CLM_PLCY_RLTNS_END_DATE                            as                            CLM_PLCY_RLTNS_END_DATE 
		, PLCY_NO                                            as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID 
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM 
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND 
		FROM SRC_CPH
            )

---- RENAME LAYER ----
,

RENAME_CPH        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, CLAIM_CREATE_DATE                                  as                                  CLAIM_CREATE_DATE
		, CLM_PLCY_RLTNS_EFF_DATE                            as                            CLM_PLCY_RLTNS_EFF_DATE
		, CLM_PLCY_RLTNS_END_DATE                            as                            CLM_PLCY_RLTNS_END_DATE
		, PLCY_NO                                            as                                            PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND 
				FROM     LOGIC_CPH   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CPH                            as ( SELECT * FROM    RENAME_CPH   ),

---- JOIN LAYER ----

 JOIN_CPH         as  ( SELECT * 
				FROM  FILTER_CPH )
 SELECT * FROM  JOIN_CPH