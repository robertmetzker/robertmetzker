

      create or replace  table DEV_EDW.STAGING.STG_CLAIM_POLICY_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_CPH as ( SELECT *     from     DEV_VIEWS.DW_REPORT.DW_CLAIM_POLICY_HISTORY ),
SRC_CLM as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
//SRC_CPH as ( SELECT *     from     DW_CLAIM_POLICY_HISTORY) ,
//SRC_CLM as ( SELECT *     from     CLAIM) ,

---- LOGIC LAYER ----

LOGIC_CPH as ( SELECT 
		  CLM_AGRE_ID                                        AS                                        CLM_AGRE_ID 
		, TRIM( CLM_NO )                                     AS                                             CLM_NO 
		, INS_PARTICIPANT                                    AS                                    INS_PARTICIPANT 
		, CLM_OCCR_DATE                                      AS                                      CLM_OCCR_DATE 
		, CLM_PTCP_EFF_DATE                                  AS                                  CLM_PTCP_EFF_DATE 
		, CLM_PLCY_RLTNS_EFF_DATE                            AS                            CLM_PLCY_RLTNS_EFF_DATE 
		, CLM_PLCY_RLTNS_END_DATE                            AS                            CLM_PLCY_RLTNS_END_DATE 
		, TRIM( PLCY_NO )                                    AS                                            PLCY_NO 
		, PLCY_AGRE_ID                                       AS                                       PLCY_AGRE_ID 
		, BUSN_SEQ_NO                                        AS                                        BUSN_SEQ_NO 
		, upper( TRIM( CTL_ELEM_SUB_TYP_CD ) )               AS                                CTL_ELEM_SUB_TYP_CD 
		, upper( TRIM( CTL_ELEM_SUB_TYP_NM ) )               AS                                CTL_ELEM_SUB_TYP_NM 
		, upper( CRNT_PLCY_IND )                             AS                                      CRNT_PLCY_IND 
		from SRC_CPH
            ),
LOGIC_CLM as ( SELECT 
		  cast( AUDIT_USER_CREA_DTM as DATE )                AS                                AUDIT_USER_CREA_DTM 
		, upper( CLM_REL_SNPSHT_IND )                        AS                                 CLM_REL_SNPSHT_IND 
		, AGRE_ID                                            AS                                            AGRE_ID 
		from SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CPH as ( SELECT 
		  CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, INS_PARTICIPANT                                    as                                    INS_PARTICIPANT
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, CLM_PTCP_EFF_DATE                                  as                                  CLM_PTCP_EFF_DATE
		, CLM_PLCY_RLTNS_EFF_DATE                            as                            CLM_PLCY_RLTNS_EFF_DATE
		, CLM_PLCY_RLTNS_END_DATE                            as                            CLM_PLCY_RLTNS_END_DATE
		, PLCY_NO                                            as                                            PLCY_NO
		, PLCY_AGRE_ID                                       as                                       PLCY_AGRE_ID
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND 
				FROM     LOGIC_CPH   ), 
RENAME_CLM as ( SELECT 
		  AUDIT_USER_CREA_DTM                                as                                     CLM_ENTRY_DATE
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, AGRE_ID                                            as                                            AGRE_ID 
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CPH                            as ( SELECT * from    RENAME_CPH   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM 
				WHERE CLM_REL_SNPSHT_IND = 'N'  ),

---- JOIN LAYER ----

CPH as ( SELECT * 
				FROM  FILTER_CPH
				LEFT JOIN FILTER_CLM ON  FILTER_CPH.CLM_AGRE_ID =  FILTER_CLM.AGRE_ID  )
SELECT * 
from CPH
QUALIFY(ROW_NUMBER() OVER(PARTITION BY CLM_AGRE_ID, CLM_PLCY_RLTNS_EFF_DATE ORDER BY CLM_PLCY_RLTNS_EFF_DATE DESC, CLM_PLCY_RLTNS_END_DATE DESC))=1
      );
    