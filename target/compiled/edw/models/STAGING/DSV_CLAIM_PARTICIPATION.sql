

---- SRC LAYER ----
WITH
SRC_PART as ( SELECT *     from     STAGING.DST_CLAIM_PARTICIPATION ),
//SRC_PART as ( SELECT *     from     DST_CLAIM_PARTICIPATION) ,

---- LOGIC LAYER ----

LOGIC_PART as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, CUST_NO                                            as                                            CUST_NO 
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM 
		, CUST_NM_NM                                         as                                         CUST_NM_NM 
		, CLM_PTCP_EFF_DT                                    as                                    CLM_PTCP_EFF_DT 
		, CLM_PTCP_END_DT                                    as                                    CLM_PTCP_END_DT 
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND 
		, PTCP_NOTE_IND                                      as                                      PTCP_NOTE_IND 
		, CLM_PTCP_DTL_COMT                                  as                                  CLM_PTCP_DTL_COMT 
		, REP_TYP_CD                                         as                                         REP_TYP_CD 
		, BNFCY_TYP_CD                                       as                                       BNFCY_TYP_CD 
		, CLM_PTCP_DEP_TYP_CD                                as                                CLM_PTCP_DEP_TYP_CD 
		, DEP_REP_TYP_CD                                     as                                     DEP_REP_TYP_CD 
		, CLM_PTCP_INSRD_SELF_INSRD_IND                      as                      CLM_PTCP_INSRD_SELF_INSRD_IND 
		, CLM_PTCP_INSRD_CNTS_IND                            as                            CLM_PTCP_INSRD_CNTS_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, cast( AUDIT_USER_UPDT_DTM as DATE )                as                                AUDIT_USER_UPDT_DTM 
		, CREATE_USER_LGN_NM                                 as                                 CREATE_USER_LGN_NM 
		, UPDATE_USER_LGN_NM                                 as                                 UPDATE_USER_LGN_NM 
		from SRC_PART
            )

---- RENAME LAYER ----
,

RENAME_PART as ( SELECT 
		  PTCP_ID                                            as                                   PARTICIPATION_ID
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, CUST_NO                                            as                                    CUSTOMER_NUMBER
		, PTCP_TYP_CD                                        as                            PARTICIPATION_TYPE_CODE
		, PTCP_TYP_NM                                        as                            PARTICIPATION_TYPE_DESC
		, CUST_NM_NM                                         as                                      CUSTOMER_NAME
		, CLM_PTCP_EFF_DT                                    as                 CLAIM_PARTICIPATION_EFFECTIVE_DATE
		, CLM_PTCP_END_DT                                    as                       CLAIM_PARTICIPATION_END_DATE
		, CLM_PTCP_PRI_IND                                   as                          PARTICIPATION_PRIMARY_IND
		, PTCP_NOTE_IND                                      as                             PARTICIPATION_NOTE_IND
		, CLM_PTCP_DTL_COMT                                  as                                 PARTICIPATION_NOTE
		, REP_TYP_CD                                         as                                      REP_TYPE_CODE
		, BNFCY_TYP_CD                                       as                              BENEFICIARY_TYPE_CODE
		, CLM_PTCP_DEP_TYP_CD                                as                  CLAIM_PARTICIPATION_DEP_TYPE_CODE
		, DEP_REP_TYP_CD                                     as                                  DEP_REP_TYPE_CODE
		, CLM_PTCP_INSRD_SELF_INSRD_IND                      as                                   SELF_INSURED_IND
		, CLM_PTCP_INSRD_CNTS_IND                            as                             INSURED_CONTESTING_IND
		, AUDIT_USER_ID_CREA                                 as                               AUDIT_USER_ID_CREATE
		, AUDIT_USER_CREA_DTM                                as                             AUDIT_USER_CREATE_DATE
		, AUDIT_USER_ID_UPDT                                 as                               AUDIT_USER_ID_UPDATE
		, AUDIT_USER_UPDT_DTM                                as                             AUDIT_USER_UPDATE_DATE
		, CREATE_USER_LGN_NM                                 as                             CREATE_USER_LOGIN_NAME
		, UPDATE_USER_LGN_NM                                 as                             UPDATE_USER_LOGIN_NAME 
				FROM     LOGIC_PART   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PART                           as ( SELECT * from    RENAME_PART   ),

---- JOIN LAYER ----

 JOIN_PART  as  ( SELECT * 
				FROM  FILTER_PART )
 SELECT * FROM  JOIN_PART