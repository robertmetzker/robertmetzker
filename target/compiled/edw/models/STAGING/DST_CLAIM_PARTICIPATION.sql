---- SRC LAYER ----
WITH
SRC_PART as ( SELECT *     from     STAGING.STG_CLAIM_PARTICIPATION ),
SRC_CR_USERS as ( SELECT *     from     STAGING.STG_USERS ),
SRC_UP_USERS as ( SELECT *     from     STAGING.STG_USERS ),
//SRC_PART as ( SELECT *     from     STG_CLAIM_PARTICIPATION) ,
//SRC_CR_USERS as ( SELECT *     from     STG_USERS) ,
//SRC_UP_USERS as ( SELECT *     from     STG_USERS) ,

---- LOGIC LAYER ----

LOGIC_PART as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, TRIM( PTCP_TYP_NM )                                as                                        PTCP_TYP_NM 
		, TRIM( CUST_NM_NM )                                 as                                         CUST_NM_NM 
		, cast( CLM_PTCP_EFF_DT as DATE )                    as                                    CLM_PTCP_EFF_DT 
		, cast( CLM_PTCP_END_DT as DATE )                    as                                    CLM_PTCP_END_DT 
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND 
		, PTCP_NOTE_IND                                      as                                      PTCP_NOTE_IND 
		, TRIM( CLM_PTCP_DTL_COMT )                          as                                  CLM_PTCP_DTL_COMT 
		, TRIM( REP_TYP_CD )                                 as                                         REP_TYP_CD 
		, TRIM( BNFCY_TYP_CD )                               as                                       BNFCY_TYP_CD 
		, TRIM( CLM_PTCP_DEP_TYP_CD )                        as                                CLM_PTCP_DEP_TYP_CD 
		, TRIM( DEP_REP_TYP_CD )                             as                                     DEP_REP_TYP_CD 
		, CLM_PTCP_INSRD_SELF_INSRD_IND                      as                      CLM_PTCP_INSRD_SELF_INSRD_IND 
		, CLM_PTCP_INSRD_CNTS_IND                            as                            CLM_PTCP_INSRD_CNTS_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, CP_VOID_IND                                        as                                        CP_VOID_IND 
		from SRC_PART
            ),
LOGIC_CR_USERS as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_CR_USERS
            ),
LOGIC_UP_USERS as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_UP_USERS
            )

---- RENAME LAYER ----
,

RENAME_PART as ( SELECT 
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
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, CP_VOID_IND                                        as                                        CP_VOID_IND 
				FROM     LOGIC_PART   ), 
RENAME_CR_USERS as ( SELECT 
		  USER_ID                                            as                                            USER_ID
		, USER_LGN_NM                                        as                                 CREATE_USER_LGN_NM 
				FROM     LOGIC_CR_USERS   ), 
RENAME_UP_USERS as ( SELECT 
		  USER_ID                                            as                                         UP_USER_ID
		, USER_LGN_NM                                        as                                 UPDATE_USER_LGN_NM 
				FROM     LOGIC_UP_USERS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PART                           as ( SELECT * from    RENAME_PART 
                                            WHERE CP_VOID_IND = 'N'  ),
FILTER_CR_USERS                       as ( SELECT * from    RENAME_CR_USERS   ),
FILTER_UP_USERS                       as ( SELECT * from    RENAME_UP_USERS   ),

---- JOIN LAYER ----

PART as ( SELECT * 
				FROM  FILTER_PART
				LEFT JOIN FILTER_CR_USERS ON  FILTER_PART.AUDIT_USER_ID_CREA =  FILTER_CR_USERS.USER_ID 
								LEFT JOIN FILTER_UP_USERS ON  FILTER_PART.AUDIT_USER_ID_UPDT =  FILTER_UP_USERS.UP_USER_ID  )
SELECT * 
from PART