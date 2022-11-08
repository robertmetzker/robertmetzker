

---- SRC LAYER ----
WITH
SRC_DIS as ( SELECT *     from     STAGING.DST_DISABILITY_MANAGEMENT ),
//SRC_DIS as ( SELECT *     from     DST_DISABILITY_MANAGEMENT) ,

---- LOGIC LAYER ----

LOGIC_DIS as ( SELECT 
		  CLM_DISAB_MANG_ID                                  as                                  CLM_DISAB_MANG_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                          CLM_DISAB_MANG_RSN_TYP_CD 
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                      CLM_DISAB_MANG_MED_STS_TYP_CD 
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                       CLM_DISAB_MANG_WK_STS_TYP_CD 
		, CURRENT_DISABILITY_STATUS_IND                      as                      CURRENT_DISABILITY_STATUS_IND
		, CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT 
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT 
		, CLM_DISAB_ACTL_ELPS_DD                             as                             CLM_DISAB_ACTL_ELPS_DD 
		, CLM_DISAB_CAL_ELPS_DD                              as                              CLM_DISAB_CAL_ELPS_DD 
		, VOID_IND                                           as                                           VOID_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
		, cast( AUDIT_USER_UPDT_DTM as DATE )                as                                AUDIT_USER_UPDT_DTM 
		, CR_USER_ID                                         as                                         CR_USER_ID 
		, CR_USER_LGN_NM                                     as                                     CR_USER_LGN_NM 
		, UP_USER_ID                                         as                                         UP_USER_ID 
		, UP_USER_LGN_NM                                     as                                     UP_USER_LGN_NM 
		from SRC_DIS
            )

---- RENAME LAYER ----
,

RENAME_DIS as ( SELECT 
		  CLM_DISAB_MANG_ID                                  as                           DISABILITY_MANAGEMENT_ID
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                               DISABILITY_TYPE_CODE
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                        DISABILITY_REASON_TYPE_CODE
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                DISABILITY_MEDICAL_STATUS_TYPE_CODE
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                   DISABILITY_WORK_STATUS_TYPE_CODE
		, CURRENT_DISABILITY_STATUS_IND                      as                      CURRENT_DISABILITY_STATUS_IND
		, CLM_DISAB_MANG_EFF_DT                              as                              DISABILITY_START_DATE
		, CLM_DISAB_MANG_END_DT                              as                                DISABILITY_END_DATE
		, CLM_DISAB_ACTL_ELPS_DD                             as                DISABILITY_ACTUAL_ELAPSED_DAY_COUNT
		, CLM_DISAB_CAL_ELPS_DD                              as              DISABILITY_CALENDAR_ELAPSED_DAY_COUNT
		, VOID_IND                                           as                                           VOID_IND
		, AUDIT_USER_ID_CREA                                 as                               AUDIT_USER_ID_CREATE
		, AUDIT_USER_ID_UPDT                                 as                               AUDIT_USER_ID_UPDATE
		, AUDIT_USER_CREA_DTM                                as                             AUDIT_USER_CREATE_DATE
		, AUDIT_USER_UPDT_DTM                                as                             AUDIT_USER_UPDATE_DATE
		, CR_USER_ID                                         as                                     CREATE_USER_ID
		, CR_USER_LGN_NM                                     as                             CREATE_USER_LOGIN_NAME
		, UP_USER_ID                                         as                                     UPDATE_USER_ID
		, UP_USER_LGN_NM                                     as                             UPDATE_USER_LOGIN_NAME 
				FROM     LOGIC_DIS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DIS                            as ( SELECT * from    RENAME_DIS   ),

---- JOIN LAYER ----

 JOIN_DIS  as  ( SELECT * 
				FROM  FILTER_DIS )
 SELECT 
   DISABILITY_MANAGEMENT_ID
, CLAIM_NUMBER
, DISABILITY_TYPE_CODE
, DISABILITY_REASON_TYPE_CODE
, DISABILITY_MEDICAL_STATUS_TYPE_CODE
, DISABILITY_WORK_STATUS_TYPE_CODE
, CURRENT_DISABILITY_STATUS_IND
, DISABILITY_START_DATE
, DISABILITY_END_DATE
, DISABILITY_ACTUAL_ELAPSED_DAY_COUNT
, DISABILITY_CALENDAR_ELAPSED_DAY_COUNT
, VOID_IND
, AUDIT_USER_ID_CREATE
, AUDIT_USER_ID_UPDATE
, AUDIT_USER_CREATE_DATE
, AUDIT_USER_UPDATE_DATE
, CREATE_USER_ID
, CREATE_USER_LOGIN_NAME
, UPDATE_USER_ID
, UPDATE_USER_LOGIN_NAME
 FROM  JOIN_DIS