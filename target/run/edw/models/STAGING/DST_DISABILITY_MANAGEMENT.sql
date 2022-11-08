

      create or replace  table DEV_EDW.STAGING.DST_DISABILITY_MANAGEMENT  as
      (---- SRC LAYER ----
WITH
SRC_DIS as ( SELECT *     from     STAGING.STG_CLAIM_DISABILITY_MANAGEMENT ),
SRC_CR_USER as ( SELECT *     from     STAGING.STG_USERS ),
SRC_UP_USER as ( SELECT *     from     STAGING.STG_USERS ),
//SRC_DIS as ( SELECT *     from     STG_CLAIM_DISABILITY_MANAGEMENT) ,
//SRC_CR_USER as ( SELECT *     from     STG_USERS) ,
//SRC_UP_USER as ( SELECT *     from     STG_USERS) ,

---- LOGIC LAYER ----

LOGIC_DIS as ( SELECT 
		  CLM_DISAB_MANG_ID                                  as                                  CLM_DISAB_MANG_ID 
		,  TRIM( CLM_NO )                                     as                                            CLM_NO 
		, TRIM( CLM_DISAB_MANG_DISAB_TYP_CD )                as                        CLM_DISAB_MANG_DISAB_TYP_CD 
		, TRIM( CLM_DISAB_MANG_RSN_TYP_CD )                  as                          CLM_DISAB_MANG_RSN_TYP_CD 
		, TRIM( CLM_DISAB_MANG_MED_STS_TYP_CD )              as                      CLM_DISAB_MANG_MED_STS_TYP_CD 
		, TRIM( CLM_DISAB_MANG_WK_STS_TYP_CD )               as                       CLM_DISAB_MANG_WK_STS_TYP_CD 
        , case when CLM_DISAB_MANG_END_DT is null then 'Y' else 'N' end   as         CURRENT_DISABILITY_STATUS_IND
		, CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT 
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT 
		, CLM_DISAB_ACTL_ELPS_DD                             as                             CLM_DISAB_ACTL_ELPS_DD 
		, CLM_DISAB_CAL_ELPS_DD                              as                              CLM_DISAB_CAL_ELPS_DD
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		from SRC_DIS
            ),
LOGIC_CR_USER as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_CR_USER
            ),
LOGIC_UP_USER as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		from SRC_UP_USER
            )

---- RENAME LAYER ----
,

RENAME_DIS as ( SELECT 
		  CLM_DISAB_MANG_ID                                  as                                  CLM_DISAB_MANG_ID
		, CLM_NO                                             as                                             CLM_NO
		, CLM_DISAB_MANG_DISAB_TYP_CD                        as                        CLM_DISAB_MANG_DISAB_TYP_CD
		, CLM_DISAB_MANG_RSN_TYP_CD                          as                          CLM_DISAB_MANG_RSN_TYP_CD
		, CLM_DISAB_MANG_MED_STS_TYP_CD                      as                      CLM_DISAB_MANG_MED_STS_TYP_CD
		, CLM_DISAB_MANG_WK_STS_TYP_CD                       as                       CLM_DISAB_MANG_WK_STS_TYP_CD
		,CURRENT_DISABILITY_STATUS_IND                       as                      CURRENT_DISABILITY_STATUS_IND
		, CLM_DISAB_MANG_EFF_DT                              as                              CLM_DISAB_MANG_EFF_DT
		, CLM_DISAB_MANG_END_DT                              as                              CLM_DISAB_MANG_END_DT
		, CLM_DISAB_ACTL_ELPS_DD                             as                             CLM_DISAB_ACTL_ELPS_DD
		, CLM_DISAB_CAL_ELPS_DD                              as                              CLM_DISAB_CAL_ELPS_DD
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, VOID_IND                                           as                                           VOID_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_DIS   ), 
RENAME_CR_USER as ( SELECT 
		  USER_ID                                            as                                         CR_USER_ID
		, USER_LGN_NM                                        as                                     CR_USER_LGN_NM 
				FROM     LOGIC_CR_USER   ), 
RENAME_UP_USER as ( SELECT 
		  USER_ID                                            as                                         UP_USER_ID
		, USER_LGN_NM                                        as                                     UP_USER_LGN_NM 
				FROM     LOGIC_UP_USER   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_DIS                            as ( SELECT * from    RENAME_DIS 
                                            WHERE VOID_IND = 'N' AND CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_CR_USER                        as ( SELECT * from    RENAME_CR_USER   ),
FILTER_UP_USER                        as ( SELECT * from    RENAME_UP_USER   ),

---- JOIN LAYER ----

DIS as ( SELECT * 
				FROM  FILTER_DIS
				LEFT JOIN FILTER_CR_USER ON  FILTER_DIS.AUDIT_USER_ID_CREA =  FILTER_CR_USER.CR_USER_ID 
				LEFT JOIN FILTER_UP_USER ON  FILTER_DIS.AUDIT_USER_ID_UPDT =  FILTER_UP_USER.UP_USER_ID  )
SELECT 
  CLM_DISAB_MANG_ID
, CLM_NO
, CLM_DISAB_MANG_DISAB_TYP_CD
, CLM_DISAB_MANG_RSN_TYP_CD
, CLM_DISAB_MANG_MED_STS_TYP_CD
, CLM_DISAB_MANG_WK_STS_TYP_CD
, CURRENT_DISABILITY_STATUS_IND
, CLM_DISAB_MANG_EFF_DT
, CLM_DISAB_MANG_END_DT
, CLM_DISAB_ACTL_ELPS_DD
, CLM_DISAB_CAL_ELPS_DD
, VOID_IND
, AUDIT_USER_ID_CREA
, AUDIT_USER_ID_UPDT
, AUDIT_USER_CREA_DTM
, AUDIT_USER_UPDT_DTM
, CR_USER_ID
, CR_USER_LGN_NM
, UP_USER_ID
, UP_USER_LGN_NM
from DIS
      );
    