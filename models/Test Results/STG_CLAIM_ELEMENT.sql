

---- SRC LAYER ----
WITH

SRC_C              as ( SELECT *     FROM     {{ ref( 'STG_CLAIM' ) }} ),
SRC_CAN            as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_ALIAS_NUMBER' ) }} ),
SRC_FF             as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_PROFILE' ) }} ),
SRC_SB             as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_PROFILE' ) }} ),
SRC_PREM           as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_PROFILE' ) }} ),
SRC_KE             as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_PROFILE' ) }} ),
SRC_KT             as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_PROFILE' ) }} ),
SRC_KR             as ( SELECT *     FROM     {{ ref( 'STG_CLAIM_PROFILE' ) }} ),

/*
SRC_C              as ( SELECT *     FROM     STAGING.STG_CLAIM ),
SRC_CAN            as ( SELECT *     FROM     STAGING.STG_CLAIM_ALIAS_NUMBER ),
SRC_FF             as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE ),
SRC_SB             as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE ),
SRC_PREM           as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE ),
SRC_KE             as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE ),
SRC_KT             as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE ),
SRC_KR             as ( SELECT *     FROM     STAGING.STG_CLAIM_PROFILE ),

*/

---- LOGIC LAYER ----


, LOGIC_C as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_NO ),'' )                        as                                             CLM_NO	, 
		 NULLIF( TRIM( CLM_REL_SNPSHT_IND ),'' )            as                                 CLM_REL_SNPSHT_IND	, 
		 NULLIF( TRIM( CLM_TYP_CD ),'' )                    as                                         CLM_TYP_CD	, 
		 NULLIF( TRIM( CLM_TYP_NM ),'' )                    as                                         CLM_TYP_NM	, 
		 NULLIF( TRIM( OCCR_SRC_TYP_CD ),'' )               as                                    OCCR_SRC_TYP_CD	, 
		 NULLIF( TRIM( OCCR_SRC_TYP_NM ),'' )               as                                    OCCR_SRC_TYP_NM	, 
		 NULLIF( TRIM( OCCR_MEDA_TYP_CD ),'' )              as                                   OCCR_MEDA_TYP_CD	, 
		 NULLIF( TRIM( OCCR_MEDA_TYP_NM ),'' )              as                                   OCCR_MEDA_TYP_NM
	 from SRC_C )

, LOGIC_CAN as ( 
	SELECT 	  
		 NULLIF( TRIM( CLM_ALIAS_NO_NO ),'' )               as                                    CLM_ALIAS_NO_NO	, 
		 IF CLM_ALIAS_NO_NO IS NOT NULL, THEN 'Y' ELSE 'N'  as                                 COMBINED_CLAIM_IND	, 
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_ALIAS_NO_TYP_CD ),'' )           as                                CLM_ALIAS_NO_TYP_CD	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND
	 from SRC_CAN )

, LOGIC_FF as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_PRFL_ANSW_TEXT ),'' )            as                                 CLM_PRFL_ANSW_TEXT	, 
		 PRFL_STMT_ID                                       as                                       PRFL_STMT_ID	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	 from SRC_FF )

, LOGIC_SB as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_PRFL_ANSW_TEXT ),'' )            as                                 CLM_PRFL_ANSW_TEXT	, 
		 PRFL_STMT_ID                                       as                                       PRFL_STMT_ID	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	 from SRC_SB )

, LOGIC_PREM as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_PRFL_ANSW_TEXT ),'' )            as                                 CLM_PRFL_ANSW_TEXT	, 
		 PRFL_STMT_ID                                       as                                       PRFL_STMT_ID	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	 from SRC_PREM )

, LOGIC_KE as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_PRFL_ANSW_TEXT ),'' )            as                                 CLM_PRFL_ANSW_TEXT	, 
		 PRFL_STMT_ID                                       as                                       PRFL_STMT_ID	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	 from SRC_KE )

, LOGIC_KT as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_PRFL_ANSW_TEXT ),'' )            as                                 CLM_PRFL_ANSW_TEXT	, 
		 PRFL_STMT_ID                                       as                                       PRFL_STMT_ID	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	 from SRC_KT )

, LOGIC_KR as ( 
	SELECT 	  
		 AGRE_ID                                            as                                            AGRE_ID	, 
		 NULLIF( TRIM( CLM_PRFL_ANSW_TEXT ),'' )            as                                 CLM_PRFL_ANSW_TEXT	, 
		 NULLIF( TRIM( PRFL_SEL_VAL_TYP_NM ),'' )           as                                PRFL_SEL_VAL_TYP_NM	, 
		 PRFL_STMT_ID                                       as                                       PRFL_STMT_ID	, 
		 NULLIF( TRIM( VOID_IND ),'' )                      as                                           VOID_IND	, 
		 AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM	, 
		 AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	 from SRC_KR )


---- RENAME LAYER ----


, RENAME_C as ( SELECT  
		 AGRE_ID                                            as                                            AGRE_ID , 
		 CLM_NO                                             as                                             CLM_NO , 
		 CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND , 
		 CLM_TYP_CD                                         as                                         CLM_TYP_CD , 
		 CLM_TYP_NM                                         as                                         CLM_TYP_NM , 
		 OCCR_SRC_TYP_CD                                    as                                    OCCR_SRC_TYP_CD , 
		 OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM , 
		 OCCR_MEDA_TYP_CD                                   as                                   OCCR_MEDA_TYP_CD , 
		 OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM 
		FROM LOGIC_C
            )

, RENAME_CAN as ( SELECT  
		 CLM_ALIAS_NO_NO                                    as                                    CLM_ALIAS_NO_NO , 
		 COMBINED_CLAIM_IND                                 as                                 COMBINED_CLAIM_IND , 
		 AGRE_ID                                            as                                        CAN_AGRE_ID , 
		 CLM_ALIAS_NO_TYP_CD                                as                                CLM_ALIAS_NO_TYP_CD , 
		 VOID_IND                                           as                                       CAN_VOID_IND 
		FROM LOGIC_CAN
            )

, RENAME_FF as ( SELECT  
		 AGRE_ID                                            as                                         FF_AGRE_ID , 
		 CLM_PRFL_ANSW_TEXT                                 as                             FIREFIGHTER_CANCER_IND , 
		 PRFL_STMT_ID                                       as                                    FF_PRFL_STMT_ID , 
		 VOID_IND                                           as                                        FF_VOID_IND , 
		 AUDIT_USER_CREA_DTM                                as                             FF_AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_UPDT_DTM                                as                             FF_AUDIT_USER_UPDT_DTM 
		FROM LOGIC_FF
            )

, RENAME_SB as ( SELECT  
		 AGRE_ID                                            as                                         SB_AGRE_ID , 
		 CLM_PRFL_ANSW_TEXT                                 as                                          SB223_IND , 
		 PRFL_STMT_ID                                       as                                    SB_PRFL_STMT_ID , 
		 VOID_IND                                           as                                        SB_VOID_IND , 
		 AUDIT_USER_CREA_DTM                                as                             SB_AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_UPDT_DTM                                as                             SB_AUDIT_USER_UPDT_DTM 
		FROM LOGIC_SB
            )

, RENAME_PREM as ( SELECT  
		 AGRE_ID                                            as                                       PREM_AGRE_ID , 
		 CLM_PRFL_ANSW_TEXT                                 as                              EMPLOYER_PREMISES_IND , 
		 PRFL_STMT_ID                                       as                                  PREM_PRFL_STMT_ID , 
		 VOID_IND                                           as                                      PREM_VOID_IND , 
		 AUDIT_USER_CREA_DTM                                as                           PREM_AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_UPDT_DTM                                as                           PREM_AUDIT_USER_UPDT_DTM 
		FROM LOGIC_PREM
            )

, RENAME_KE as ( SELECT  
		 AGRE_ID                                            as                                         KE_AGRE_ID , 
		 CLM_PRFL_ANSW_TEXT                                 as              EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC , 
		 PRFL_STMT_ID                                       as                                    KE_PRFL_STMT_ID , 
		 VOID_IND                                           as                                        KE_VOID_IND , 
		 AUDIT_USER_CREA_DTM                                as                             KE_AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_UPDT_DTM                                as                             KE_AUDIT_USER_UPDT_DTM 
		FROM LOGIC_KE
            )

, RENAME_KT as ( SELECT  
		 AGRE_ID                                            as                                         KT_AGRE_ID , 
		 CLM_PRFL_ANSW_TEXT                                 as                    EMPLOYER_PAID_PROGRAM_TYPE_DESC , 
		 PRFL_STMT_ID                                       as                                    KT_PRFL_STMT_ID , 
		 VOID_IND                                           as                                        KT_VOID_IND , 
		 AUDIT_USER_CREA_DTM                                as                             KT_AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_UPDT_DTM                                as                             KT_AUDIT_USER_UPDT_DTM 
		FROM LOGIC_KT
            )

, RENAME_KR as ( SELECT  
		 AGRE_ID                                            as                                         KR_AGRE_ID , 
		 CLM_PRFL_ANSW_TEXT                                 as                  EMPLOYER_PAID_PROGRAM_REASON_CODE , 
		 PRFL_SEL_VAL_TYP_NM                                as                  EMPLOYER_PAID_PROGRAM_REASON_DESC , 
		 PRFL_STMT_ID                                       as                                    KR_PRFL_STMT_ID , 
		 VOID_IND                                           as                                        KR_VOID_IND , 
		 AUDIT_USER_CREA_DTM                                as                             KR_AUDIT_USER_CREA_DTM , 
		 AUDIT_USER_UPDT_DTM                                as                             KR_AUDIT_USER_UPDT_DTM 
		FROM LOGIC_KR
            )

---- FILTER LAYER ----

FILTER_C                              as ( SELECT * FROM    RENAME_C 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'   ),
FILTER_CAN                            as ( SELECT * FROM    RENAME_CAN 
                                            WHERE CLM_ALIAS_NO_TYP_CD = 'DUPEXPRDCLM' AND CAN_VOID_IND = 'N'   ),
FILTER_FF                             as ( SELECT * FROM    RENAME_FF 
                                            WHERE FF_VOID_IND = 'N' AND FF_PRFL_STMT_ID = 6334002   ),
FILTER_SB                             as ( SELECT * FROM    RENAME_SB 
                                            WHERE SB_VOID_IND = 'N' AND SB_PRFL_STMT_ID = 6000355   ),
FILTER_PREM                           as ( SELECT * FROM    RENAME_PREM 
                                            WHERE PREM_VOID_IND = 'N' AND PREM_PRFL_STMT_ID = 6000310   ),
FILTER_KE                             as ( SELECT * FROM    RENAME_KE 
                                            WHERE KE_VOID_IND = 'N' AND KE_PRFL_STMT_ID = 6000349   ),
FILTER_KT                             as ( SELECT * FROM    RENAME_KT 
                                            WHERE KT_VOID_IND = 'N' AND KT_PRFL_STMT_ID = 6000260   ),
FILTER_KR                             as ( SELECT * FROM    RENAME_KR 
                                            WHERE KR_VOID_IND = 'N' AND KR_PRFL_STMT_ID = 6000352   )

---- JOIN LAYER ----

C as ( SELECT * 
				FROM  FILTER_C
				LEFT JOIN FILTER_CAN ON  FILTER_C.CLM_NO =  FILTER_CAN.CLM_ALIAS_NO_NO 
								LEFT JOIN FILTER_FF ON  FILTER_C.AGRE_ID =  FILTER_FF.FF_AGRE_ID 
								LEFT JOIN FILTER_SB ON  FILTER_C.AGRE_ID =  FILTER_SB.SB_AGRE_ID 
								LEFT JOIN FILTER_PREM ON  FILTER_C.AGRE_ID =  FILTER_PREM.PREM_AGRE_ID 
								LEFT JOIN FILTER_KE ON  FILTER_C.AGRE_ID =  FILTER_KE.KE_AGRE_ID 
								LEFT JOIN FILTER_KT ON  FILTER_C.AGRE_ID =  FILTER_KT.KT_AGRE_ID 
								LEFT JOIN FILTER_KR ON  FILTER_C.AGRE_ID =  FILTER_KR.KR_AGRE_ID  )
SELECT 
		  AGRE_ID
		, CLM_NO
		, CLM_TYP_CD
		, CLM_TYP_NM
		, OCCR_SRC_TYP_CD
		, OCCR_SRC_TYP_NM
		, OCCR_MEDA_TYP_CD
		, OCCR_MEDA_TYP_NM
		, CLM_ALIAS_NO_NO
		, COMBINED_CLAIM_IND
		, CAN_AGRE_ID
		, FIREFIGHTER_CANCER_IND
		, FF_AUDIT_USER_CREA_DTM
		, FF_AUDIT_USER_UPDT_DTM
		, SB223_IND
		, SB_AUDIT_USER_CREA_DTM
		, SB_AUDIT_USER_UPDT_DTM
		, EMPLOYER_PREMISES_IND
		, PREM_AUDIT_USER_CREA_DTM
		, PREM_AUDIT_USER_UPDT_DTM
		, EMPLOYER_PAID_PROGRAM_ENROLLMENT_DESC
		, KE_AUDIT_USER_CREA_DTM
		, KE_AUDIT_USER_UPDT_DTM
		, EMPLOYER_PAID_PROGRAM_TYPE_DESC
		, KT_AUDIT_USER_CREA_DTM
		, KT_AUDIT_USER_UPDT_DTM
		, EMPLOYER_PAID_PROGRAM_REASON_CODE
		, EMPLOYER_PAID_PROGRAM_REASON_DESC
		, KR_AUDIT_USER_CREA_DTM
		, KR_AUDIT_USER_UPDT_DTM 
FROM C