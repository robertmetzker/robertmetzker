

---- SRC LAYER ----
WITH
SRC_CPH as ( SELECT *     from     STAGING.DSV_CLAIM_POLICY_HISTORY ),
SRC_EMP as ( SELECT *     from     EDW_STAGING_DIM.DIM_EMPLOYER ),
//SRC_CPH as ( SELECT *     from     DSV_CLAIM_POLICY_HISTORY) ,
//SRC_EMP as ( SELECT *     from     DIM_EMPLOYER) ,

---- LOGIC LAYER ----

LOGIC_CPH as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO 
		, CASE WHEN CLM_PLCY_RLTNS_EFF_DATE is null then '-1' 
			WHEN CLM_PLCY_RLTNS_EFF_DATE < '1901-01-01' then '-2' 
			WHEN CLM_PLCY_RLTNS_EFF_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_PLCY_RLTNS_EFF_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                            CLM_PLCY_RLTNS_EFF_DATE 
		, CASE WHEN CLM_PLCY_RLTNS_END_DATE is null then '-1' 
			WHEN CLM_PLCY_RLTNS_END_DATE < '1901-01-01' then '-2' 
			WHEN CLM_PLCY_RLTNS_END_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_PLCY_RLTNS_END_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                            CLM_PLCY_RLTNS_END_DATE 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CRNT_PLCY_IND, CTL_ELEM_SUB_TYP_CD ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CRNT_PLCY_IND as 
    varchar
), '') || '-' || coalesce(cast(CTL_ELEM_SUB_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                         CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY                                            
		, CASE WHEN CLAIM_CREATE_DATE is null then '-1' 
			WHEN CLAIM_CREATE_DATE < '1901-01-01' then '-2' 
			WHEN CLAIM_CREATE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLAIM_CREATE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  CLAIM_CREATE_DATE 
		, CASE WHEN CLM_OCCR_DATE is null then '-1' 
			WHEN CLM_OCCR_DATE < '1901-01-01' then '-2' 
			WHEN CLM_OCCR_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_OCCR_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                      CLM_OCCR_DATE 
		, PLCY_NO                                            as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO 
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND 
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
		from SRC_CPH
            ),
LOGIC_EMP as ( SELECT 
		  COALESCE( EMPLOYER_HKEY, MD5( '99999' ) )          as                                      EMPLOYER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_EMP
            )

---- RENAME LAYER ----
,

RENAME_CPH as ( SELECT 
		  CLM_NO                                             as                                       CLAIM_NUMBER
		, CLM_PLCY_RLTNS_EFF_DATE                            as                    RELATIONSHIP_EFFECTIVE_DATE_KEY
		, CLM_PLCY_RLTNS_END_DATE                            as                          RELATIONSHIP_END_DATE_KEY
		, CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY                as                CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY
		, CLAIM_CREATE_DATE                                  as                       SOURCE_SYSTEM_CREATE_DATE_KEY
		, CLM_OCCR_DATE                                      as                                OCCURRENCE_DATE_KEY
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, BUSN_SEQ_NO                                        as                           BUSINESS_SEQUENCE_NUMBER
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO
		, CRNT_PLCY_IND                                      as                                      CRNT_PLCY_IND
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD 
				FROM     LOGIC_CPH   ), 
RENAME_EMP as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                          EMP_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                EMP_RECORD_END_DATE 
				FROM     LOGIC_EMP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CPH                            as ( SELECT * from    RENAME_CPH   ),
FILTER_EMP                            as ( SELECT * from    RENAME_EMP   ),

---- JOIN LAYER ----

CPH as ( SELECT * 
				FROM  FILTER_CPH
				LEFT JOIN FILTER_EMP ON  coalesce( FILTER_CPH.EMP_CUST_NO, '99999') =  FILTER_EMP.CUSTOMER_NUMBER 
				          AND CURRENT_DATE  BETWEEN EMP_RECORD_EFFECTIVE_DATE 
				          AND coalesce( EMP_RECORD_END_DATE, '2099-12-31')  )
SELECT 
		  CLAIM_NUMBER
		, RELATIONSHIP_EFFECTIVE_DATE_KEY
		, RELATIONSHIP_END_DATE_KEY
		, coalesce( EMPLOYER_HKEY, MD5( '-1111' )) as EMPLOYER_HKEY
		, coalesce( CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY, MD5( '-1111' )) as CLAIM_POLICY_ASSIGNMENT_DETAIL_HKEY
		, SOURCE_SYSTEM_CREATE_DATE_KEY
		, OCCURRENCE_DATE_KEY
		, POLICY_NUMBER
		, BUSINESS_SEQUENCE_NUMBER 
        , CURRENT_TIMESTAMP AS LOAD_DATETIME
        , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
        , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 		
from CPH