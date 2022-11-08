---- SRC LAYER ----
WITH
SRC_FSC as ( SELECT *     from     STAGING.STG_FSC ),
SRC_PC as ( SELECT *     from     STAGING.STG_CPT_PAYMENT_CATEGORY ),
SRC_DEP as ( SELECT *     from     STAGING.STG_CPT_DEP_FEE_SCHEDULE ),
SRC_VR as ( SELECT *     from     STAGING.STG_CPT_VR_FEE_SCHEDULE ),
//SRC_FSC as ( SELECT *     from     STG_FSC) ,
//SRC_PC as ( SELECT *     from     STG_CPT_PAYMENT_CATEGORY) ,
//SRC_DEP as ( SELECT *     from     STG_CPT_DEP_FEE_SCHEDULE) ,
//SRC_VR as ( SELECT *     from     STG_CPT_VR_FEE_SCHEDULE) ,

---- LOGIC LAYER ----

LOGIC_FSC as ( SELECT 
		  TRIM( SERVICE_CODE )                               AS                                       SERVICE_CODE 
		, TRIM( SERVICE_MOD )                                AS                                        SERVICE_MOD 
		, TRIM( SERVICE_DESC )                               AS                                       SERVICE_DESC 
		, TRIM( SERVICE_LONG_DESC )                          AS                                  SERVICE_LONG_DESC 
		, TRIM( MOD_DESC )                                   AS                                           MOD_DESC 
		, TRIM( MCO_VALID )                                  AS                                          MCO_VALID 
		, TRIM( TYPE_OF_SVC_CODE )                           AS                                   TYPE_OF_SVC_CODE 
		, TRIM( TYPE_OF_SVC_DESC )                           AS                                   TYPE_OF_SVC_DESC 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		, FSC_RID                                            AS                                            FSC_RID 
		, TRIM( ENTRY_USER_ID )                              AS                                      ENTRY_USER_ID 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, TRIM( ULM )                                        AS                                                ULM 
		, DLM                                                AS                                                DLM 
		, conditional_change_event(service_code||nvl(service_mod,'a') ) over(partition by service_code order by nvl(service_mod,'a') desc, EXPIRATION_DATE desc) rown 
				from SRC_FSC
            qualify  rown = 0
            ),
LOGIC_PC as ( SELECT 
		  TRIM( PAYMENT_SUBCATEGORY )                        AS                                PAYMENT_SUBCATEGORY 
		, TRIM( PAYMENT_CATEGORY )                           AS                                   PAYMENT_CATEGORY 
		, TRIM( PROCEDURE_CODE )                             AS                                     PROCEDURE_CODE 
		from SRC_PC
            ),
LOGIC_DEP as ( SELECT 
		  TRIM( PROCEDURE_CODE )                             AS                                     PROCEDURE_CODE 
		, TRIM( FEE_SCHEDULE )                               AS                                       FEE_SCHEDULE 
		from SRC_DEP
            ),
LOGIC_VR as ( SELECT 
		  TRIM( PROCEDURE_CODE )                             AS                                     PROCEDURE_CODE 
		, TRIM( FEE_SCHEDULE )                               AS                                       FEE_SCHEDULE 
		from SRC_VR
            )

---- RENAME LAYER ----
,

RENAME_FSC as ( SELECT 
		  SERVICE_CODE                                       as                                       SERVICE_CODE
		, SERVICE_MOD                                        as                                        SERVICE_MOD
		, SERVICE_DESC                                       as                                       SERVICE_DESC
		, SERVICE_LONG_DESC                                  as                                  SERVICE_LONG_DESC
		, MOD_DESC                                           as                                           MOD_DESC
		, MCO_VALID                                          as                                          MCO_VALID
		, TYPE_OF_SVC_CODE                                   as                                   TYPE_OF_SVC_CODE
		, TYPE_OF_SVC_DESC                                   as                                   TYPE_OF_SVC_DESC
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, FSC_RID                                            as                                            FSC_RID
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ULM                                                as                                                ULM
		, DLM                                                as                                                DLM 
				FROM     LOGIC_FSC   ), 
RENAME_PC as ( SELECT 
		  PAYMENT_SUBCATEGORY                                as                                PAYMENT_SUBCATEGORY
		, PAYMENT_CATEGORY                                   as                          CPT_CODE_PAYMENT_CATEGORY
		, PROCEDURE_CODE                                     as                                  PC_PROCEDURE_CODE 
				FROM     LOGIC_PC   ), 
RENAME_DEP as ( SELECT 
		  PROCEDURE_CODE                                     as                                 DEP_PROCEDURE_CODE
		, FEE_SCHEDULE                                       as                                   DEP_FEE_SCHEDULE 
				FROM     LOGIC_DEP   ), 
RENAME_VR as ( SELECT 
		  PROCEDURE_CODE                                     as                                  VR_PROCEDURE_CODE
		, FEE_SCHEDULE                                       as                                    VR_FEE_SCHEDULE 
				FROM     LOGIC_VR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_FSC                            as ( SELECT * from    RENAME_FSC 
						WHERE Length(SERVICE_CODE) = 5  
						QUALIFY (row_number() over (partition by SERVICE_CODE order by EXPIRATION_DATE desc)) = 1),
FILTER_PC                             as ( SELECT * from    RENAME_PC   ),
FILTER_DEP                            as ( SELECT * from    RENAME_DEP   ),
FILTER_VR                             as ( SELECT * from    RENAME_VR   ),

---- JOIN LAYER ----

FSC as ( SELECT * 
				FROM  FILTER_FSC
				LEFT JOIN  FILTER_PC ON  FILTER_FSC.SERVICE_CODE =  FILTER_PC.PC_PROCEDURE_CODE 
								LEFT JOIN  FILTER_DEP ON  FILTER_FSC.SERVICE_CODE =  FILTER_DEP.DEP_PROCEDURE_CODE 
								LEFT JOIN  FILTER_VR ON  FILTER_FSC.SERVICE_CODE =  FILTER_VR.VR_PROCEDURE_CODE  )
--- ETL LAYER ---
, ETL_FSC AS (select 
md5(cast(
    
    coalesce(cast(SERVICE_CODE as 
    varchar
), '') || '-' || coalesce(cast(SERVICE_MOD as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
, SERVICE_CODE
, SERVICE_MOD
, SERVICE_DESC
, SERVICE_LONG_DESC
, MOD_DESC
, MCO_VALID
, TYPE_OF_SVC_CODE
, TYPE_OF_SVC_DESC
, EFFECTIVE_DATE
, NULLIF(EXPIRATION_DATE, '2099-12-31'::DATE) as EXPIRATION_DATE
, FSC_RID
, ENTRY_USER_ID
, ENTRY_DATE::DATE AS ENTRY_DATE
, ULM
, DLM::DATE AS DLM
, CASE WHEN DEP_FEE_SCHEDULE IS NOT NULL THEN DEP_FEE_SCHEDULE
		WHEN VR_FEE_SCHEDULE IS NOT NULL THEN VR_FEE_SCHEDULE
			ELSE 'PROFESSIONAL' END AS CPT_CODE_FEE_SCHEDULE
, CPT_CODE_PAYMENT_CATEGORY
, PAYMENT_SUBCATEGORY
, PC_PROCEDURE_CODE
, DEP_PROCEDURE_CODE
, DEP_FEE_SCHEDULE
, VR_PROCEDURE_CODE
, VR_FEE_SCHEDULE
 from FSC)
 SELECT * FROM ETL_FSC