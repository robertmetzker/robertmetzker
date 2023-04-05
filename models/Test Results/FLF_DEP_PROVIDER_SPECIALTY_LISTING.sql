{{ config( 
post_hook = ("
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING ADD  PRIMARY KEY (DEP_PROVIDER_HKEY,DEP_PROVIDER_SPECIALIZATION_HKEY,EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY,DEP_PROVIDER_LOCATION_HKEY);
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING ADD  CONSTRAINT DEP_PROVIDER_SPECIALTY_LISTING_PROVIDER_FK FOREIGN KEY (DEP_PROVIDER_HKEY) REFERENCES DIMENSIONS.DIM_PROVIDER (PROVIDER_HKEY);
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING ADD  CONSTRAINT DEP_PROVIDER_SPECIALTY_LISTING_PROVIDER_SPECIALIZATION_FK FOREIGN KEY (DEP_PROVIDER_SPECIALIZATION_HKEY) REFERENCES DIMENSIONS.DIM_DEP_PROVIDER_SPECIALIZATION (DEP_PROVIDER_SPECIALIZATION_HKEY);
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING ADD  CONSTRAINT DEP_PROVIDER_SPECIALTY_LISTING_DEP_PROVIDER_LOCATION_FK FOREIGN KEY (DEP_PROVIDER_LOCATION_HKEY) REFERENCES DIMENSIONS.DIM_DEP_PROVIDER_LOCATION (DEP_PROVIDER_LOCATION_HKEY);
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING ADD  CONSTRAINT DEP_PROVIDER_SPECIALTY_LISTING_EQUIVALENT_REQUESTED_EXAM_TYPE_FK FOREIGN KEY (EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY) REFERENCES DIMENSIONS.DIM_EXAM_TYPE (EXAM_TYPE_HKEY);
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY LOAD_DATETIME SET NOT NULL;
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY PRIMARY_SOURCE_SYSTEM SET NOT NULL;
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY PRIMARY_SOURCE_SYSTEM VARCHAR();
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY DEP_PROVIDER_HKEY SET NOT NULL;
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY DEP_PROVIDER_SPECIALIZATION_HKEY SET NOT NULL;
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY SET NOT NULL;
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY DEP_PROVIDER_LOCATION_HKEY SET NOT NULL;
ALTER TABLE EDW_STG_MEDICAL_MART.FLF_DEP_PROVIDER_SPECIALTY_LISTING MODIFY CLAIM_NUMBER SET NOT NULL;

")  
) }}

---- SRC LAYER ----
WITH

SRC_PSL            as ( SELECT *     FROM     {{ ref( 'DSV_DEP_PROVIDER_SPECIALTY_LISTING' ) }} ),
SRC_PRVDR          as ( SELECT *     FROM     {{ ref( 'DIM_PROVIDER' ) }} ),

/*
SRC_PSL            as ( SELECT *     FROM     STAGING.DSV_DEP_PROVIDER_SPECIALTY_LISTING ),
SRC_PRVDR          as ( SELECT *     FROM     EDW.DIM_PROVIDER ),

*/

---- LOGIC LAYER ----


, LOGIC_PSL as ( 
	SELECT 	  
		 CASE WHEN  nullif(array_to_string(array_construct_compact( DEP_PROVIDER_SPECIALTY_DESC, DEP_PROVIDER_EXAM_TYPE_NAME ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'DEP_PROVIDER_SPECIALTY_DESC','DEP_PROVIDER_EXAM_TYPE_NAME' ] ) }} 
				END                                         
                                                             as                   DEP_PROVIDER_SPECIALIZATION_HKEY	, 
		 CASE WHEN EXAM_TYPE_CODE is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'EXAM_TYPE_CODE' ] ) }} 
				END                                         
                                                             as                EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY	, 
		 CASE WHEN  nullif(array_to_string(array_construct_compact( ADDRESS_LINE_1, ADDRESS_LINE_2, CITY_NAME, STATE_CODE, ZIP_CODE, COUNTY_NAME, ADMINISTRATION_ADDRESS_IND, SHIPPING_ADDRESS_IND, PRIMARY_ADDRESS_IND, EXAM_LOCATION_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE {{ dbt_utils.generate_surrogate_key ( [ 'ADDRESS_LINE_1','ADDRESS_LINE_2','CITY_NAME','STATE_CODE','ZIP_CODE','COUNTY_NAME','ADMINISTRATION_ADDRESS_IND','SHIPPING_ADDRESS_IND','PRIMARY_ADDRESS_IND','EXAM_LOCATION_IND' ] ) }} 
				END                                         
                                                             as                         DEP_PROVIDER_LOCATION_HKEY	, 
		 FOR NOW PRINT THE VALUES FROM EXAM_TYPE_NAME       as     SPECIALTY_ACCEPTED_EXAM_TYPE_COMBINATION_INDEX	, 
		 PEACH_NUMBER                                       as                                       PEACH_NUMBER	, 
		 DEP_PROVIDER_SPECIALTY_DESC                        as                        DEP_PROVIDER_SPECIALTY_DESC	, 
		 DEP_PROVIDER_EXAM_TYPE_NAME                        as                        DEP_PROVIDER_EXAM_TYPE_NAME	, 
		 ADDRESS_LINE_1                                     as                                     ADDRESS_LINE_1	, 
		 ADDRESS_LINE_2                                     as                                     ADDRESS_LINE_2	, 
		 CITY_NAME                                          as                                          CITY_NAME	, 
		 STATE_CODE                                         as                                         STATE_CODE	, 
		 ZIP_CODE                                           as                                           ZIP_CODE	, 
		 COUNTY_NAME                                        as                                        COUNTY_NAME	, 
		 ADMINISTRATION_ADDRESS_IND                         as                         ADMINISTRATION_ADDRESS_IND	, 
		 SHIPPING_ADDRESS_IND                               as                               SHIPPING_ADDRESS_IND	, 
		 PRIMARY_ADDRESS_IND                                as                                PRIMARY_ADDRESS_IND	, 
		 EXAM_LOCATION_IND                                  as                                  EXAM_LOCATION_IND	, 
		 EXAM_TYPE_CODE                                     as                                     EXAM_TYPE_CODE	, 
		 EXAM_TYPE_NAME                                     as                                     EXAM_TYPE_NAME
	 from SRC_PSL )

, LOGIC_PRVDR as ( 
	SELECT 	  
		 PROVIDER_HKEY                                      as                                      PROVIDER_HKEY	, 
		 PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER	, 
		 CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
	 from SRC_PRVDR )


---- RENAME LAYER ----


, RENAME_PRVDR as ( SELECT  
		 PROVIDER_HKEY                                      as                                  DEP_PROVIDER_HKEY , 
		 PROVIDER_PEACH_NUMBER                              as                        PRVDR_PROVIDER_PEACH_NUMBER , 
		 CURRENT_RECORD_IND                                 as                           PRVDR_CURRENT_RECORD_IND 
		FROM LOGIC_PRVDR
            )

, RENAME_PSL as ( SELECT  
		 DEP_PROVIDER_SPECIALIZATION_HKEY                   as                   DEP_PROVIDER_SPECIALIZATION_HKEY , 
		 EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY                as                EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY , 
		 DEP_PROVIDER_LOCATION_HKEY                         as                         DEP_PROVIDER_LOCATION_HKEY , 
		 SPECIALTY_ACCEPTED_EXAM_TYPE_COMBINATION_INDEX     as     SPECIALTY_ACCEPTED_EXAM_TYPE_COMBINATION_INDEX , 
		 PEACH_NUMBER                                       as                                       PEACH_NUMBER , 
		 DEP_PROVIDER_SPECIALTY_DESC                        as                        DEP_PROVIDER_SPECIALTY_DESC , 
		 DEP_PROVIDER_EXAM_TYPE_NAME                        as                        DEP_PROVIDER_EXAM_TYPE_NAME , 
		 ADDRESS_LINE_1                                     as                                     ADDRESS_LINE_1 , 
		 ADDRESS_LINE_2                                     as                                     ADDRESS_LINE_2 , 
		 CITY_NAME                                          as                                          CITY_NAME , 
		 STATE_CODE                                         as                                         STATE_CODE , 
		 ZIP_CODE                                           as                                           ZIP_CODE , 
		 COUNTY_NAME                                        as                                        COUNTY_NAME , 
		 ADMINISTRATION_ADDRESS_IND                         as                         ADMINISTRATION_ADDRESS_IND , 
		 SHIPPING_ADDRESS_IND                               as                               SHIPPING_ADDRESS_IND , 
		 PRIMARY_ADDRESS_IND                                as                                PRIMARY_ADDRESS_IND , 
		 EXAM_LOCATION_IND                                  as                                  EXAM_LOCATION_IND , 
		 EXAM_TYPE_CODE                                     as                                     EXAM_TYPE_CODE , 
		 EXAM_TYPE_NAME                                     as                                     EXAM_TYPE_NAME 
		FROM LOGIC_PSL
            )

---- FILTER LAYER ----

FILTER_PSL                            as ( SELECT * FROM    RENAME_PSL    ),
FILTER_PRVDR                          as ( SELECT * FROM    RENAME_PRVDR    )

---- JOIN LAYER ----

PSL as ( SELECT * 
				FROM  FILTER_PSL
				INNER JOIN FILTER_PRVDR ON  coalesce( FILTER_PSL.PEACH_NUMBER, '99999999999') =  FILTER_PRVDR.PRVDR_PROVIDER_PEACH_NUMBER AND PRVDR_CURRENT_RECORD_IND ='Y'  )
SELECT 
		  DEP_PROVIDER_HKEY
		, DEP_PROVIDER_SPECIALIZATION_HKEY
		, EQUIVALENT_REQUESTED_EXAM_TYPE_HKEY
		, DEP_PROVIDER_LOCATION_HKEY
		, SPECIALTY_ACCEPTED_EXAM_TYPE_COMBINATION_INDEX
		, 
		 ETL PROCESSED DATE TIME                            as                                      LOAD_DATETIME
		, 
		 AUTHORITATIVE DATA SOURCE :  'DEP'                 as                              PRIMARY_SOURCE_SYSTEM 
FROM PSL