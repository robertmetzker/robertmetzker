{{ config( 
materialized = 'view' 
) }}

---- SRC LAYER ----
WITH
SRC_CUST           as ( SELECT *     FROM     {{ ref( 'DST_CUSTOMER' ) }} ),
//SRC_CUST           as ( SELECT *     FROM     DST_CUSTOMER) ,

---- LOGIC LAYER ----


LOGIC_CUST as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CUST_NO                                            as                                            CUST_NO 
		, ROLE_ID_NUMBER                                     as                                     ROLE_ID_NUMBER 
		, ROLE_ID_NUMBER_TYPE                                as                                ROLE_ID_NUMBER_TYPE 
		, CUST_NM_NM                                         as                                         CUST_NM_NM 
		, CUST_NM_FST                                        as                                        CUST_NM_FST 
		, CUST_NM_MID                                        as                                        CUST_NM_MID 
		, CUST_NM_LST                                        as                                        CUST_NM_LST 
		, CUST_NM_SFX_TYP_NM                                 as                                 CUST_NM_SFX_TYP_NM 
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD 
		, TAX_ID_TYP_NM                                      as                                      TAX_ID_TYP_NM 
		, TAX_ID_NO                                          as                                          TAX_ID_NO 
		, PHONE_NUMBER                                       as                                       PHONE_NUMBER 
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND 
		, PHYSICAL_STREET_ADDRESS_1                          as                          PHYSICAL_STREET_ADDRESS_1 
		, PHYSICAL_STREET_ADDRESS_2                          as                          PHYSICAL_STREET_ADDRESS_2 
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME 
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE 
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME 
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE 
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME 
		, PHYSICAL_ADDRESS_COUNTRY_NAME                      as                      PHYSICAL_ADDRESS_COUNTRY_NAME 
		, PHYSICAL_ADDRESS_VALIDATED_IND                     as                     PHYSICAL_ADDRESS_VALIDATED_IND 
		, PHYSICAL_ADDRESS_COMMENT_TEXT                      as                      PHYSICAL_ADDRESS_COMMENT_TEXT 
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE 
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1 
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2 
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME 
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE 
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME 
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE 
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME 
		, MAILING_ADDRESS_COUNTRY_NAME                       as                       MAILING_ADDRESS_COUNTRY_NAME 
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE 
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE 
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE 
		, MAILING_ADDRESS_VALIDATED_IND                      as                      MAILING_ADDRESS_VALIDATED_IND 
		, MAILING_ADDRESS_COMMENT_TEXT                       as                       MAILING_ADDRESS_COMMENT_TEXT 
		, EMPLOYER_IND                                       as                                       EMPLOYER_IND 
		, LEGAL_REPRESENTATIVE_IND                           as                           LEGAL_REPRESENTATIVE_IND 
		, INJURED_WORKER_IND                                 as                                 INJURED_WORKER_IND 
		, COVERED_INDIVIDUAL_IND                             as                             COVERED_INDIVIDUAL_IND 
		, MISC_CUST_IND                                      as                                      MISC_CUST_IND 
		FROM SRC_CUST
            )

---- RENAME LAYER ----
,

RENAME_CUST       as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CUST_NO                                            as                                    CUSTOMER_NUMBER
		, ROLE_ID_NUMBER                                     as                                     ROLE_ID_NUMBER
		, ROLE_ID_NUMBER_TYPE                                as                                ROLE_ID_NUMBER_TYPE
		, CUST_NM_NM                                         as                                      CUSTOMER_NAME
		, CUST_NM_FST                                        as                                         FIRST_NAME
		, CUST_NM_MID                                        as                                        MIDDLE_NAME
		, CUST_NM_LST                                        as                                          LAST_NAME
		, CUST_NM_SFX_TYP_NM                                 as                                        SUFFIX_NAME
		, TAX_ID_TYP_CD                                      as                                   TAX_ID_TYPE_CODE
		, TAX_ID_TYP_NM                                      as                                   TAX_ID_TYPE_DESC
		, TAX_ID_NO                                          as                                      TAX_ID_NUMBER
		, PHONE_NUMBER                                       as                                       PHONE_NUMBER
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND
		, PHYSICAL_STREET_ADDRESS_1                          as                            PHYSICAL_ADDRESS_LINE_1
		, PHYSICAL_STREET_ADDRESS_2                          as                            PHYSICAL_ADDRESS_LINE_2
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME
		, PHYSICAL_ADDRESS_COUNTRY_NAME                      as                      PHYSICAL_ADDRESS_COUNTRY_NAME
		, PHYSICAL_ADDRESS_VALIDATED_IND                     as                     PHYSICAL_ADDRESS_VALIDATED_IND
		, PHYSICAL_ADDRESS_COMMENT_TEXT                      as                      PHYSICAL_ADDRESS_COMMENT_TEXT
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME
		, MAILING_ADDRESS_COUNTRY_NAME                       as                       MAILING_ADDRESS_COUNTRY_NAME
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE
		, MAILING_ADDRESS_VALIDATED_IND                      as                      MAILING_ADDRESS_VALIDATED_IND
		, MAILING_ADDRESS_COMMENT_TEXT                       as                       MAILING_ADDRESS_COMMENT_TEXT
		, EMPLOYER_IND                                       as                                       EMPLOYER_IND
		, LEGAL_REPRESENTATIVE_IND                           as                           LEGAL_REPRESENTATIVE_IND
		, INJURED_WORKER_IND                                 as                                 INJURED_WORKER_IND
		, COVERED_INDIVIDUAL_IND                             as                             COVERED_INDIVIDUAL_IND
		, MISC_CUST_IND                                      as                                  MISCELLANEOUS_IND 
				FROM     LOGIC_CUST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CUST                           as ( SELECT * FROM    RENAME_CUST   ),

---- JOIN LAYER ----

 JOIN_CUST        as  ( SELECT * 
				FROM  FILTER_CUST )
 SELECT * FROM  JOIN_CUST