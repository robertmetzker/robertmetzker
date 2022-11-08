

---- SRC LAYER ----
WITH
SRC_CIC as ( SELECT *     from     STAGING.DST_COVERED_INDIVIDUAL_CUSTOMER ),
//SRC_CIC as ( SELECT *     from     DST_COVERED_INDIVIDUAL_CUSTOMER) ,

---- LOGIC LAYER ----

LOGIC_CIC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CUST_NO                                            as                                            CUST_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, COVERED_INDIVIDUAL_NAME                            as                            COVERED_INDIVIDUAL_NAME 
		, COVERED_INDIVIDUAL_FIRST_NAME                      as                      COVERED_INDIVIDUAL_FIRST_NAME 
		, COVERED_INDIVIDUAL_LAST_NAME                       as                       COVERED_INDIVIDUAL_LAST_NAME 
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1 
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2 
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME 
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE 
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME 
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME 
		, MAILING_ADDRESS_COUNTRY_NAME                       as                       MAILING_ADDRESS_COUNTRY_NAME 
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE 
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE 
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE 
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE 
		, MAILING_ADDRESS_COMMENT_TEXT                       as                       MAILING_ADDRESS_COMMENT_TEXT 
		, MAILING_ADDRESS_VALIDATED_IND                      as                      MAILING_ADDRESS_VALIDATED_IND 
		, PHYSICAL_STREET_ADDRESS_1                          as                          PHYSICAL_STREET_ADDRESS_1 
		, PHYSICAL_STREET_ADDRESS_2                          as                          PHYSICAL_STREET_ADDRESS_2 
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME 
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE 
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME 
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME 
		, PHYSICAL_ADDRESS_COUNTRY_NAME                      as                      PHYSICAL_ADDRESS_COUNTRY_NAME 
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE 
		, PHYSICAL_ADDRESS_COMMENT_TEXT                      as                      PHYSICAL_ADDRESS_COMMENT_TEXT 
		, PHYSICAL_ADDRESS_VALIDATED_IND                     as                     PHYSICAL_ADDRESS_VALIDATED_IND 
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND 
		, HOME_PHONE_NUMBER                                  as                                  HOME_PHONE_NUMBER 
		, CELL_PHONE_NUMBER                                  as                                  CELL_PHONE_NUMBER 
		, EMAIL_ADDRESS                                      as                                      EMAIL_ADDRESS 
		, TAX_ID_NO                                          as                                          TAX_ID_NO 
		from SRC_CIC
            )

---- RENAME LAYER ----
,

RENAME_CIC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CUST_NO                                            as                 COVERED_INDIVIDUAL_CUSTOMER_NUMBER
		, CUST_ID                                            as                     COVERED_INDIVIDUAL_CUSTOMER_ID
		, COVERED_INDIVIDUAL_NAME                            as                            COVERED_INDIVIDUAL_NAME
		, COVERED_INDIVIDUAL_FIRST_NAME                      as                      COVERED_INDIVIDUAL_FIRST_NAME
		, COVERED_INDIVIDUAL_LAST_NAME                       as                       COVERED_INDIVIDUAL_LAST_NAME
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME
		, MAILING_ADDRESS_COUNTRY_NAME                       as                       MAILING_ADDRESS_COUNTRY_NAME
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE
		, MAILING_ADDRESS_COMMENT_TEXT                       as                       MAILING_ADDRESS_COMMENT_TEXT
		, MAILING_ADDRESS_VALIDATED_IND                      as                      MAILING_ADDRESS_VALIDATED_IND
		, PHYSICAL_STREET_ADDRESS_1                          as                          PHYSICAL_STREET_ADDRESS_1
		, PHYSICAL_STREET_ADDRESS_2                          as                          PHYSICAL_STREET_ADDRESS_2
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME
		, PHYSICAL_ADDRESS_COUNTRY_NAME                      as                      PHYSICAL_ADDRESS_COUNTRY_NAME
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
		, PHYSICAL_ADDRESS_COMMENT_TEXT                      as                      PHYSICAL_ADDRESS_COMMENT_TEXT
		, PHYSICAL_ADDRESS_VALIDATED_IND                     as                     PHYSICAL_ADDRESS_VALIDATED_IND
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND
		, HOME_PHONE_NUMBER                                  as                                  HOME_PHONE_NUMBER
		, CELL_PHONE_NUMBER                                  as                                  CELL_PHONE_NUMBER
		, EMAIL_ADDRESS                                      as                                      EMAIL_ADDRESS 
		, TAX_ID_NO                                          as                                      TAX_ID_NUMBER 
				FROM     LOGIC_CIC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CIC                            as ( SELECT * from    RENAME_CIC   ),

---- JOIN LAYER ----

 JOIN_CIC  as  ( SELECT * 
				FROM  FILTER_CIC )
 SELECT 
   UNIQUE_ID_KEY
, COVERED_INDIVIDUAL_CUSTOMER_NUMBER
, COVERED_INDIVIDUAL_CUSTOMER_ID
, COVERED_INDIVIDUAL_NAME
, COVERED_INDIVIDUAL_FIRST_NAME
, COVERED_INDIVIDUAL_LAST_NAME
, MAILING_STREET_ADDRESS_1
, MAILING_STREET_ADDRESS_2
, MAILING_ADDRESS_CITY_NAME
, MAILING_ADDRESS_STATE_CODE
, MAILING_ADDRESS_STATE_NAME
, MAILING_ADDRESS_COUNTY_NAME
, MAILING_ADDRESS_COUNTRY_NAME
, MAILING_ADDRESS_POSTAL_CODE
, MAILING_FORMATTED_ADDRESS_POSTAL_CODE
, MAILING_FORMATTED_ADDRESS_ZIP_CODE
, MAILING_FORMATTED_ADDRESS_ZIP4_CODE
, MAILING_ADDRESS_COMMENT_TEXT
, MAILING_ADDRESS_VALIDATED_IND
, PHYSICAL_STREET_ADDRESS_1
, PHYSICAL_STREET_ADDRESS_2
, PHYSICAL_ADDRESS_CITY_NAME
, PHYSICAL_ADDRESS_STATE_CODE
, PHYSICAL_ADDRESS_STATE_NAME
, PHYSICAL_ADDRESS_COUNTY_NAME
, PHYSICAL_ADDRESS_COUNTRY_NAME
, PHYSICAL_ADDRESS_POSTAL_CODE
, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
, PHYSICAL_ADDRESS_COMMENT_TEXT
, PHYSICAL_ADDRESS_VALIDATED_IND
, DOCUMENT_BLOCK_IND
, HOME_PHONE_NUMBER
, CELL_PHONE_NUMBER
, EMAIL_ADDRESS
, TAX_ID_NUMBER
  FROM  JOIN_CIC