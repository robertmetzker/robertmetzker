
  create or replace  view DEV_EDW.STAGING.DSV_REPRESENTATIVE  as (
    

---- SRC LAYER ----
WITH
SRC_REP as ( SELECT *     from     STAGING.DST_REPRESENTATIVE ),
//SRC_REP as ( SELECT *     from     DST_REPRESENTATIVE) ,

---- LOGIC LAYER ----


LOGIC_REP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CUST_NO                                            as                                            CUST_NO 
		, CUST_NM_NM                                         as                                         CUST_NM_NM 
		, CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR 
		, CUST_ROLE_ID_EFF_DT                                as                                CUST_ROLE_ID_EFF_DT 
		, CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT 
		, CUST_TYP_CD                                        as                                        CUST_TYP_CD 
		, CUST_TYP_NM                                        as                                        CUST_TYP_NM 
		, CUST_1099_RECV_IND                                 as                                 CUST_1099_RECV_IND 
		, CUST_W9_RECV_IND                                   as                                   CUST_W9_RECV_IND 
		, CUST_TAX_EXMT_IND                                  as                                  CUST_TAX_EXMT_IND 
		, CUST_FRGN_CITZ_IND                                 as                                 CUST_FRGN_CITZ_IND 
		, CUST_TAX_ID_UNAVL_IND                              as                              CUST_TAX_ID_UNAVL_IND 
		, CUST_TAX_ID_OVRRD_IND                              as                              CUST_TAX_ID_OVRRD_IND 
		, CUST_INTRN_CHNL_PHN_NO                             as                             CUST_INTRN_CHNL_PHN_NO 
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND 
		, THREAT_BEHAVIOR_BLOCK_IND                          as                          THREAT_BEHAVIOR_BLOCK_IND 
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1 
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2 
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME 
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE 
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME 
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE 
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE 
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE 
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE 
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME 
		, MAILING_ADDRESS_COUNTRY_NAME                       as                       MAILING_ADDRESS_COUNTRY_NAME 
		, MAILING_ADDRESS_VALIDATED_IND                      as                      MAILING_ADDRESS_VALIDATED_IND 
		, MAILING_ADDRESS_COMMENT_TEXT                       as                       MAILING_ADDRESS_COMMENT_TEXT 
		, MAILING_ADDRESS_EFFECTIVE_DATE                     as                     MAILING_ADDRESS_EFFECTIVE_DATE 
		, MAILING_ADDRESS_END_DATE                           as                           MAILING_ADDRESS_END_DATE 
		, PHYSICAL_STREET_ADDRESS_1                          as                          PHYSICAL_STREET_ADDRESS_1 
		, PHYSICAL_STREET_ADDRESS_2                          as                          PHYSICAL_STREET_ADDRESS_2 
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME 
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE 
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME 
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE 
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE 
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME 
		, PHYSICAL_ADDRESS_COUNTRY_NAME                      as                      PHYSICAL_ADDRESS_COUNTRY_NAME 
		, PHYSICAL_ADDRESS_VALIDATED_IND                     as                     PHYSICAL_ADDRESS_VALIDATED_IND 
		, PHYSICAL_ADDRESS_COMMENT_TEXT                      as                      PHYSICAL_ADDRESS_COMMENT_TEXT 
		, PHYSICAL_ADDRESS_EFFECTIVE_DATE                    as                    PHYSICAL_ADDRESS_EFFECTIVE_DATE 
		, PHYSICAL_ADDRESS_END_DATE                          as                          PHYSICAL_ADDRESS_END_DATE 
		from SRC_REP
            )

---- RENAME LAYER ----
,

RENAME_REP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CUST_NO                                            as                                    CUSTOMER_NUMBER
		, CUST_NM_NM                                         as                                      CUSTOMER_NAME
		, CUST_ROLE_ID_VAL_STR                               as                           REPRESENTATIVE_ID_NUMBER
		, CUST_ROLE_ID_EFF_DT                                as                   REPRESENTATIVE_ID_EFFECTIVE_DATE
		, CUST_ROLE_ID_END_DT                                as                         REPRESENTATIVE_ID_END_DATE
		, CUST_TYP_CD                                        as                                 CUSTOMER_TYPE_CODE
		, CUST_TYP_NM                                        as                                 CUSTOMER_TYPE_DESC
		, CUST_1099_RECV_IND                                 as                                  "1099_RECEIVER_IND"
		, CUST_W9_RECV_IND                                   as                                   W9_RECVEIVER_IND
		, CUST_TAX_EXMT_IND                                  as                                     TAX_EXEMPT_IND
		--, CUST_FRGN_CITZ_IND                                 as                                FOREIGN_CITIZEN_IND
		, CUST_TAX_ID_UNAVL_IND                              as                             TAX_ID_UNAVAILABLE_IND
		, CUST_TAX_ID_OVRRD_IND                              as                                TAX_ID_OVERRIDE_IND
		, CUST_FRGN_CITZ_IND                                 as                                FOREIGN_CITIZEN_IND
		, CUST_INTRN_CHNL_PHN_NO                             as                             CUST_INTRN_CHNL_PHN_NO
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND
		, THREAT_BEHAVIOR_BLOCK_IND                          as                          THREAT_BEHAVIOR_BLOCK_IND
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME
		, MAILING_ADDRESS_COUNTRY_NAME                       as                       MAILING_ADDRESS_COUNTRY_NAME
		, MAILING_ADDRESS_VALIDATED_IND                      as                      MAILING_ADDRESS_VALIDATED_IND
		, MAILING_ADDRESS_COMMENT_TEXT                       as                       MAILING_ADDRESS_COMMENT_TEXT
		, MAILING_ADDRESS_EFFECTIVE_DATE                     as                     MAILING_ADDRESS_EFFECTIVE_DATE
		, MAILING_ADDRESS_END_DATE                           as                           MAILING_ADDRESS_END_DATE
		, PHYSICAL_STREET_ADDRESS_1                          as                          PHYSICAL_STREET_ADDRESS_1
		, PHYSICAL_STREET_ADDRESS_2                          as                          PHYSICAL_STREET_ADDRESS_2
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME
		, PHYSICAL_ADDRESS_COUNTRY_NAME                      as                      PHYSICAL_ADDRESS_COUNTRY_NAME
		, PHYSICAL_ADDRESS_VALIDATED_IND                     as                     PHYSICAL_ADDRESS_VALIDATED_IND
		, PHYSICAL_ADDRESS_COMMENT_TEXT                      as                      PHYSICAL_ADDRESS_COMMENT_TEXT
		, PHYSICAL_ADDRESS_EFFECTIVE_DATE                    as                    PHYSICAL_ADDRESS_EFFECTIVE_DATE
		, PHYSICAL_ADDRESS_END_DATE                          as                          PHYSICAL_ADDRESS_END_DATE 
				FROM     LOGIC_REP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_REP                            as ( SELECT * from    RENAME_REP   ),

---- JOIN LAYER ----

 JOIN_REP  as  ( SELECT * 
				FROM  FILTER_REP )
 SELECT * FROM  JOIN_REP
  );
