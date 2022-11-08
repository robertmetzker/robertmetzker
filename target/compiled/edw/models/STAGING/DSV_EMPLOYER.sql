

---- SRC LAYER ----
WITH
SRC_EMP            as ( SELECT *     FROM     STAGING.DST_EMPLOYER ),
//SRC_EMP            as ( SELECT *     FROM     DST_EMPLOYER) ,

---- LOGIC LAYER ----


LOGIC_EMP as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CUST_NO                                            as                                            CUST_NO 
		, BLN_CUST_NM_NM                                     as                                     BLN_CUST_NM_NM 
		, CUST_1099_RECV_IND                                 as                                 CUST_1099_RECV_IND 
		, OWNSHP_TYP_NM                                      as                                      OWNSHP_TYP_NM 
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD 
		, TAX_ID_NO                                          as                                          TAX_ID_NO 
		, TAX_ID_SEQ_NO                                      as                                      TAX_ID_SEQ_NO 
		, FORMATTED_FEIN_NUMBER                              as                              FORMATTED_FEIN_NUMBER 
		, BUSN_YR_EST_DTM                                    as                                    BUSN_YR_EST_DTM 
		, BUSN_CS_OPER_DTM                                   as                                   BUSN_CS_OPER_DTM 
		, DBA_CUST_NM_NM                                     as                                     DBA_CUST_NM_NM 
		, CL_LANG_TYP_NM                                     as                                     CL_LANG_TYP_NM 
		, PHONE_NUMBER                                       as                                       PHONE_NUMBER 
		, PHONE_EXTENSION_NUMBER                             as                             PHONE_EXTENSION_NUMBER 
		, FAX_NUMBER                                         as                                         FAX_NUMBER 
		, FAX_EXTENSION_NUMBER                               as                               FAX_EXTENSION_NUMBER 
		, EMAIL_ADDRESS                                      as                                      EMAIL_ADDRESS 
		, PHYSICAL_STREET_ADDRESS_1                          as                          PHYSICAL_STREET_ADDRESS_1 
		, PHYSICAL_STREET_ADDRESS_2                          as                          PHYSICAL_STREET_ADDRESS_2 
		, PHYSICAL_ADDRESS_CITY_NAME                         as                         PHYSICAL_ADDRESS_CITY_NAME 
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE 
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME 
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE 
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                       PHYSICAL_ADDRESS_COUNTY_NAME 
		, MAILING_STREET_ADDRESS_1                           as                           MAILING_STREET_ADDRESS_1 
		, MAILING_STREET_ADDRESS_2                           as                           MAILING_STREET_ADDRESS_2 
		, MAILING_ADDRESS_CITY_NAME                          as                          MAILING_ADDRESS_CITY_NAME 
		, MAILING_ADDRESS_STATE_CODE                         as                         MAILING_ADDRESS_STATE_CODE 
		, MAILING_ADDRESS_STATE_NAME                         as                         MAILING_ADDRESS_STATE_NAME 
		, MAILING_ADDRESS_POSTAL_CODE                        as                        MAILING_ADDRESS_POSTAL_CODE 
		, MAILING_ADDRESS_COUNTY_NAME                        as                        MAILING_ADDRESS_COUNTY_NAME 
		, CUST_TAX_ID_UNAVL_IND                              as                              CUST_TAX_ID_UNAVL_IND 
		, CUST_TAX_EXMT_IND                                  as                                  CUST_TAX_EXMT_IND 
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND 
		, CUST_CNTC_NM                                       as                                       CUST_CNTC_NM 
		, SIC_TYP_CD                                         as                                         SIC_TYP_CD 
		, SIC_TYP_NM                                         as                                         SIC_TYP_NM 
		FROM SRC_EMP
            )

---- RENAME LAYER ----
,

RENAME_EMP        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CUST_NO                                            as                                    CUSTOMER_NUMBER
		, BLN_CUST_NM_NM                                     as                                      BUSINESS_NAME
		, CUST_1099_RECV_IND                                 as                                  BUSINESS_1099_IND
		, OWNSHP_TYP_NM                                      as                                     OWNERSHIP_TYPE
		, TAX_ID_TYP_CD                                      as                                        TAX_ID_TYPE
		, TAX_ID_NO                                          as                                      TAX_ID_NUMBER
		, TAX_ID_SEQ_NO                                      as                             TAX_ID_SEQUENCE_NUMBER
		, FORMATTED_FEIN_NUMBER                              as                              FORMATTED_FEIN_NUMBER
		, BUSN_YR_EST_DTM                                    as                          BUSINESS_YEAR_ESTABLISHED
		, BUSN_CS_OPER_DTM                                   as                    BUSINESS_CEASED_OPERATIONS_DATE
		, DBA_CUST_NM_NM                                     as                                   PRIMARY_DBA_NAME
		, CL_LANG_TYP_NM                                     as                  PRIMARY_LANGUAGE_TYPE_DESCRIPTION
		, PHONE_NUMBER                                       as                      PRIMARY_BUSINESS_PHONE_NUMBER
		, PHONE_EXTENSION_NUMBER                             as            PRIMARY_BUSINESS_PHONE_EXTENSION_NUMBER
		, FAX_NUMBER                                         as                        PRIMARY_BUSINESS_FAX_NUMBER
		, FAX_EXTENSION_NUMBER                               as              PRIMARY_BUSINESS_FAX_EXTENSION_NUMBER
		, EMAIL_ADDRESS                                      as                     PRIMARY_BUSINESS_EMAIL_ADDRESS
		, PHYSICAL_STREET_ADDRESS_1                          as                            PHYSICAL_ADDRESS_LINE_1
		, PHYSICAL_STREET_ADDRESS_2                          as                            PHYSICAL_ADDRESS_LINE_2
		, PHYSICAL_ADDRESS_CITY_NAME                         as                              PHYSICAL_ADDRESS_CITY
		, PHYSICAL_ADDRESS_STATE_CODE                        as                        PHYSICAL_ADDRESS_STATE_CODE
		, PHYSICAL_ADDRESS_STATE_NAME                        as                        PHYSICAL_ADDRESS_STATE_NAME
		, PHYSICAL_ADDRESS_POSTAL_CODE                       as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_ADDRESS_COUNTY_NAME                       as                            PHYSICAL_ADDRESS_COUNTY
		, MAILING_STREET_ADDRESS_1                           as                                MAIL_ADDRESS_LINE_1
		, MAILING_STREET_ADDRESS_2                           as                                MAIL_ADDRESS_LINE_2
		, MAILING_ADDRESS_CITY_NAME                          as                                  MAIL_ADDRESS_CITY
		, MAILING_ADDRESS_STATE_CODE                         as                            MAIL_ADDRESS_STATE_CODE
		, MAILING_ADDRESS_STATE_NAME                         as                            MAIL_ADDRESS_STATE_NAME
		, MAILING_ADDRESS_POSTAL_CODE                        as                           MAIL_ADDRESS_POSTAL_CODE
		, MAILING_ADDRESS_COUNTY_NAME                        as                                MAIL_ADDRESS_COUNTY
		, CUST_TAX_ID_UNAVL_IND                              as                             TAX_ID_UNAVAILABLE_IND
		, CUST_TAX_EXMT_IND                                  as                                     TAX_EXEMPT_IND
		, DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND
		, CUST_CNTC_NM                                       as                               PRIMARY_CONTACT_NAME
		, SIC_TYP_CD                                         as                                INDUSTRY_GROUP_CODE
		, SIC_TYP_NM                                         as                                INDUSTRY_GROUP_DESC 
				FROM     LOGIC_EMP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EMP                            as ( SELECT * FROM    RENAME_EMP   ),

---- JOIN LAYER ----

 JOIN_EMP         as  ( SELECT * 
				FROM  FILTER_EMP )
 SELECT 
  UNIQUE_ID_KEY
, CUSTOMER_NUMBER
, BUSINESS_NAME
, BUSINESS_1099_IND
, OWNERSHIP_TYPE
, TAX_ID_TYPE
, TAX_ID_NUMBER
, TAX_ID_SEQUENCE_NUMBER
, FORMATTED_FEIN_NUMBER
, BUSINESS_YEAR_ESTABLISHED
, BUSINESS_CEASED_OPERATIONS_DATE
, PRIMARY_DBA_NAME
, PRIMARY_LANGUAGE_TYPE_DESCRIPTION
, PRIMARY_BUSINESS_PHONE_NUMBER
, PRIMARY_BUSINESS_PHONE_EXTENSION_NUMBER
, PRIMARY_BUSINESS_FAX_NUMBER
, PRIMARY_BUSINESS_FAX_EXTENSION_NUMBER
, PRIMARY_BUSINESS_EMAIL_ADDRESS
, PHYSICAL_ADDRESS_LINE_1
, PHYSICAL_ADDRESS_LINE_2
, PHYSICAL_ADDRESS_CITY
, PHYSICAL_ADDRESS_STATE_CODE
, PHYSICAL_ADDRESS_STATE_NAME
, PHYSICAL_ADDRESS_POSTAL_CODE
, PHYSICAL_ADDRESS_COUNTY
, MAIL_ADDRESS_LINE_1
, MAIL_ADDRESS_LINE_2
, MAIL_ADDRESS_CITY
, MAIL_ADDRESS_STATE_CODE
, MAIL_ADDRESS_STATE_NAME
, MAIL_ADDRESS_POSTAL_CODE
, MAIL_ADDRESS_COUNTY
, TAX_ID_UNAVAILABLE_IND
, TAX_EXEMPT_IND
, DOCUMENT_BLOCK_IND
, PRIMARY_CONTACT_NAME
, INDUSTRY_GROUP_CODE
, INDUSTRY_GROUP_DESC
 FROM  JOIN_EMP