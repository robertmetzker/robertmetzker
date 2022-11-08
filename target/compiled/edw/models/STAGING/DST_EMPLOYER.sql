---- SRC LAYER ----
WITH
SRC_CUST           as ( SELECT *     FROM     STAGING.STG_BUSINESS_CUSTOMER ),
SRC_BLN            as ( SELECT *     FROM     STAGING.STG_CUSTOMER_NAME ),
--SRC_DBA            as ( SELECT *     FROM     STAGING.STG_CUSTOMER_NAME ),
SRC_CL             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_LANGUAGE ),
SRC_PA             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_ADDRESS ),
--SRC_MA             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_ADDRESS ),
SRC_DB             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_BLOCK ),
SRC_PH             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
--SRC_FAX            as ( SELECT *     FROM     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
--SRC_EM             as ( SELECT *     FROM     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
SRC_CONT           as ( SELECT *     FROM     STAGING.STG_CUSTOMER_CONTACT ),
SRC_CRAH           as ( SELECT *     FROM     STAGING.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER ),
SRC_ROLE           as ( SELECT *     FROM     STAGING.STG_CUSTOMER_ROLE ),
//SRC_CUST           as ( SELECT *     FROM     STG_BUSINESS_CUSTOMER) ,
//SRC_BLN            as ( SELECT *     FROM     STG_CUSTOMER_NAME) ,
--//SRC_DBA            as ( SELECT *     FROM     STG_CUSTOMER_NAME) ,
//SRC_CL             as ( SELECT *     FROM     STG_CUSTOMER_LANGUAGE) ,
//SRC_PA             as ( SELECT *     FROM     STG_CUSTOMER_ADDRESS) ,
--//SRC_MA             as ( SELECT *     FROM     STG_CUSTOMER_ADDRESS) ,
//SRC_DB             as ( SELECT *     FROM     STG_CUSTOMER_BLOCK) ,
//SRC_PH             as ( SELECT *     FROM     STG_CUSTOMER_INTERACTION_CHANNEL) ,
--//SRC_FAX            as ( SELECT *     FROM     STG_CUSTOMER_INTERACTION_CHANNEL) ,
--//SRC_EM             as ( SELECT *     FROM     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_CONT           as ( SELECT *     FROM     STG_CUSTOMER_CONTACT) ,
//SRC_CRAH           as ( SELECT *     FROM     STG_CUSTOMER_ROLE_ACCOUNT_HOLDER) ,
//SRC_ROLE           as ( SELECT *     FROM     STG_CUSTOMER_ROLE) ,

---- LOGIC LAYER ----


LOGIC_CUST as ( SELECT 
		 md5(cast(
    
    coalesce(cast(CUST_ID as 
    varchar
), '')

 as 
    varchar
))        as                                      UNIQUE_ID_KEY 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, TRIM( CUST_TYP_CD )                                as                                        CUST_TYP_CD 
		, TRIM( CUST_TAX_EXMT_IND )                          as                                  CUST_TAX_EXMT_IND 
		, TRIM( CUST_TAX_ID_OVRRD_IND )                      as                              CUST_TAX_ID_OVRRD_IND 
		, TRIM( CUST_TAX_ID_UNAVL_IND )                      as                              CUST_TAX_ID_UNAVL_IND 
		, TRIM( CUST_1099_RECV_IND )                         as                                 CUST_1099_RECV_IND 
		, BUSN_CS_OPER_DTM                                   as                                   BUSN_CS_OPER_DTM 
		, BUSN_YR_EST_DTM                                    as                                    BUSN_YR_EST_DTM 
		, TRIM( OWNSHP_TYP_CD )                              as                                      OWNSHP_TYP_CD 
		, TRIM( OWNSHP_TYP_NM )                              as                                      OWNSHP_TYP_NM 
		, TRIM( CUST_OWNSHP_TYP_DESC )                       as                               CUST_OWNSHP_TYP_DESC 
		, CUST_OWNSHP_EFF_DT                                 as                                 CUST_OWNSHP_EFF_DT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, B_CUST_ID                                          as                                          B_CUST_ID 
		, CO_CUST_ID                                         as                                         CO_CUST_ID 
		, CUST_OWNSHP_END_DT                                 as                                 CUST_OWNSHP_END_DT 
		, TRIM( CO_VOID_IND )                                as                                        CO_VOID_IND 
		, TRIM( OT_OWNSHP_TYP_CD )                           as                                   OT_OWNSHP_TYP_CD 
		, TRIM( TAX_ID_TYP_CD )                              as                                      TAX_ID_TYP_CD 
		, TRIM( TAX_ID_NO )                                  as                                          TAX_ID_NO 
		, TRIM( TAX_ID_SEQ_NO )                              as                                      TAX_ID_SEQ_NO 
        , CASE WHEN TAX_ID_TYP_CD = 'FEIN' THEN SUBSTR(TAX_ID_NO, 1, 2) ||'-'||SUBSTR(TAX_ID_NO, 3, 7)
               WHEN TAX_ID_TYP_CD = 'SSN' THEN SUBSTR(TAX_ID_NO, 1, 3) ||'-'||SUBSTR(TAX_ID_NO, 4, 2)||'-'||SUBSTR(TAX_ID_NO, 6, 4)
                    ELSE TAX_ID_TYP_CD END                   as                              FORMATTED_FEIN_NUMBER
		FROM SRC_CUST
            ),

LOGIC_BLN as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NM_TYP_CD )                             as                                     CUST_NM_TYP_CD 
		, TRIM( CUST_NM_NM )                                 as                                         CUST_NM_NM 
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT 
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM  
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_BLN
            ),

LOGIC_DBA as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NM_TYP_CD )                             as                                     CUST_NM_TYP_CD 
		, TRIM( CUST_NM_NM )                                 as                                         CUST_NM_NM 
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT 
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM  
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_BLN
            ),

LOGIC_CL as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_LANG_PRI_IND )                          as                                  CUST_LANG_PRI_IND 
		, TRIM( LANG_TYP_NM )                                as                                        LANG_TYP_NM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_CL
            ),

LOGIC_PA as ( SELECT 
		  nullif(TRIM( CUST_ADDR_STR_1 ),'')                 as                                    CUST_ADDR_STR_1 
		, TRIM( CUST_ADDR_STR_2 )                            as                                    CUST_ADDR_STR_2 
		, TRIM( CUST_ADDR_CITY_NM )                          as                                  CUST_ADDR_CITY_NM 
		, TRIM( STT_ABRV )                                   as                                           STT_ABRV 
		, TRIM( STT_NM )                                     as                                             STT_NM 
		, TRIM( CUST_ADDR_POST_CD )                          as                                  CUST_ADDR_POST_CD
		, CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES' THEN LEFT(CUST_ADDR_POST_CD, 5) ||'-'|| RIGHT(CUST_ADDR_POST_CD, 4)
               WHEN LENGTH(CUST_ADDR_POST_CD) = 6 AND CNTRY_NM = 'CANADA'  THEN LEFT(CUST_ADDR_POST_CD, 3) ||' '|| RIGHT(CUST_ADDR_POST_CD, 3)
               ELSE CUST_ADDR_POST_CD  END                   as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES' THEN LEFT(CUST_ADDR_POST_CD, 5)
               ELSE CUST_ADDR_POST_CD END                    as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES' THEN RIGHT(CUST_ADDR_POST_CD, 4)
               ELSE NULL END                                 as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE 
		, TRIM( CUST_ADDR_CNTY_NM )                          as                                  CUST_ADDR_CNTY_NM 
		, TRIM( CNTRY_NM )                                   as                                           CNTRY_NM 
		, TRIM( CUST_ADDR_VLDT_IND )                         as                                 CUST_ADDR_VLDT_IND 
		, TRIM( CUST_ADDR_COMT )                             as                                     CUST_ADDR_COMT 
		, cast( CUST_ADDR_EFF_DATE as DATE )                 as                                 CUST_ADDR_EFF_DATE 
		, cast( DRVD_CUST_ADDR_END_DATE as DATE )            as                                 CUST_ADDR_END_DATE 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_ADDR_TYP_CD )                           as                                   CUST_ADDR_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_PA
            ),

LOGIC_MA as ( SELECT 
		  nullif(TRIM( CUST_ADDR_STR_1 ),'')                 as                                    CUST_ADDR_STR_1 
		, TRIM( CUST_ADDR_STR_2 )                            as                                    CUST_ADDR_STR_2 
		, TRIM( CUST_ADDR_CITY_NM )                          as                                  CUST_ADDR_CITY_NM 
		, TRIM( STT_ABRV )                                   as                                           STT_ABRV 
		, TRIM( STT_NM )                                     as                                             STT_NM 
		, TRIM( CUST_ADDR_POST_CD )                          as                                  CUST_ADDR_POST_CD
		, CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 THEN LEFT(CUST_ADDR_POST_CD, 5) ||'-'|| RIGHT(CUST_ADDR_POST_CD, 4)
               ELSE CUST_ADDR_POST_CD END                    as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 THEN LEFT(CUST_ADDR_POST_CD, 5)
               ELSE CUST_ADDR_POST_CD END                    as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 THEN RIGHT(CUST_ADDR_POST_CD, 4)
               ELSE NULL END                                 as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE 
		, TRIM( CUST_ADDR_CNTY_NM )                          as                                  CUST_ADDR_CNTY_NM 
		, TRIM( CNTRY_NM )                                   as                                           CNTRY_NM 
		, TRIM( CUST_ADDR_VLDT_IND )                         as                                 CUST_ADDR_VLDT_IND 
		, TRIM( CUST_ADDR_COMT )                             as                                     CUST_ADDR_COMT 
		, cast( CUST_ADDR_EFF_DATE as DATE )                 as                                 CUST_ADDR_EFF_DATE 
		, cast( DRVD_CUST_ADDR_END_DATE as DATE )            as                                 CUST_ADDR_END_DATE 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_ADDR_TYP_CD )                           as                                   CUST_ADDR_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_PA
            ),

LOGIC_DB as ( SELECT 
		  B_BLK_ID                                           as                                           B_BLK_ID 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( BLK_TYP_CD )                                 as                                         BLK_TYP_CD 
		, BLK_END_DT                                         as                                         BLK_END_DT 
		, BLK_EFF_DT                                         as                                         BLK_EFF_DT
		, B_AUDIT_USER_CREA_DTM                              as                              B_AUDIT_USER_CREA_DTM  
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_DB
            ),

LOGIC_PH as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( PHN_NO_TYP_NM )                              as                                      PHN_NO_TYP_NM 
		, TRIM( CUST_INTRN_CHNL_PRI_IND )                    as                            CUST_INTRN_CHNL_PRI_IND 
		, TRIM( CUST_INTRN_CHNL_PHN_NO )                     as                             CUST_INTRN_CHNL_PHN_NO 
		, TRIM( CUST_INTRN_CHNL_PHN_NO_EXT )                 as                         CUST_INTRN_CHNL_PHN_NO_EXT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_PH
            ),

LOGIC_FAX as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( PHN_NO_TYP_NM )                              as                                      PHN_NO_TYP_NM 
		, TRIM( CUST_INTRN_CHNL_PRI_IND )                    as                            CUST_INTRN_CHNL_PRI_IND 
		, TRIM( CUST_INTRN_CHNL_PHN_NO )                     as                             CUST_INTRN_CHNL_PHN_NO 
		, TRIM( CUST_INTRN_CHNL_PHN_NO_EXT )                 as                         CUST_INTRN_CHNL_PHN_NO_EXT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_PH
            ),

LOGIC_EM as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( INTRN_CHNL_TYP_NM )                          as                                  INTRN_CHNL_TYP_NM 
		, TRIM( CUST_INTRN_CHNL_PRI_IND )                    as                            CUST_INTRN_CHNL_PRI_IND 
		, TRIM( CUST_INTRN_CHNL_ADDR )                       as                               CUST_INTRN_CHNL_ADDR 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_PH
            ),

LOGIC_CONT as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, CUST_CNTC_EFF_DT                                   as                                   CUST_CNTC_EFF_DT 
		, CUST_CNTC_END_DT                                   as                                   CUST_CNTC_END_DT 
		, TRIM( CNTC_TYP_NM )                                as                                        CNTC_TYP_NM 
		, TRIM( CUST_CNTC_VOID_IND )                         as                                  CUST_CNTC_VOID_IND 
		, TRIM( CUST_CNTC_NM )                               as                                       CUST_CNTC_NM 
		FROM SRC_CONT
            ),

LOGIC_CRAH as ( SELECT 
		  SIC_TYP_CD                                         as                                         SIC_TYP_CD 
		, SIC_TYP_NM                                         as                                         SIC_TYP_NM 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, TRIM( ACCT_STS_TYP_CD )                            as                                    ACCT_STS_TYP_CD 
		FROM SRC_CRAH
            ),

LOGIC_ROLE as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( ROLE_TYP_CD )                                as                                        ROLE_TYP_CD 
		, TRIM( CUST_ROLE_TYP_NM )                           as                                   CUST_ROLE_TYP_NM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_ROLE
            )

---- RENAME LAYER ----
,

RENAME_CUST       as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CUST_ID                                            as                                            CUST_ID
		, CUST_NO                                            as                                            CUST_NO
		, CUST_TYP_CD                                        as                                        CUST_TYP_CD
		, CUST_TAX_EXMT_IND                                  as                                  CUST_TAX_EXMT_IND
		, CUST_TAX_ID_OVRRD_IND                              as                              CUST_TAX_ID_OVRRD_IND
		, CUST_TAX_ID_UNAVL_IND                              as                              CUST_TAX_ID_UNAVL_IND
		, CUST_1099_RECV_IND                                 as                                 CUST_1099_RECV_IND
		, BUSN_CS_OPER_DTM                                   as                                   BUSN_CS_OPER_DTM
		, BUSN_YR_EST_DTM                                    as                                    BUSN_YR_EST_DTM
		, OWNSHP_TYP_CD                                      as                                      OWNSHP_TYP_CD
		, OWNSHP_TYP_NM                                      as                                      OWNSHP_TYP_NM
		, CUST_OWNSHP_TYP_DESC                               as                               CUST_OWNSHP_TYP_DESC
		, CUST_OWNSHP_EFF_DT                                 as                                 CUST_OWNSHP_EFF_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, B_CUST_ID                                          as                                          B_CUST_ID
		, CO_CUST_ID                                         as                                         CO_CUST_ID
		, CUST_OWNSHP_END_DT                                 as                                 CUST_OWNSHP_END_DT
		, CO_VOID_IND                                        as                                        CO_VOID_IND
		, OT_OWNSHP_TYP_CD                                   as                                   OT_OWNSHP_TYP_CD 
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD
		, TAX_ID_NO                                          as                                          TAX_ID_NO
		, TAX_ID_SEQ_NO                                      as                                      TAX_ID_SEQ_NO
		, FORMATTED_FEIN_NUMBER                              as                              FORMATTED_FEIN_NUMBER 
				FROM     LOGIC_CUST   ), 
RENAME_BLN        as ( SELECT 
		  CUST_ID                                            as                                        BLN_CUST_ID
		, CUST_NM_TYP_CD                                     as                                 BLN_CUST_NM_TYP_CD
		, CUST_NM_NM                                         as                                     BLN_CUST_NM_NM
		, CUST_NM_EFF_DT                                     as                                 BLN_CUST_NM_EFF_DT
		, CUST_NM_END_DT                                     as                                 BLN_CUST_NM_END_DT
		, AUDIT_USER_CREA_DTM                                as                            BLN_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                       BLN_VOID_IND 
				FROM     LOGIC_BLN   ), 
RENAME_DBA        as ( SELECT 
		  CUST_ID                                            as                                        DBA_CUST_ID
		, CUST_NM_TYP_CD                                     as                                 DBA_CUST_NM_TYP_CD
		, CUST_NM_NM                                         as                                     DBA_CUST_NM_NM
		, CUST_NM_EFF_DT                                     as                                 DBA_CUST_NM_EFF_DT
		, CUST_NM_END_DT                                     as                                 DBA_CUST_NM_END_DT
		, AUDIT_USER_CREA_DTM                                as                            DBA_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                       DBA_VOID_IND 
				FROM     LOGIC_DBA   ), 
RENAME_CL         as ( SELECT 
		  CUST_ID                                            as                                         CL_CUST_ID
		, CUST_LANG_PRI_IND                                  as                               CL_CUST_LANG_PRI_IND
		, LANG_TYP_NM                                        as                                     CL_LANG_TYP_NM
		, VOID_IND                                           as                                        CL_VOID_IND 
				FROM     LOGIC_CL   ), 
RENAME_PA         as ( SELECT 
		  CUST_ADDR_STR_1                                    as                          PHYSICAL_STREET_ADDRESS_1
		, CUST_ADDR_STR_2                                    as                          PHYSICAL_STREET_ADDRESS_2
		, CUST_ADDR_CITY_NM                                  as                         PHYSICAL_ADDRESS_CITY_NAME
		, STT_ABRV                                           as                        PHYSICAL_ADDRESS_STATE_CODE
		, STT_NM                                             as                        PHYSICAL_ADDRESS_STATE_NAME
		, CUST_ADDR_POST_CD                                  as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
		, CUST_ADDR_CNTY_NM                                  as                       PHYSICAL_ADDRESS_COUNTY_NAME
		, CNTRY_NM                                           as                      PHYSICAL_ADDRESS_COUNTRY_NAME
		, CUST_ADDR_VLDT_IND                                 as                     PHYSICAL_ADDRESS_VALIDATED_IND
		, CUST_ADDR_COMT                                     as                      PHYSICAL_ADDRESS_COMMENT_TEXT
		, CUST_ADDR_EFF_DATE                                 as                    PHYSICAL_ADDRESS_EFFECTIVE_DATE
		, CUST_ADDR_END_DATE                                 as                          PHYSICAL_ADDRESS_END_DATE
		, CUST_ID                                            as                                         PA_CUST_ID
		, CUST_ADDR_TYP_CD                                   as                                PA_CUST_ADDR_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                             PA_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                        PA_VOID_IND 
				FROM     LOGIC_PA   ), 
RENAME_MA         as ( SELECT 
		  CUST_ADDR_STR_1                                    as                           MAILING_STREET_ADDRESS_1
		, CUST_ADDR_STR_2                                    as                           MAILING_STREET_ADDRESS_2
		, CUST_ADDR_CITY_NM                                  as                          MAILING_ADDRESS_CITY_NAME
		, STT_ABRV                                           as                         MAILING_ADDRESS_STATE_CODE
		, STT_NM                                             as                         MAILING_ADDRESS_STATE_NAME
		, CUST_ADDR_POST_CD                                  as                        MAILING_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE
		, CUST_ADDR_CNTY_NM                                  as                        MAILING_ADDRESS_COUNTY_NAME
		, CNTRY_NM                                           as                       MAILING_ADDRESS_COUNTRY_NAME
		, CUST_ADDR_VLDT_IND                                 as                      MAILING_ADDRESS_VALIDATED_IND
		, CUST_ADDR_COMT                                     as                       MAILING_ADDRESS_COMMENT_TEXT
		, CUST_ADDR_EFF_DATE                                 as                     MAILING_ADDRESS_EFFECTIVE_DATE
		, CUST_ADDR_END_DATE                                 as                           MAILING_ADDRESS_END_DATE
		, CUST_ID                                            as                                         MA_CUST_ID
		, CUST_ADDR_TYP_CD                                   as                                MA_CUST_ADDR_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                             MA_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                        MA_VOID_IND 
				FROM     LOGIC_MA   ), 
RENAME_DB         as ( SELECT 
		  B_BLK_ID                                           as                                        DB_B_BLK_ID
		, CUST_ID                                            as                                         DB_CUST_ID
		, BLK_TYP_CD                                         as                                      DB_BLK_TYP_CD
		, BLK_END_DT                                         as                                      DB_BLK_END_DT
		, BLK_EFF_DT                                         as                                      DB_BLK_EFF_DT
		, B_AUDIT_USER_CREA_DTM                              as                           DB_B_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                        DB_VOID_IND 
				FROM     LOGIC_DB   ), 
RENAME_PH         as ( SELECT 
		  CUST_ID                                            as                                         PH_CUST_ID
		, PHN_NO_TYP_NM                                      as                                   PH_PHN_NO_TYP_NM
		, CUST_INTRN_CHNL_PRI_IND                            as                         PH_CUST_INTRN_CHNL_PRI_IND
		, CUST_INTRN_CHNL_PHN_NO                             as                                       PHONE_NUMBER
		, CUST_INTRN_CHNL_PHN_NO_EXT                         as                             PHONE_EXTENSION_NUMBER
		, VOID_IND                                           as                                        PH_VOID_IND 
				FROM     LOGIC_PH   ), 
RENAME_FAX        as ( SELECT 
		  CUST_ID                                            as                                        FAX_CUST_ID
		, PHN_NO_TYP_NM                                      as                                  FAX_PHN_NO_TYP_NM
		, CUST_INTRN_CHNL_PRI_IND                            as                        FAX_CUST_INTRN_CHNL_PRI_IND
		, CUST_INTRN_CHNL_PHN_NO                             as                                         FAX_NUMBER
		, CUST_INTRN_CHNL_PHN_NO_EXT                         as                               FAX_EXTENSION_NUMBER
		, VOID_IND                                           as                                       FAX_VOID_IND 
				FROM     LOGIC_FAX   ), 
RENAME_EM         as ( SELECT 
		  CUST_ID                                            as                                         EM_CUST_ID
		, INTRN_CHNL_TYP_NM                                  as                               EM_INTRN_CHNL_TYP_NM
		, CUST_INTRN_CHNL_PRI_IND                            as                         EM_CUST_INTRN_CHNL_PRI_IND
		, CUST_INTRN_CHNL_ADDR                               as                                      EMAIL_ADDRESS
		, VOID_IND                                           as                                        EM_VOID_IND 
				FROM     LOGIC_EM   ), 
RENAME_CONT       as ( SELECT 
		  CUST_ID                                            as                                       CONT_CUST_ID
		, CUST_CNTC_EFF_DT                                   as                                   CUST_CNTC_EFF_DT
		, CUST_CNTC_END_DT                                   as                                   CUST_CNTC_END_DT
		, CNTC_TYP_NM                                        as                                        CNTC_TYP_NM
		, CUST_CNTC_VOID_IND                                 as                                      CONT_VOID_IND
		, CUST_CNTC_NM                                       as                                       CUST_CNTC_NM 
				FROM     LOGIC_CONT   ), 
RENAME_CRAH       as ( SELECT 
		  SIC_TYP_CD                                         as                                         SIC_TYP_CD
		, SIC_TYP_NM                                         as                                         SIC_TYP_NM
		, CUST_ID                                            as                                       CRAH_CUST_ID
		, VOID_IND                                           as                                      CRAH_VOID_IND
		, ACCT_STS_TYP_CD                                    as                                    ACCT_STS_TYP_CD 
				FROM     LOGIC_CRAH   ), 
RENAME_ROLE       as ( SELECT 
		  CUST_ID                                            as                                       ROLE_CUST_ID
		, ROLE_TYP_CD                                        as                                        ROLE_TYP_CD
		, CUST_ROLE_TYP_NM                                   as                                   CUST_ROLE_TYP_NM
		, VOID_IND                                           as                                      ROLE_VOID_IND 
				FROM     LOGIC_ROLE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CUST                           as ( SELECT * FROM    RENAME_CUST   ),
FILTER_ROLE                           as ( SELECT * FROM    RENAME_ROLE 
                                            WHERE ROLE_TYP_CD = 'INS' AND ROLE_VOID_IND = 'N'  ),
FILTER_BLN                            as ( SELECT * FROM    RENAME_BLN 
                                            WHERE BLN_CUST_NM_TYP_CD = 'BUSN_LEGAL_NM' AND BLN_VOID_IND = 'N' AND BLN_CUST_NM_END_DT IS NULL  
			QUALIFY (ROW_NUMBER() OVER(PARTITION BY BLN_CUST_ID ORDER BY BLN_CUST_NM_EFF_DT DESC, BLN_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_DBA                            as ( SELECT * FROM    RENAME_DBA 
                                            WHERE DBA_CUST_NM_TYP_CD = 'PRI_DBA_NM' AND DBA_VOID_IND = 'N' AND DBA_CUST_NM_END_DT IS NULL
			QUALIFY (ROW_NUMBER() OVER(PARTITION BY DBA_CUST_ID ORDER BY DBA_CUST_NM_EFF_DT DESC, DBA_AUDIT_USER_CREA_DTM DESC)) =1 ),
FILTER_CL                             as ( SELECT * FROM    RENAME_CL 
                                            WHERE CL_CUST_LANG_PRI_IND='Y' AND CL_VOID_IND = 'N'  ),
FILTER_MA                             as ( SELECT * FROM    RENAME_MA 
                                            WHERE MA_CUST_ADDR_TYP_CD = 'MAIL' AND MA_VOID_IND = 'N' AND CURRENT_DATE BETWEEN MAILING_ADDRESS_EFFECTIVE_DATE AND COALESCE(MAILING_ADDRESS_END_DATE ,'12/31/2999'::DATE)
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY MA_CUST_ID ORDER BY MAILING_ADDRESS_EFFECTIVE_DATE DESC, MA_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_PA                             as ( SELECT * FROM    RENAME_PA 
                                            WHERE PA_CUST_ADDR_TYP_CD = 'PHYS' AND PA_VOID_IND = 'N' AND CURRENT_DATE BETWEEN PHYSICAL_ADDRESS_EFFECTIVE_DATE AND COALESCE(PHYSICAL_ADDRESS_END_DATE ,'12/31/2999'::DATE)
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY PA_CUST_ID ORDER BY PHYSICAL_ADDRESS_EFFECTIVE_DATE DESC, PA_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_DB                             as ( SELECT * FROM    RENAME_DB 
                                            WHERE DB_BLK_TYP_CD in ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK') AND DB_VOID_IND = 'N' AND coalesce(DB_BLK_END_DT, '2999-12-31') >= current_date
			QUALIFY (ROW_NUMBER() OVER(PARTITION BY DB_CUST_ID ORDER BY DB_BLK_EFF_DT, DB_B_AUDIT_USER_CREA_DTM)) =1  ),
FILTER_PH                             as ( SELECT * FROM    RENAME_PH 
                                            WHERE PH_VOID_IND = 'N' AND PH_PHN_NO_TYP_NM = 'BUSINESS' AND PH_CUST_INTRN_CHNL_PRI_IND = 'Y'  ),
FILTER_FAX                            as ( SELECT * FROM    RENAME_FAX 
                                            WHERE FAX_VOID_IND = 'N' AND FAX_PHN_NO_TYP_NM = 'FAX' AND FAX_CUST_INTRN_CHNL_PRI_IND = 'Y'  ),
FILTER_EM                             as ( SELECT * FROM    RENAME_EM 
                                            WHERE EM_VOID_IND = 'N' AND EM_INTRN_CHNL_TYP_NM = 'E-MAIL' AND EM_CUST_INTRN_CHNL_PRI_IND = 'Y'  ),
FILTER_CONT                           as ( SELECT * FROM    RENAME_CONT 
                                            WHERE CONT_VOID_IND = 'N' AND CUST_CNTC_END_DT IS NULL AND CNTC_TYP_NM = 'PRIMARY'  
			QUALIFY (ROW_NUMBER() OVER(PARTITION BY CONT_CUST_ID ORDER BY CUST_CNTC_EFF_DT)) =1  ),	
FILTER_CRAH                           as ( SELECT * FROM    RENAME_CRAH 
                                            WHERE CRAH_VOID_IND = 'N' AND ACCT_STS_TYP_CD = 'ACT'  ),

---- JOIN LAYER ----

JOIN_ETL as ( SELECT * 
				FROM  FILTER_CUST
				               INNER JOIN FILTER_ROLE ON  FILTER_CUST.CUST_ID =  FILTER_ROLE.ROLE_CUST_ID 
								LEFT JOIN FILTER_BLN  ON  FILTER_CUST.CUST_ID =  FILTER_BLN.BLN_CUST_ID 
								LEFT JOIN FILTER_DBA  ON  FILTER_CUST.CUST_ID =  FILTER_DBA.DBA_CUST_ID 
								LEFT JOIN FILTER_CL   ON  FILTER_CUST.CUST_ID =  FILTER_CL.CL_CUST_ID 
								LEFT JOIN FILTER_MA   ON  FILTER_CUST.CUST_ID =  FILTER_MA.MA_CUST_ID 
								LEFT JOIN FILTER_PA   ON  FILTER_CUST.CUST_ID =  FILTER_PA.PA_CUST_ID 
								LEFT JOIN FILTER_DB   ON  FILTER_CUST.CUST_ID =  FILTER_DB.DB_CUST_ID 
								LEFT JOIN FILTER_PH   ON  FILTER_CUST.CUST_ID =  FILTER_PH.PH_CUST_ID 
								LEFT JOIN FILTER_FAX  ON  FILTER_CUST.CUST_ID =  FILTER_FAX.FAX_CUST_ID 
								LEFT JOIN FILTER_EM   ON  FILTER_CUST.CUST_ID =  FILTER_EM.EM_CUST_ID 
								LEFT JOIN FILTER_CONT ON  FILTER_CUST.CUST_ID =  FILTER_CONT.CONT_CUST_ID 
								LEFT JOIN FILTER_CRAH ON  FILTER_CUST.CUST_ID =  FILTER_CRAH.CRAH_CUST_ID  )
SELECT DISTINCT
		  UNIQUE_ID_KEY  
		, CUST_ID
		, CUST_NO
		, CUST_TYP_CD
		, CUST_TAX_EXMT_IND
		, CUST_TAX_ID_OVRRD_IND
		, CUST_TAX_ID_UNAVL_IND
		, CUST_1099_RECV_IND
		, BUSN_CS_OPER_DTM
		, BUSN_YR_EST_DTM
		, OWNSHP_TYP_CD
		, OWNSHP_TYP_NM
		, CUST_OWNSHP_TYP_DESC
		, CUST_OWNSHP_EFF_DT
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, VOID_IND
		, B_CUST_ID
		, CO_CUST_ID
		, CUST_OWNSHP_END_DT
		, CO_VOID_IND
		, OT_OWNSHP_TYP_CD
		, TAX_ID_TYP_CD
		, TAX_ID_NO
		, TAX_ID_SEQ_NO
		, FORMATTED_FEIN_NUMBER
		, BLN_CUST_ID
		, BLN_CUST_NM_TYP_CD
		, BLN_CUST_NM_NM
		, BLN_CUST_NM_EFF_DT
		, BLN_CUST_NM_END_DT
		, BLN_VOID_IND
		, DBA_CUST_ID
		, DBA_CUST_NM_TYP_CD
		, DBA_CUST_NM_NM
		, DBA_CUST_NM_EFF_DT
		, DBA_CUST_NM_END_DT
		, DBA_VOID_IND
		, CL_CUST_ID
		, CL_CUST_LANG_PRI_IND
		, CL_LANG_TYP_NM
		, CL_VOID_IND
		, PHYSICAL_STREET_ADDRESS_1
		, PHYSICAL_STREET_ADDRESS_2
		, PHYSICAL_ADDRESS_CITY_NAME
		, PHYSICAL_ADDRESS_STATE_CODE
		, PHYSICAL_ADDRESS_STATE_NAME
		, PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
		, PHYSICAL_ADDRESS_COUNTY_NAME
		, PHYSICAL_ADDRESS_COUNTRY_NAME
		, PHYSICAL_ADDRESS_VALIDATED_IND
		, PHYSICAL_ADDRESS_COMMENT_TEXT
		, PHYSICAL_ADDRESS_EFFECTIVE_DATE
		, PHYSICAL_ADDRESS_END_DATE
		, PA_CUST_ID
		, PA_CUST_ADDR_TYP_CD
		, PA_AUDIT_USER_CREA_DTM
		, PA_VOID_IND
		, MAILING_STREET_ADDRESS_1
		, MAILING_STREET_ADDRESS_2
		, MAILING_ADDRESS_CITY_NAME
		, MAILING_ADDRESS_STATE_CODE
		, MAILING_ADDRESS_STATE_NAME
		, MAILING_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE
		, MAILING_ADDRESS_COUNTY_NAME
		, MAILING_ADDRESS_COUNTRY_NAME
		, MAILING_ADDRESS_VALIDATED_IND
		, MAILING_ADDRESS_COMMENT_TEXT
		, MAILING_ADDRESS_EFFECTIVE_DATE
		, MAILING_ADDRESS_END_DATE
		, MA_CUST_ID
		, MA_CUST_ADDR_TYP_CD
		, MA_AUDIT_USER_CREA_DTM
		, MA_VOID_IND
		, CASE WHEN DB_B_BLK_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS DOCUMENT_BLOCK_IND
		, DB_CUST_ID
		, DB_BLK_TYP_CD
		, DB_BLK_END_DT
		, DB_BLK_EFF_DT
		, DB_VOID_IND
		, PH_CUST_ID
		, PH_PHN_NO_TYP_NM
		, PH_CUST_INTRN_CHNL_PRI_IND
		, PHONE_NUMBER
		, PHONE_EXTENSION_NUMBER
		, PH_VOID_IND
		, FAX_CUST_ID
		, FAX_PHN_NO_TYP_NM
		, FAX_CUST_INTRN_CHNL_PRI_IND
		, FAX_NUMBER
		, FAX_EXTENSION_NUMBER
		, FAX_VOID_IND
		, EM_CUST_ID
		, EM_INTRN_CHNL_TYP_NM
		, EM_CUST_INTRN_CHNL_PRI_IND
		, EMAIL_ADDRESS
		, EM_VOID_IND
		, CONT_CUST_ID
		, CUST_CNTC_EFF_DT
		, CUST_CNTC_END_DT
		, CNTC_TYP_NM
		, CONT_VOID_IND
		, CUST_CNTC_NM
		, SIC_TYP_CD
		, SIC_TYP_NM
		, CRAH_CUST_ID
		, CRAH_VOID_IND
		, ACCT_STS_TYP_CD
        , ROLE_CUST_ID
		, ROLE_TYP_CD
		, CUST_ROLE_TYP_NM
		, ROLE_VOID_IND 
FROM JOIN_ETL