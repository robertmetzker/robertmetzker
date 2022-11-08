

      create or replace  table DEV_EDW.STAGING.DST_COVERED_INDIVIDUAL_CUSTOMER  as
      (---- SRC LAYER ----
WITH
SRC_PPP as ( SELECT *     from     STAGING.STG_POLICY_PERIOD_PARTICIPATION ),
SRC_CN as ( SELECT *     from     STAGING.STG_CUSTOMER_NAME ),
SRC_MAIL as ( SELECT *     from     STAGING.STG_CUSTOMER_ADDRESS ),
SRC_PHYS as ( SELECT *     from     STAGING.STG_CUSTOMER_ADDRESS ),
SRC_DB as ( SELECT *     from     STAGING.STG_CUSTOMER_BLOCK ),
SRC_HP as ( SELECT *     from     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
SRC_CP as ( SELECT *     from     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
SRC_EM as ( SELECT *     from     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
SRC_TI as ( SELECT *     from     STAGING.STG_TAX_IDENTIFIER ),
//SRC_PPP as ( SELECT *     from     STG_POLICY_PERIOD_PARTICIPATION) ,
//SRC_CN as ( SELECT *     from     STG_CUSTOMER_NAME) ,
//SRC_MAIL as ( SELECT *     from     STG_CUSTOMER_ADDRESS) ,
//SRC_PHYS as ( SELECT *     from     STG_CUSTOMER_ADDRESS) ,
//SRC_DB as ( SELECT *     from     STG_CUSTOMER_BLOCK) ,
//SRC_HP as ( SELECT *     from     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_CP as ( SELECT *     from     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_EM as ( SELECT *     from     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_TI as ( SELECT *     from     STG_TAX_IDENTIFIER) ,

---- LOGIC LAYER ----

LOGIC_PPP as ( SELECT 
		  TRIM( CUST_NO )                                    as                                            CUST_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		from SRC_PPP
            ),
LOGIC_CN as ( SELECT 
		  TRIM( CUST_NM_NM )                                 as                                         CUST_NM_NM 
		, TRIM( CUST_NM_FST )                                as                                        CUST_NM_FST 
		, TRIM( CUST_NM_MID )                                as                                        CUST_NM_MID 
		, TRIM( CUST_NM_LST )                                as                                        CUST_NM_LST 
		, TRIM( CUST_NM_SFX_TYP_NM )                         as                                 CUST_NM_SFX_TYP_NM 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NM_TYP_CD )                             as                                     CUST_NM_TYP_CD 
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_CN
            ),
LOGIC_MAIL as ( SELECT 
		  TRIM( CUST_ADDR_STR_1 )                            as                                    CUST_ADDR_STR_1 
		, TRIM( CUST_ADDR_STR_2 )                            as                                    CUST_ADDR_STR_2 
		, TRIM( CUST_ADDR_CITY_NM )                          as                                  CUST_ADDR_CITY_NM 
		, TRIM( STT_ABRV )                                   as                                           STT_ABRV 
		, TRIM( STT_NM )                                     as                                             STT_NM 
		, TRIM( CUST_ADDR_CNTY_NM )                          as                                  CUST_ADDR_CNTY_NM 
		, TRIM( CNTRY_NM )                                   as                                           CNTRY_NM 
		, TRIM( CUST_ADDR_POST_CD )                          as                                  CUST_ADDR_POST_CD 
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES'
               THEN LEFT(CUST_ADDR_POST_CD, 5) ||'-'|| RIGHT(CUST_ADDR_POST_CD, 4) 
               WHEN LENGTH(CUST_ADDR_POST_CD) = 6 AND CNTRY_NM = 'CANADA' 
               THEN LEFT(CUST_ADDR_POST_CD, 3) ||' '|| RIGHT(CUST_ADDR_POST_CD, 3)
                     ELSE CUST_ADDR_POST_CD end              as               MAILING_FORMATTED_ADDRESS_POSTAL_CODE

         , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES'
                 THEN LEFT(CUST_ADDR_POST_CD, 5)
                        ELSE CUST_ADDR_POST_CD end           as 	              MAILING_FORMATTED_ADDRESS_ZIP_CODE

         , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES'
                 THEN RIGHT(CUST_ADDR_POST_CD, 4)
                         ELSE NULL END                        as 	             MAILING_FORMATTED_ADDRESS_ZIP4_CODE		
		, TRIM( CUST_ADDR_COMT )                             as                                     CUST_ADDR_COMT 
		, TRIM( CUST_ADDR_VLDT_IND )                         as                                 CUST_ADDR_VLDT_IND 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_ADDR_TYP_CD )                           as                                   CUST_ADDR_TYP_CD 
		, CUST_ADDR_END_DATE                                 as                                   CUST_ADDR_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_MAIL
        QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUST_ADDR_EFF_DATE DESC, AUDIT_USER_CREA_DTM DESC ))=1 
		    ),
LOGIC_PHYS as ( SELECT 
		  TRIM( CUST_ADDR_STR_1 )                            as                                    CUST_ADDR_STR_1 
		, TRIM( CUST_ADDR_STR_2 )                            as                                    CUST_ADDR_STR_2 
		, TRIM( CUST_ADDR_CITY_NM )                          as                                  CUST_ADDR_CITY_NM 
		, TRIM( STT_ABRV )                                   as                                           STT_ABRV 
		, TRIM( STT_NM )                                     as                                             STT_NM 
		, TRIM( CUST_ADDR_CNTY_NM )                          as                                  CUST_ADDR_CNTY_NM 
		, TRIM( CNTRY_NM )                                   as                                           CNTRY_NM 
		, TRIM( CUST_ADDR_POST_CD )                          as                                  CUST_ADDR_POST_CD 
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES'
               THEN LEFT(CUST_ADDR_POST_CD, 5) ||'-'|| RIGHT(CUST_ADDR_POST_CD, 4)
               WHEN LENGTH(CUST_ADDR_POST_CD) = 6 AND CNTRY_NM = 'CANADA' 
               THEN LEFT(CUST_ADDR_POST_CD, 3) ||' '|| RIGHT(CUST_ADDR_POST_CD, 3)
                    ELSE CUST_ADDR_POST_CD END                as 	        PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES'
               THEN LEFT(CUST_ADDR_POST_CD, 5)
                    ELSE CUST_ADDR_POST_CD END               as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
        , CASE WHEN LENGTH(CUST_ADDR_POST_CD) = 9 AND CNTRY_NM = 'UNITED STATES'
                THEN RIGHT(CUST_ADDR_POST_CD, 4)
                     ELSE NULL END                           as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE   
		, TRIM( CUST_ADDR_COMT )                             as                                     CUST_ADDR_COMT 
		, TRIM( CUST_ADDR_VLDT_IND )                         as                                 CUST_ADDR_VLDT_IND 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_ADDR_TYP_CD )                           as                                   CUST_ADDR_TYP_CD 
		, CUST_ADDR_END_DATE                                 as                                   CUST_ADDR_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_PHYS
        QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUST_ADDR_EFF_DATE DESC, AUDIT_USER_CREA_DTM DESC ))=1 
		    ),
LOGIC_DB as ( SELECT 
		  CASE WHEN B_BLK_ID IS NOT NULL THEN 'Y' else 'N' 
		  end                                                as                                 DOCUMENT_BLOCK_IND        
        , CUST_ID                                            as                                            CUST_ID 
		, TRIM( BLK_TYP_CD )                                 as                                         BLK_TYP_CD 
		, BLK_END_DT                                         as                                         BLK_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_DB
        QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY BLK_EFF_DT ))=1 
            ),
LOGIC_HP as ( SELECT 
		  TRIM( CUST_INTRN_CHNL_PHN_NO )                     as                             CUST_INTRN_CHNL_PHN_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( PHN_NO_TYP_CD )                              as                                      PHN_NO_TYP_CD 
		, TRIM( INTRN_CHNL_TYP_VOID_IND )                    as                            INTRN_CHNL_TYP_VOID_IND 
		from SRC_HP
		QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY AUDIT_USER_CREA_DTM DESC ))=1 
            ),
LOGIC_CP as ( SELECT 
		  TRIM( CUST_INTRN_CHNL_PHN_NO )                     as                             CUST_INTRN_CHNL_PHN_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( PHN_NO_TYP_CD )                              as                                      PHN_NO_TYP_CD 
		, TRIM( INTRN_CHNL_TYP_VOID_IND )                    as                            INTRN_CHNL_TYP_VOID_IND 
		from SRC_CP
        QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY AUDIT_USER_CREA_DTM DESC ))=1 
		    ),
LOGIC_EM as ( SELECT 
		  TRIM( CUST_INTRN_CHNL_ADDR )                       as                               CUST_INTRN_CHNL_ADDR 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( INTRN_CHNL_TYP_CD )                          as                                  INTRN_CHNL_TYP_CD 
		, TRIM( INTRN_CHNL_TYP_VOID_IND )                    as                            INTRN_CHNL_TYP_VOID_IND 
		from SRC_EM
        QUALIFY(ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY AUDIT_USER_CREA_DTM DESC ))=1 
		    ),
LOGIC_TI as ( SELECT 
		  CASE WHEN LENGTH(TRIM(TAX_ID_NO))=9 THEN  SUBSTR(TAX_ID_NO, 1, 3) ||'-'||SUBSTR(TAX_ID_NO, 4, 2)||'-'||SUBSTR(TAX_ID_NO, 6, 4)
          ELSE TAX_ID_NO END                                 as                                          TAX_ID_NO
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( TAX_ID_TYP_CD )                              as                                      TAX_ID_TYP_CD 
		, TAX_ID_END_DT                                      as                                      TAX_ID_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_TI
            )			

---- RENAME LAYER ----
,

RENAME_PPP as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                            CUST_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
				FROM     LOGIC_PPP   ), 
RENAME_CN as ( SELECT 
		  CUST_NM_NM                                         as                            COVERED_INDIVIDUAL_NAME
		, CUST_NM_FST                                        as                      COVERED_INDIVIDUAL_FIRST_NAME
		, CUST_NM_MID                                        as                     COVERED_INDIVIDUAL_MIDDLE_NAME
		, CUST_NM_LST                                        as                       COVERED_INDIVIDUAL_LAST_NAME
		, CUST_NM_SFX_TYP_NM                                 as                     COVERED_INDIVIDUAL_SUFFIX_NAME
		, CUST_ID                                            as                                         CN_CUST_ID
		, CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, VOID_IND                                           as                                        CN_VOID_IND 
				FROM     LOGIC_CN   ), 
RENAME_MAIL as ( SELECT 
		  CUST_ADDR_STR_1                                    as                           MAILING_STREET_ADDRESS_1
		, CUST_ADDR_STR_2                                    as                           MAILING_STREET_ADDRESS_2
		, CUST_ADDR_CITY_NM                                  as                          MAILING_ADDRESS_CITY_NAME
		, STT_ABRV                                           as                         MAILING_ADDRESS_STATE_CODE
		, STT_NM                                             as                         MAILING_ADDRESS_STATE_NAME
		, CUST_ADDR_CNTY_NM                                  as                        MAILING_ADDRESS_COUNTY_NAME
		, CNTRY_NM                                           as                       MAILING_ADDRESS_COUNTRY_NAME
		, CUST_ADDR_POST_CD                                  as                        MAILING_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_POSTAL_CODE              as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP_CODE                 as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, MAILING_FORMATTED_ADDRESS_ZIP4_CODE                as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE
		, CUST_ADDR_COMT                                     as                       MAILING_ADDRESS_COMMENT_TEXT
		, CUST_ADDR_VLDT_IND                                 as                      MAILING_ADDRESS_VALIDATED_IND
		, CUST_ID                                            as                                       MAIL_CUST_ID
		, CUST_ADDR_TYP_CD                                   as                              MAIL_CUST_ADDR_TYP_CD
		, CUST_ADDR_END_DT                                   as                              MAIL_CUST_ADDR_END_DT
		, VOID_IND                                           as                                      MAIL_VOID_IND 
				FROM     LOGIC_MAIL   ), 
RENAME_PHYS as ( SELECT 
		  CUST_ADDR_STR_1                                    as                          PHYSICAL_STREET_ADDRESS_1
		, CUST_ADDR_STR_2                                    as                          PHYSICAL_STREET_ADDRESS_2
		, CUST_ADDR_CITY_NM                                  as                         PHYSICAL_ADDRESS_CITY_NAME
		, STT_ABRV                                           as                        PHYSICAL_ADDRESS_STATE_CODE
		, STT_NM                                             as                        PHYSICAL_ADDRESS_STATE_NAME
		, CUST_ADDR_CNTY_NM                                  as                       PHYSICAL_ADDRESS_COUNTY_NAME
		, CNTRY_NM                                           as                      PHYSICAL_ADDRESS_COUNTRY_NAME
		, CUST_ADDR_POST_CD                                  as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE             as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE                as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE               as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
		, CUST_ADDR_COMT                                     as                      PHYSICAL_ADDRESS_COMMENT_TEXT
		, CUST_ADDR_VLDT_IND                                 as                     PHYSICAL_ADDRESS_VALIDATED_IND
		, CUST_ID                                            as                                       PHYS_CUST_ID
		, CUST_ADDR_TYP_CD                                   as                              PHYS_CUST_ADDR_TYP_CD
		, CUST_ADDR_END_DT                                   as                              PHYS_CUST_ADDR_END_DT
		, VOID_IND                                           as                                      PHYS_VOID_IND 
				FROM     LOGIC_PHYS   ), 
RENAME_DB as ( SELECT 
		  DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND
		, CUST_ID                                            as                                         DB_CUST_ID
		, BLK_TYP_CD                                         as                                      DB_BLK_TYP_CD
		, BLK_END_DT                                         as                                      DB_BLK_END_DT
		, VOID_IND                                           as                                        DB_VOID_IND 
				FROM     LOGIC_DB   ), 
RENAME_HP as ( SELECT 
		  CUST_INTRN_CHNL_PHN_NO                             as                                  HOME_PHONE_NUMBER
		, CUST_ID                                            as                                         HP_CUST_ID
		, PHN_NO_TYP_CD                                      as                                   HP_PHN_NO_TYP_CD
		, INTRN_CHNL_TYP_VOID_IND                            as                         HP_INTRN_CHNL_TYP_VOID_IND 
				FROM     LOGIC_HP   ), 
RENAME_CP as ( SELECT 
		  CUST_INTRN_CHNL_PHN_NO                             as                                  CELL_PHONE_NUMBER
		, CUST_ID                                            as                                         CP_CUST_ID
		, PHN_NO_TYP_CD                                      as                                   CP_PHN_NO_TYP_CD
		, INTRN_CHNL_TYP_VOID_IND                            as                         CP_INTRN_CHNL_TYP_VOID_IND 
				FROM     LOGIC_CP   ), 
RENAME_EM as ( SELECT 
		  CUST_INTRN_CHNL_ADDR                               as                                      EMAIL_ADDRESS
		, CUST_ID                                            as                                         EM_CUST_ID
		, INTRN_CHNL_TYP_CD                                  as                               EM_INTRN_CHNL_TYP_CD
		, INTRN_CHNL_TYP_VOID_IND                            as                         EM_INTRN_CHNL_TYP_VOID_IND 
				FROM     LOGIC_EM   ),
RENAME_TI as ( SELECT 
		  TAX_ID_NO                                          as                                          TAX_ID_NO
		, CUST_ID                                            as                                         TI_CUST_ID
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD
		, TAX_ID_END_DT                                      as                                      TAX_ID_END_DT
		, VOID_IND                                           as                                        TI_VOID_IND 
				FROM     LOGIC_TI   )				

---- FILTER LAYER (uses aliases) ----
,
FILTER_PPP                            as ( SELECT distinct * from    RENAME_PPP 
                                            WHERE PTCP_TYP_CD = 'COV_INDV'  ),
FILTER_CN                             as ( SELECT * from    RENAME_CN 
                                            WHERE CUST_NM_TYP_CD = 'PRSN_NM' AND CN_VOID_IND = 'N' AND CUST_NM_END_DT IS NULL  ),
FILTER_MAIL                           as ( SELECT * from    RENAME_MAIL 
                                            WHERE MAIL_CUST_ADDR_TYP_CD = 'MAIL' AND MAIL_VOID_IND = 'N' AND MAIL_CUST_ADDR_END_DT IS NULL  ),
FILTER_PHYS                           as ( SELECT * from    RENAME_PHYS 
                                            WHERE PHYS_CUST_ADDR_TYP_CD = 'PHYS' AND PHYS_VOID_IND = 'N' AND PHYS_CUST_ADDR_END_DT IS NULL  ),
FILTER_HP                             as ( SELECT * from    RENAME_HP 
                                            WHERE HP_PHN_NO_TYP_CD = 'H' AND HP_INTRN_CHNL_TYP_VOID_IND = 'N'  ),
FILTER_CP                             as ( SELECT * from    RENAME_CP 
                                            WHERE CP_PHN_NO_TYP_CD = 'C' AND CP_INTRN_CHNL_TYP_VOID_IND = 'N'  ),
FILTER_EM                             as ( SELECT * from    RENAME_EM 
                                            WHERE EM_INTRN_CHNL_TYP_CD = 'E' AND EM_INTRN_CHNL_TYP_VOID_IND = 'N'  ),
FILTER_DB                             as ( SELECT * from    RENAME_DB 
                                            WHERE DB_BLK_TYP_CD in ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK') AND DB_VOID_IND = 'N' AND COALESCE(DB_BLK_END_DT, '2999-12-31') >= CURRENT_DATE  ),
FILTER_TI                             as ( SELECT * from    RENAME_TI 
                                            WHERE TAX_ID_TYP_CD = 'SSN' and TAX_ID_END_DT IS NULL AND TI_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PPP as ( SELECT * 
				FROM  FILTER_PPP
				LEFT JOIN FILTER_CN ON  FILTER_PPP.CUST_ID =  FILTER_CN.CN_CUST_ID 
								LEFT JOIN FILTER_MAIL ON  FILTER_PPP.CUST_ID =  FILTER_MAIL.MAIL_CUST_ID 
								LEFT JOIN FILTER_PHYS ON  FILTER_PPP.CUST_ID =  FILTER_PHYS.PHYS_CUST_ID 
								LEFT JOIN FILTER_HP ON  FILTER_PPP.CUST_ID =  FILTER_HP.HP_CUST_ID 
								LEFT JOIN FILTER_CP ON  FILTER_PPP.CUST_ID =  FILTER_CP.CP_CUST_ID 
								LEFT JOIN FILTER_EM ON  FILTER_PPP.CUST_ID =  FILTER_EM.EM_CUST_ID 
								LEFT JOIN FILTER_DB ON  FILTER_PPP.CUST_ID =  FILTER_DB.DB_CUST_ID  
								LEFT JOIN FILTER_TI ON  FILTER_PPP.CUST_ID =  FILTER_TI.TI_CUST_ID  
)
SELECT 
  md5(cast(
    
    coalesce(cast(CUST_NO as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY
, CUST_NO
, CUST_ID
, COVERED_INDIVIDUAL_NAME
, COVERED_INDIVIDUAL_FIRST_NAME
, COVERED_INDIVIDUAL_MIDDLE_NAME
, COVERED_INDIVIDUAL_LAST_NAME
, COVERED_INDIVIDUAL_SUFFIX_NAME
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
, COALESCE(DOCUMENT_BLOCK_IND, 'N') AS DOCUMENT_BLOCK_IND
, HOME_PHONE_NUMBER
, CELL_PHONE_NUMBER
, EMAIL_ADDRESS
, TAX_ID_NO
from PPP
      );
    