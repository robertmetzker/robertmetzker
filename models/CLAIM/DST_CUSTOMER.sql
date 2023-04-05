

---- SRC LAYER ----
WITH
SRC_CUST           as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER' ) }} ),
SRC_CRI            as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_ROLE_IDENTIFIER' ) }} ),
SRC_PRTCP          as ( SELECT *     FROM     {{ ref( 'STG_PARTICIPATION' ) }} ),
SRC_TI             as ( SELECT *     FROM     {{ ref( 'STG_TAX_IDENTIFIER' ) }} ),
SRC_NAME           as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_NAME' ) }} ),
SRC_PH             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_INTERACTION_CHANNEL' ) }} ),
SRC_FAX            as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_INTERACTION_CHANNEL' ) }} ),
SRC_EM             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_INTERACTION_CHANNEL' ) }} ),
SRC_CL             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_LANGUAGE' ) }} ),
SRC_DB             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_BLOCK' ) }} ),
SRC_TB             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_BLOCK' ) }} ),
SRC_PA             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_ADDRESS' ) }} ),
SRC_MA             as ( SELECT *     FROM     {{ ref( 'STG_CUSTOMER_ADDRESS' ) }} ),
//SRC_CUST           as ( SELECT *     FROM     STG_CUSTOMER) ,
//SRC_CRI            as ( SELECT *     FROM     STG_CUSTOMER_ROLE_IDENTIFIER) ,
//SRC_PRTCP          as ( SELECT *     FROM     STG_PARTICIPATION) ,
//SRC_TI             as ( SELECT *     FROM     STG_TAX_IDENTIFIER) ,
//SRC_NAME           as ( SELECT *     FROM     STG_CUSTOMER_NAME) ,
//SRC_PH             as ( SELECT *     FROM     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_FAX            as ( SELECT *     FROM     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_EM             as ( SELECT *     FROM     STG_CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_CL             as ( SELECT *     FROM     STG_CUSTOMER_LANGUAGE) ,
//SRC_DB             as ( SELECT *     FROM     STG_CUSTOMER_BLOCK) ,
//SRC_TB             as ( SELECT *     FROM     STG_CUSTOMER_BLOCK) ,
//SRC_PA             as ( SELECT *     FROM     STG_CUSTOMER_ADDRESS) ,
//SRC_MA             as ( SELECT *     FROM     STG_CUSTOMER_ADDRESS) ,

---- LOGIC LAYER ----


LOGIC_CUST as ( SELECT 
		   {{ dbt_utils.generate_surrogate_key ( [ 'CUST_NO' ] ) }} 
                                                             as                                                    
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( CUST_NO ),'')                        as                                            CUST_NO 
		, NULLIF( TRIM( CUST_TYP_CD ),'')                    as                                        CUST_TYP_CD 
		, NULLIF( TRIM( CUST_TYP_NM ),'')                    as                                        CUST_TYP_NM 
		, NULLIF( TRIM( CUST_1099_RECV_IND ),'')             as                                 CUST_1099_RECV_IND 
		, NULLIF( TRIM( CUST_W9_RECV_IND ),'')               as                                   CUST_W9_RECV_IND 
		, NULLIF( TRIM( CUST_TAX_EXMT_IND ),'')              as                                  CUST_TAX_EXMT_IND 
		, NULLIF( TRIM( CUST_FRGN_CITZ_IND ),'')             as                                 CUST_FRGN_CITZ_IND 
		, NULLIF( TRIM( CUST_TAX_ID_UNAVL_IND ),'')          as                              CUST_TAX_ID_UNAVL_IND 
		, NULLIF( TRIM( CUST_TAX_ID_OVRRD_IND ),'')          as                              CUST_TAX_ID_OVRRD_IND 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_CUST
            ),

LOGIC_CRI as ( SELECT 
		  IF ID_TYP_CD IN HICN OR REP THEN CUST_ROLE_ID_VAL_STR ELSE NULL
                                                             as                                                    
		  IF ID_TYP_CD = HICN THEN 'HICN' IF ID_TYP_CD = REP THEN 'REP ID'   ELSE NULL
                                                             as                                                    
		, CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( CRI_VOID_IND ),'')                   as                                       CRI_VOID_IND 
		, NULLIF( TRIM( ID_TYP_CD ),'')                      as                                          ID_TYP_CD 
		, NULLIF( TRIM( CUST_ROLE_ID_VAL_STR ),'')           as                               CUST_ROLE_ID_VAL_STR 
		FROM SRC_CRI
            ),

LOGIC_PRTCP as ( SELECT 
		  DATA EXTRACTION RULE: ONE ROW PER CUST ID.
TABLE ALIAS: PRTCP

IF PTCP_TYP_CD = 'INSRD' THEN 'Y' ELSE 'N'
                                                             as                                                    
		  IF PTCP_TYP_CD = 'CLMT' THEN 'Y' ELSE 'N'          as                                                    
		, IF PTCP_TYP_CD = 'COV_INDV' THEN 'Y' ELSE 'N'      as                                                    
		, CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( PTCP_TYP_CD ),'')                    as                                        PTCP_TYP_CD 
		FROM SRC_PRTCP
            ),

LOGIC_TI as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( TAX_ID_TYP_CD ),'')                  as                                      TAX_ID_TYP_CD 
		, NULLIF( TRIM( TAX_ID_TYP_NM ),'')                  as                                      TAX_ID_TYP_NM 
		, NULLIF( TRIM( TAX_ID_NO ),'')                      as                                          TAX_ID_NO 
		, NULLIF( TRIM( TAX_ID_SEQ_NO ),'')                  as                                      TAX_ID_SEQ_NO 
		, USE TAX_ID_NO BUT FORMAT TO BELOW
WHEN TAX_ID_TYP_CD = 'FEIN' THEN 00-0000000 ELSE TAX_ID_TYP_CD = 'SSN' THEN 000-00-0000
                                                             as                                          TAX_ID_NO 
		, TAX_ID_EFF_DT                                      as                                      TAX_ID_EFF_DT 
		, TAX_ID_END_DT                                      as                                      TAX_ID_END_DT 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_TI
            ),

LOGIC_NAME as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( CUST_NM_NM ),'')                     as                                         CUST_NM_NM 
		, NULLIF( TRIM( CUST_NM_FST ),'')                    as                                        CUST_NM_FST 
		, NULLIF( TRIM( CUST_NM_MID ),'')                    as                                        CUST_NM_MID 
		, NULLIF( TRIM( CUST_NM_LST ),'')                    as                                        CUST_NM_LST 
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT 
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT 
		, NULLIF( TRIM( CUST_NM_TYP_CD ),'')                 as                                     CUST_NM_TYP_CD 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_NAME
            ),

LOGIC_PH as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( PHN_NO_TYP_NM ),'')                  as                                      PHN_NO_TYP_NM 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PRI_IND ),'')        as                            CUST_INTRN_CHNL_PRI_IND 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PHN_NO ),'')         as                             CUST_INTRN_CHNL_PHN_NO 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PHN_NO_EXT ),'')     as                         CUST_INTRN_CHNL_PHN_NO_EXT 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		FROM SRC_PH
            ),

LOGIC_FAX as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( PHN_NO_TYP_NM ),'')                  as                                      PHN_NO_TYP_NM 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PRI_IND ),'')        as                            CUST_INTRN_CHNL_PRI_IND 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PHN_NO ),'')         as                             CUST_INTRN_CHNL_PHN_NO 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PHN_NO_EXT ),'')     as                         CUST_INTRN_CHNL_PHN_NO_EXT 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_FAX
            ),

LOGIC_EM as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( INTRN_CHNL_TYP_NM ),'')              as                                  INTRN_CHNL_TYP_NM 
		, NULLIF( TRIM( CUST_INTRN_CHNL_PRI_IND ),'')        as                            CUST_INTRN_CHNL_PRI_IND 
		, NULLIF( TRIM( CUST_INTRN_CHNL_ADDR ),'')           as                               CUST_INTRN_CHNL_ADDR 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_EM
            ),

LOGIC_CL as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( CUST_LANG_PRI_IND ),'')              as                                  CUST_LANG_PRI_IND 
		, NULLIF( TRIM( LANG_TYP_NM ),'')                    as                                        LANG_TYP_NM 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_CL
            ),

LOGIC_DB as ( SELECT 
		  (DERIVED)                                          as                                                    
		, CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( BLK_TYP_CD ),'')                     as                                         BLK_TYP_CD 
		, BLK_END_DT                                         as                                         BLK_END_DT 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_DB
            ),

LOGIC_TB as ( SELECT 
		  (DERIVED)                                          as                                                    
		, CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( BLK_TYP_CD ),'')                     as                                         BLK_TYP_CD 
		, BLK_END_DT                                         as                                         BLK_END_DT 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_TB
            ),

LOGIC_PA as ( SELECT 
		  NULLIF( TRIM( CUST_ADDR_STR_1 ),'')                as                                    CUST_ADDR_STR_1 
		, NULLIF( TRIM( CUST_ADDR_STR_2 ),'')                as                                    CUST_ADDR_STR_2 
		, NULLIF( TRIM( CUST_ADDR_CITY_NM ),'')              as                                  CUST_ADDR_CITY_NM 
		, NULLIF( TRIM( STT_ABRV ),'')                       as                                           STT_ABRV 
		, NULLIF( TRIM( STT_NM ),'')                         as                                             STT_NM 
		, NULLIF( TRIM( CUST_ADDR_POST_CD ),'')              as                                  CUST_ADDR_POST_CD 
		, NULLIF( TRIM( CUST_ADDR_CNTY_NM ),'')              as                                  CUST_ADDR_CNTY_NM 
		, NULLIF( TRIM( CNTRY_NM ),'')                       as                                           CNTRY_NM 
		, NULLIF( TRIM( CUST_ADDR_VLDT_IND ),'')             as                                 CUST_ADDR_VLDT_IND 
		, NULLIF( TRIM( CUST_ADDR_COMT ),'')                 as                                     CUST_ADDR_COMT 
		, cast( CUST_ADDR_EFF_DATE as DATE )                 as                                 CUST_ADDR_EFF_DATE 
		, cast( CUST_ADDR_END_DATE as DATE )                 as                                 CUST_ADDR_END_DATE 
		, CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( CUST_ADDR_TYP_CD ),'')               as                                   CUST_ADDR_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_PA
            ),

LOGIC_MA as ( SELECT 
		  NULLIF( TRIM( CUST_ADDR_STR_1 ),'')                as                                    CUST_ADDR_STR_1 
		, NULLIF( TRIM( CUST_ADDR_STR_2 ),'')                as                                    CUST_ADDR_STR_2 
		, NULLIF( TRIM( CUST_ADDR_CITY_NM ),'')              as                                  CUST_ADDR_CITY_NM 
		, NULLIF( TRIM( STT_ABRV ),'')                       as                                           STT_ABRV 
		, NULLIF( TRIM( STT_NM ),'')                         as                                             STT_NM 
		, NULLIF( TRIM( CUST_ADDR_POST_CD ),'')              as                                  CUST_ADDR_POST_CD 
		, NULLIF( TRIM( CUST_ADDR_CNTY_NM ),'')              as                                  CUST_ADDR_CNTY_NM 
		, NULLIF( TRIM( CNTRY_NM ),'')                       as                                           CNTRY_NM 
		, NULLIF( TRIM( CUST_ADDR_VLDT_IND ),'')             as                                 CUST_ADDR_VLDT_IND 
		, NULLIF( TRIM( CUST_ADDR_COMT ),'')                 as                                     CUST_ADDR_COMT 
		, cast( CUST_ADDR_EFF_DATE as DATE )                 as                                 CUST_ADDR_EFF_DATE 
		, cast( CUST_ADDR_END_DATE as DATE )                 as                                 CUST_ADDR_END_DATE 
		, CUST_ID                                            as                                            CUST_ID 
		, NULLIF( TRIM( CUST_ADDR_TYP_CD ),'')               as                                   CUST_ADDR_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, NULLIF( TRIM( VOID_IND ),'')                       as                                           VOID_IND 
		FROM SRC_MA
            )

---- RENAME LAYER ----
,

RENAME_CUST       as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CUST_ID                                            as                                            CUST_ID
		, CUST_NO                                            as                                            CUST_NO
		, CUST_TYP_CD                                        as                                        CUST_TYP_CD
		, CUST_TYP_NM                                        as                                        CUST_TYP_NM
		, CUST_1099_RECV_IND                                 as                                 CUST_1099_RECV_IND
		, CUST_W9_RECV_IND                                   as                                   CUST_W9_RECV_IND
		, CUST_TAX_EXMT_IND                                  as                                  CUST_TAX_EXMT_IND
		, CUST_FRGN_CITZ_IND                                 as                                 CUST_FRGN_CITZ_IND
		, CUST_TAX_ID_UNAVL_IND                              as                              CUST_TAX_ID_UNAVL_IND
		, CUST_TAX_ID_OVRRD_IND                              as                              CUST_TAX_ID_OVRRD_IND
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CUST   ), 
RENAME_CRI        as ( SELECT 
		  ROLE_ID_NUMBER                                     as                                     ROLE_ID_NUMBER
		, ROLE_ID_NUMBER_TYPE                                as                                ROLE_ID_NUMBER_TYPE
		, CUST_ID                                            as                                        CRI_CUST_ID
		, CRI_VOID_IND                                       as                                       CRI_VOID_IND
		, ID_TYP_CD                                          as                                          ID_TYP_CD
		, CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR 
				FROM     LOGIC_CRI   ), 
RENAME_PRTCP      as ( SELECT 
		  EMPLOYER_IND                                       as                                       EMPLOYER_IND
		, INJURED_WORKER_IND                                 as                                 INJURED_WORKER_IND
		, COVERED_INDIVIDUAL_IND                             as                             COVERED_INDIVIDUAL_IND
		, CUST_ID                                            as                                      PRTCP_CUST_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
				FROM     LOGIC_PRTCP   ), 
RENAME_TI         as ( SELECT 
		  CUST_ID                                            as                                         TI_CUST_ID
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD
		, TAX_ID_TYP_NM                                      as                                      TAX_ID_TYP_NM
		, TAX_ID_NO                                          as                                          TAX_ID_NO
		, TAX_ID_SEQ_NO                                      as                                      TAX_ID_SEQ_NO
		, FORMATTED_TAX_NUMBER                               as                               FORMATTED_TAX_NUMBER
		, TAX_ID_EFF_DT                                      as                                      TAX_ID_EFF_DT
		, TAX_ID_END_DT                                      as                                      TAX_ID_END_DT
		, VOID_IND                                           as                                        TI_VOID_IND 
				FROM     LOGIC_TI   ), 
RENAME_NAME       as ( SELECT 
		  CUST_ID                                            as                                       NAME_CUST_ID
		, CUST_NM_NM                                         as                                         CUST_NM_NM
		, CUST_NM_FST                                        as                                        CUST_NM_FST
		, CUST_NM_MID                                        as                                        CUST_NM_MID
		, CUST_NM_LST                                        as                                        CUST_NM_LST
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD
		, VOID_IND                                           as                                      NAME_VOID_IND 
				FROM     LOGIC_NAME   ), 
RENAME_PH         as ( SELECT 
		  CUST_ID                                            as                                         PH_CUST_ID
		, PHN_NO_TYP_NM                                      as                                   PH_PHN_NO_TYP_NM
		, CUST_INTRN_CHNL_PRI_IND                            as                         PH_CUST_INTRN_CHNL_PRI_IND
		, CUST_INTRN_CHNL_PHN_NO                             as                                       PHONE_NUMBER
		, CUST_INTRN_CHNL_PHN_NO_EXT                         as                             PHONE_EXTENSION_NUMBER
		, VOID_IND                                           as                                        PH_VOID_IND
		, AUDIT_USER_CREA_DTM                                as                             PH_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                             PH_AUDIT_USER_UPDT_DTM
		, AUDIT_USER_CREA_DTM                                as                            FAX_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                            FAX_AUDIT_USER_UPDT_DTM
		, AUDIT_USER_CREA_DTM                                as                            FAX_AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                            FAX_AUDIT_USER_UPDT_DTM 
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
RENAME_CL         as ( SELECT 
		  CUST_ID                                            as                                         CL_CUST_ID
		, CUST_LANG_PRI_IND                                  as                               CL_CUST_LANG_PRI_IND
		, LANG_TYP_NM                                        as                                     CL_LANG_TYP_NM
		, VOID_IND                                           as                                        CL_VOID_IND 
				FROM     LOGIC_CL   ), 
RENAME_DB         as ( SELECT 
		  DOCUMENT_BLOCK_IND                                 as                                 DOCUMENT_BLOCK_IND
		, CUST_ID                                            as                                         DB_CUST_ID
		, BLK_TYP_CD                                         as                                      DB_BLK_TYP_CD
		, BLK_END_DT                                         as                                      DB_BLK_END_DT
		, VOID_IND                                           as                                        DB_VOID_IND 
				FROM     LOGIC_DB   ), 
RENAME_TB         as ( SELECT 
		  THREAT_BEHAVIOR_BLOCK_IND                          as                          THREAT_BEHAVIOR_BLOCK_IND
		, CUST_ID                                            as                                         TB_CUST_ID
		, BLK_TYP_CD                                         as                                      TB_BLK_TYP_CD
		, BLK_END_DT                                         as                                      TB_BLK_END_DT
		, VOID_IND                                           as                                        TB_VOID_IND 
				FROM     LOGIC_TB   ), 
RENAME_PA         as ( SELECT 
		  CUST_ADDR_STR_1                                    as                          PHYSICAL_STREET_ADDRESS_1
		, CUST_ADDR_STR_2                                    as                          PHYSICAL_STREET_ADDRESS_2
		, CUST_ADDR_CITY_NM                                  as                         PHYSICAL_ADDRESS_CITY_NAME
		, STT_ABRV                                           as                        PHYSICAL_ADDRESS_STATE_CODE
		, STT_NM                                             as                        PHYSICAL_ADDRESS_STATE_NAME
		, CUST_ADDR_POST_CD                                  as                       PHYSICAL_ADDRESS_POSTAL_CODE
		, CUST_ADDR_POST_CD                                  as             PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, CUST_ADDR_POST_CD                                  as                PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, CUST_ADDR_POST_CD                                  as               PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
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
		, CUST_ADDR_POST_CD                                  as              MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, CUST_ADDR_POST_CD                                  as                 MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, CUST_ADDR_POST_CD                                  as                MAILING_FORMATTED_ADDRESS_ZIP4_CODE
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
				FROM     LOGIC_MA   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CUST                           as ( SELECT * FROM    RENAME_CUST 
                                            WHERE VOID_IND = 'N'  ),
FILTER_NAME                           as ( SELECT * FROM    RENAME_NAME 
                                            WHERE CUST_NM_TYP_CD IN ('PRSN_NM', 'BUSN_LEGAL_NM') AND NAME_VOID_IND = 'N' AND coalesce(CUST_NM_END_DT, '2999-12-31') >= current_date  ),
FILTER_DB                             as ( SELECT * FROM    RENAME_DB 
                                            WHERE DB_BLK_TYP_CD in ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK') AND DB_VOID_IND = 'N' AND coalesce(DB_BLK_END_DT, '2999-12-31') >= current_date  ),
FILTER_TB                             as ( SELECT * FROM    RENAME_TB 
                                            WHERE TB_BLK_TYP_CD = 'ALERT' AND TB_VOID_IND = 'N' AND COALESCE(TB_BLK_END_DT, '2999-12-31') >= current_date  ),
FILTER_MA                             as ( SELECT * FROM    RENAME_MA 
                                            WHERE MA_CUST_ADDR_TYP_CD = 'MAIL' AND MA_VOID_IND = 'N' AND CURRENT_DATE BETWEEN MAILING_ADDRESS_EFFECTIVE_DATE AND COALESCE(MAILING_ADDRESS_END_DATE ,'12/31/2999'::DATE)
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY MA_CUST_ID ORDER BY MAILING_ADDRESS_EFFECTIVE_DATE DESC, MA_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_PA                             as ( SELECT * FROM    RENAME_PA 
                                            WHERE PA_CUST_ADDR_TYP_CD = 'PHYS' AND PA_VOID_IND = 'N' AND CURRENT_DATE BETWEEN PHYSICAL_ADDRESS_EFFECTIVE_DATE AND COALESCE(PHYSICAL_ADDRESS_END_DATE ,'12/31/2999'::DATE)
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY PA_CUST_ID ORDER BY PHYSICAL_ADDRESS_EFFECTIVE_DATE DESC, PA_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_PH                             as ( SELECT * FROM    RENAME_PH 
                                            WHERE PH_VOID_IND = 'N' AND PH_PHN_NO_TYP_NM = 'BUSINESS' AND PH_CUST_INTRN_CHNL_PRI_IND = 'Y'  
QUALIFY row_number() OVER (PARTITION BY CUST_ID ORDER BY COALESCE(PH_AUDIT_USER_UPDT_DTM, PH_AUDIT_USER_CREA_DTM ) DESC) =1  ),
FILTER_FAX                            as ( SELECT * FROM    RENAME_FAX 
                                            WHERE FAX_VOID_IND = 'N' AND FAX_PHN_NO_TYP_NM = 'FAX' AND FAX_CUST_INTRN_CHNL_PRI_IND = 'Y'
QUALIFY row_number() OVER (PARTITION BY CUST_ID ORDER BY COALESCE(FAX_AUDIT_USER_UPDT_DTM, FAX_AUDIT_USER_CREA_DTM ) DESC) =1  ),
FILTER_EM                             as ( SELECT * FROM    RENAME_EM 
                                            WHERE EM_VOID_IND = 'N' AND EM_INTRN_CHNL_TYP_NM = 'E-MAIL' AND EM_CUST_INTRN_CHNL_PRI_IND = 'Y'
QUALIFY row_number() OVER (PARTITION BY CUST_ID ORDER BY COALESCE(EM_AUDIT_USER_UPDT_DTM, EM_AUDIT_USER_CREA_DTM ) DESC) =1  ),
FILTER_CL                             as ( SELECT * FROM    RENAME_CL 
                                            WHERE CL_CUST_LANG_PRI_IND='Y' AND CL_VOID_IND = 'N'  ),
FILTER_TI                             as ( SELECT * FROM    RENAME_TI 
                                            WHERE TAX_ID_END_DT IS NULL AND TI_VOID_IND = 'N'  ),
FILTER_CRI                            as ( SELECT * FROM    RENAME_CRI 
                                            WHERE CRI_VOID_IND = 'N'  AND ID_TYP_CD IN ('HICN', 'REP')
-- Pick a hierarchy when it has more than one ID_TYP_CD for a CUST_ID. HICN is the top of the order  ),
FILTER_PRTCP                          as ( SELECT * FROM    RENAME_PRTCP   ),

---- JOIN LAYER ----

CUST as ( SELECT * 
				FROM  FILTER_CUST
				LEFT JOIN FILTER_NAME ON  FILTER_CUST.CUST_ID =  FILTER_NAME.NAME_CUST_ID 
								LEFT JOIN FILTER_DB ON  FILTER_CUST.CUST_ID =  FILTER_DB.DB_CUST_ID 
								LEFT JOIN FILTER_TB ON  FILTER_CUST.CUST_ID =  FILTER_TB.TB_CUST_ID 
								LEFT JOIN FILTER_MA ON  FILTER_CUST.CUST_ID =  FILTER_MA.MA_CUST_ID 
								LEFT JOIN FILTER_PA ON  FILTER_CUST.CUST_ID =  FILTER_PA.PA_CUST_ID 
								LEFT JOIN FILTER_PH ON  FILTER_CUST.CUST_ID =  FILTER_PH.PH_CUST_ID 
								LEFT JOIN FILTER_FAX ON  FILTER_CUST.CUST_ID =  FILTER_FAX.FAX_CUST_ID 
								LEFT JOIN FILTER_EM ON  FILTER_CUST.CUST_ID =  FILTER_EM.EM_CUST_ID 
								LEFT JOIN FILTER_CL ON  FILTER_CUST.CUST_ID =  FILTER_CL.CL_CUST_ID 
								LEFT JOIN FILTER_TI ON  FILTER_CUST.CUST_ID =  FILTER_TI.TI_CUST_ID 
								LEFT JOIN FILTER_CRI ON  FILTER_CUST.CUST_ID =  FILTER_CRI_CUST_ID 
								LEFT JOIN FILTER_PRTCP ON  FILTER_CUST.CUST_ID =  FILTER_PRTCP_CUST_ID  )
SELECT 
		  UNIQUE_ID_KEY
		, CUST_ID
		, CUST_NO
		, CUST_TYP_CD
		, CUST_TYP_NM
		, ROLE_ID_NUMBER
		, ROLE_ID_NUMBER_TYPE
		, EMPLOYER_IND
		, INJURED_WORKER_IND
		, COVERED_INDIVIDUAL_IND
		, CUST_1099_RECV_IND
		, CUST_W9_RECV_IND
		, CUST_TAX_EXMT_IND
		, CUST_FRGN_CITZ_IND
		, CUST_TAX_ID_UNAVL_IND
		, CUST_TAX_ID_OVRRD_IND
		, VOID_IND
		, TI_CUST_ID
		, TAX_ID_TYP_CD
		, TAX_ID_TYP_NM
		, TAX_ID_NO
		, TAX_ID_SEQ_NO
		, FORMATTED_TAX_NUMBER
		, TAX_ID_EFF_DT
		, TAX_ID_END_DT
		, TI_VOID_IND
		, NAME_CUST_ID
		, CUST_NM_NM
		, CUST_NM_FST
		, CUST_NM_MID
		, CUST_NM_LST
		, CUST_NM_EFF_DT
		, CUST_NM_END_DT
		, CUST_NM_TYP_CD
		, NAME_VOID_IND
		, PH_CUST_ID
		, PH_PHN_NO_TYP_NM
		, PH_CUST_INTRN_CHNL_PRI_IND
		, PHONE_NUMBER
		, PHONE_EXTENSION_NUMBER
		, PH_VOID_IND
		, PH_AUDIT_USER_CREA_DTM
		, PH_AUDIT_USER_UPDT_DTM
		, FAX_CUST_ID
		, FAX_PHN_NO_TYP_NM
		, FAX_CUST_INTRN_CHNL_PRI_IND
		, FAX_NUMBER
		, FAX_EXTENSION_NUMBER
		, FAX_VOID_IND
		, FAX_AUDIT_USER_CREA_DTM
		, FAX_AUDIT_USER_UPDT_DTM
		, EM_CUST_ID
		, EM_INTRN_CHNL_TYP_NM
		, EM_CUST_INTRN_CHNL_PRI_IND
		, EMAIL_ADDRESS
		, EM_VOID_IND
		, FAX_AUDIT_USER_CREA_DTM
		, FAX_AUDIT_USER_UPDT_DTM
		, CL_CUST_ID
		, CL_CUST_LANG_PRI_IND
		, CL_LANG_TYP_NM
		, CL_VOID_IND
		, DOCUMENT_BLOCK_IND
		, DB_CUST_ID
		, DB_BLK_TYP_CD
		, DB_BLK_END_DT
		, DB_VOID_IND
		, THREAT_BEHAVIOR_BLOCK_IND
		, TB_CUST_ID
		, TB_BLK_TYP_CD
		, TB_BLK_END_DT
		, TB_VOID_IND
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
FROM CUST