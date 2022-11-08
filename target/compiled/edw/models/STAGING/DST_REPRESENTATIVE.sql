---- SRC LAYER ----
WITH
SRC_REP as ( SELECT *     from     STAGING.STG_REPRESENTATIVE ),
SRC_NAME as ( SELECT *     from     STAGING.STG_CUSTOMER_NAME ),
--SRC_RATE as ( SELECT *     from     STAGING.STG_CUSTOMER_NAME ),
SRC_DB as ( SELECT *     from     STAGING.STG_CUSTOMER_BLOCK ),
--SRC_TB as ( SELECT *     from     STAGING.STG_CUSTOMER_BLOCK ),
SRC_MA as ( SELECT *     from     STAGING.STG_CUSTOMER_ADDRESS ),
--SRC_PA as ( SELECT *     from     STAGING.STG_CUSTOMER_ADDRESS ),
SRC_PH as ( SELECT *     from     STAGING.STG_CUSTOMER_INTERACTION_CHANNEL ),
//SRC_REP as ( SELECT *     from     STG_REPRESENTATIVE) ,
//SRC_NAME as ( SELECT *     from     STG_CUSTOMER_NAME) ,
--//SRC_RATE as ( SELECT *     from     STG_CUSTOMER_NAME) ,
//SRC_DB as ( SELECT *     from     STG_CUSTOMER_BLOCK) ,
--//SRC_TB as ( SELECT *     from     STG_CUSTOMER_BLOCK) ,
//SRC_MA as ( SELECT *     from     STG_CUSTOMER_ADDRESS) ,
--//SRC_PA as ( SELECT *     from     STG_CUSTOMER_ADDRESS) ,
//SRC_PH as ( SELECT *     from     STG_CUSTOMER_INTERACTION_CHANNEL) ,

---- LOGIC LAYER ----


LOGIC_REP as ( SELECT                                               
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, TRIM( CUST_TYP_CD )                                as                                        CUST_TYP_CD 
		, TRIM( CUST_TYP_NM )                                as                                        CUST_TYP_NM 
		, TRIM( CUST_1099_RECV_IND )                         as                                 CUST_1099_RECV_IND 
		, TRIM( CUST_W9_RECV_IND )                           as                                   CUST_W9_RECV_IND 
		, TRIM( CUST_TAX_EXMT_IND )                          as                                  CUST_TAX_EXMT_IND 
		, TRIM( CUST_FRGN_CITZ_IND )                         as                                 CUST_FRGN_CITZ_IND 
		, TRIM( CUST_TAX_ID_UNAVL_IND )                      as                              CUST_TAX_ID_UNAVL_IND 
		, TRIM( CUST_TAX_ID_OVRRD_IND )                      as                              CUST_TAX_ID_OVRRD_IND 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, cast( CUST_ROLE_ID_EFF_DT as DATE )                as                                CUST_ROLE_ID_EFF_DT 
		, cast( CUST_ROLE_ID_END_DT as DATE )                as                                CUST_ROLE_ID_END_DT 
		, TRIM( CUST_ROLE_ID_VAL_STR )                       as                               CUST_ROLE_ID_VAL_STR 
		from SRC_REP
            ),

LOGIC_NAME as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NM_NM )                                 as                                         CUST_NM_NM 
		, cast( CUST_NM_EFF_DT as DATE )                     as                                     CUST_NM_EFF_DT 
		, cast( CUST_NM_END_DT as DATE )                     as                                     CUST_NM_END_DT 
		, TRIM( CUST_NM_TYP_CD )                             as                                     CUST_NM_TYP_CD
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_NAME
            ),

/*LOGIC_RATE as ( SELECT 
		  TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_RATE
            ),*/

LOGIC_DB as ( SELECT 
		                                                
		 CUST_ID                                            as                                            CUST_ID 
		, TRIM( BLK_TYP_CD )                                 as                                         BLK_TYP_CD 
		, BLK_EFF_DT                                         as                                         BLK_EFF_DT 
		, BLK_END_DT                                         as                                         BLK_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_DB
            ),

LOGIC_TB as ( SELECT 
                                      
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( BLK_TYP_CD )                                 as                                         BLK_TYP_CD 
		, BLK_EFF_DT                                         as                                         BLK_EFF_DT 
		, BLK_END_DT                                         as                                         BLK_END_DT 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_DB        ----Previously SRC_TB
            ),

LOGIC_MA as ( SELECT 
		  TRIM( CUST_ADDR_STR_1 )                            as                                    CUST_ADDR_STR_1 
		, TRIM( CUST_ADDR_STR_2 )                            as                                    CUST_ADDR_STR_2 
		, TRIM( CUST_ADDR_CITY_NM )                          as                                  CUST_ADDR_CITY_NM 
		, TRIM( STT_ABRV )                                   as                                           STT_ABRV 
		, TRIM( STT_NM )                                     as                                             STT_NM 
		, TRIM( CUST_ADDR_POST_CD )                          as                                  CUST_ADDR_POST_CD 
		, TRIM( CUST_ADDR_CNTY_NM )                          as                                  CUST_ADDR_CNTY_NM 
		, TRIM( CNTRY_NM )                                   as                                           CNTRY_NM 
		, TRIM( CUST_ADDR_VLDT_IND )                         as                                 CUST_ADDR_VLDT_IND 
		, TRIM( CUST_ADDR_COMT )                             as                                     CUST_ADDR_COMT 
		, cast( CUST_ADDR_EFF_DATE as DATE )                 as                                 CUST_ADDR_EFF_DATE 
		, cast( DRVD_CUST_ADDR_END_DATE as DATE )                 as                                 DRVD_CUST_ADDR_END_DATE 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_ADDR_TYP_CD )                           as                                   CUST_ADDR_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_MA
            ),

LOGIC_PA as ( SELECT 
		  TRIM( CUST_ADDR_STR_1 )                            as                                    CUST_ADDR_STR_1 
		, TRIM( CUST_ADDR_STR_2 )                            as                                    CUST_ADDR_STR_2 
		, TRIM( CUST_ADDR_CITY_NM )                          as                                  CUST_ADDR_CITY_NM 
		, TRIM( STT_ABRV )                                   as                                           STT_ABRV 
		, TRIM( STT_NM )                                     as                                             STT_NM 
		, TRIM( CUST_ADDR_POST_CD )                          as                                  CUST_ADDR_POST_CD 
		, TRIM( CUST_ADDR_CNTY_NM )                          as                                  CUST_ADDR_CNTY_NM 
		, TRIM( CNTRY_NM )                                   as                                           CNTRY_NM 
		, TRIM( CUST_ADDR_VLDT_IND )                         as                                 CUST_ADDR_VLDT_IND 
		, TRIM( CUST_ADDR_COMT )                             as                                     CUST_ADDR_COMT 
		, cast( CUST_ADDR_EFF_DATE as DATE )                 as                                 CUST_ADDR_EFF_DATE 
		, cast( DRVD_CUST_ADDR_END_DATE as DATE )                 as                                 DRVD_CUST_ADDR_END_DATE 
		, CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_ADDR_TYP_CD )                           as                                   CUST_ADDR_TYP_CD 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_MA   ------------- Previously SRC_PA
            ),

LOGIC_PH as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( PHN_NO_TYP_NM )                              as                                      PHN_NO_TYP_NM 
		, TRIM( CUST_INTRN_CHNL_PRI_IND )                    as                            CUST_INTRN_CHNL_PRI_IND 
		, TRIM( CUST_INTRN_CHNL_PHN_NO )                     as                             CUST_INTRN_CHNL_PHN_NO
		, TRIM( VOID_IND )                              as                                                VOID_IND  
		from SRC_PH
            )

---- RENAME LAYER ----
,

RENAME_REP as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
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
		, CUST_ROLE_ID_EFF_DT                                as                                CUST_ROLE_ID_EFF_DT
		, CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT
		, CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR 
				FROM     LOGIC_REP   ), 
RENAME_NAME as ( SELECT 
		  CUST_ID                                            as                                       NAME_CUST_ID
		, CUST_NM_NM                                         as                                         CUST_NM_NM
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD
		, VOID_IND                                           as                                      NAME_VOID_IND 
				FROM     LOGIC_NAME   ), 
/* RENAME_RATE as ( SELECT 
		  VOID_IND                                           as                                      RATE_VOID_IND 
				FROM     LOGIC_RATE   ), */
RENAME_DB as ( SELECT 
		  CUST_ID                                            as                                         DB_CUST_ID
		, BLK_TYP_CD                                         as                                      DB_BLK_TYP_CD
		, BLK_EFF_DT                                         as                                      DB_BLK_EFF_DT 
		, BLK_END_DT                                         as                                      DB_BLK_END_DT
		, VOID_IND                                           as                                        DB_VOID_IND 
				FROM     LOGIC_DB   ), 
RENAME_TB as ( SELECT 
		  CUST_ID                                            as                                         TB_CUST_ID
		, BLK_TYP_CD                                         as                                      TB_BLK_TYP_CD
		, BLK_EFF_DT                                         as                                      TB_BLK_EFF_DT
		, BLK_END_DT                                         as                                      TB_BLK_END_DT
		, VOID_IND                                           as                                        TB_VOID_IND 
				FROM     LOGIC_TB   ), 
RENAME_MA as ( SELECT 
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
		, DRVD_CUST_ADDR_END_DATE                                 as                           MAILING_ADDRESS_END_DATE
		, CUST_ID                                            as                                         MA_CUST_ID
		, CUST_ADDR_TYP_CD                                   as                                MA_CUST_ADDR_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                             MA_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                        MA_VOID_IND 
				FROM     LOGIC_MA   ), 
RENAME_PA as ( SELECT 
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
		, DRVD_CUST_ADDR_END_DATE                                 as                          PHYSICAL_ADDRESS_END_DATE
		, CUST_ID                                            as                                         PA_CUST_ID
		, CUST_ADDR_TYP_CD                                   as                                PA_CUST_ADDR_TYP_CD
		, AUDIT_USER_CREA_DTM                                as                             PA_AUDIT_USER_CREA_DTM
		, VOID_IND                                           as                                        PA_VOID_IND 
				FROM     LOGIC_PA   ), 
RENAME_PH as ( SELECT 
		  CUST_ID                                            as                                         PH_CUST_ID
		, PHN_NO_TYP_NM                                      as                                      PHN_NO_TYP_NM
		, CUST_INTRN_CHNL_PRI_IND                            as                            CUST_INTRN_CHNL_PRI_IND
		, CUST_INTRN_CHNL_PHN_NO                             as                             CUST_INTRN_CHNL_PHN_NO 
		, VOID_IND                                           as                                        PH_VOID_IND
				FROM     LOGIC_PH   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_REP                            as ( SELECT * from    RENAME_REP 
                                            WHERE VOID_IND = 'N'  ),
FILTER_NAME                           as ( SELECT * from    RENAME_NAME 
                                            WHERE CUST_NM_TYP_CD IN ('PRSN_NM', 'BUSN_LEGAL_NM') AND NAME_VOID_IND = 'N' AND coalesce(CUST_NM_END_DT, '2999-12-31') >= current_date  ),
FILTER_DB                             as ( SELECT * from    RENAME_DB 
                                            WHERE DB_BLK_TYP_CD in ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK') AND DB_VOID_IND = 'N' AND coalesce(DB_BLK_END_DT, '2999-12-31') >= current_date  
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY DB_cust_id ORDER BY DB_BLK_EFF_DT)) =1),
FILTER_TB                             as ( SELECT * from    RENAME_TB 
                                            WHERE TB_BLK_TYP_CD = 'ALERT' AND TB_VOID_IND = 'N' AND COALESCE(TB_BLK_END_DT, '2999-12-31') >= current_date  
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY TB_cust_id ORDER BY TB_BLK_EFF_DT)) =1),
FILTER_MA                             as ( SELECT * from    RENAME_MA 
                                            WHERE MA_CUST_ADDR_TYP_CD = 'MAIL' AND MA_VOID_IND = 'N' AND CURRENT_DATE BETWEEN MAILING_ADDRESS_EFFECTIVE_DATE AND COALESCE(MAILING_ADDRESS_END_DATE ,'12/31/2999'::DATE)
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY MA_CUST_ID ORDER BY MAILING_ADDRESS_EFFECTIVE_DATE DESC, MA_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_PA                             as ( SELECT * from    RENAME_PA 
                                            WHERE PA_CUST_ADDR_TYP_CD = 'PHYS' AND PA_VOID_IND = 'N' AND CURRENT_DATE BETWEEN PHYSICAL_ADDRESS_EFFECTIVE_DATE AND COALESCE(PHYSICAL_ADDRESS_END_DATE ,'12/31/2999'::DATE)
            QUALIFY (ROW_NUMBER() OVER(PARTITION BY PA_CUST_ID ORDER BY PHYSICAL_ADDRESS_EFFECTIVE_DATE DESC, PA_AUDIT_USER_CREA_DTM DESC)) =1  ),
FILTER_PH                             as ( SELECT * from    RENAME_PH 
                                            WHERE PH_VOID_IND = 'N' AND PHN_NO_TYP_NM = 'BUSINESS' AND CUST_INTRN_CHNL_PRI_IND = 'Y'    ),

---- JOIN LAYER ----

REP as ( SELECT * 
				FROM  FILTER_REP
				LEFT JOIN FILTER_PH   ON  FILTER_REP.CUST_ID =  FILTER_PH.PH_CUST_ID 
				LEFT JOIN FILTER_NAME ON  FILTER_REP.CUST_ID =  FILTER_NAME.NAME_CUST_ID 
				LEFT JOIN FILTER_DB   ON  FILTER_REP.CUST_ID =  FILTER_DB.DB_CUST_ID 
				LEFT JOIN FILTER_TB   ON  FILTER_REP.CUST_ID =  FILTER_TB.TB_CUST_ID 
				LEFT JOIN FILTER_MA   ON  FILTER_REP.CUST_ID =  FILTER_MA.MA_CUST_ID 
				LEFT JOIN FILTER_PA   ON  FILTER_REP.CUST_ID =  FILTER_PA.PA_CUST_ID  )
SELECT 
		 md5(cast(
    
    coalesce(cast(CUST_ID as 
    varchar
), '')

 as 
    varchar
))   as  UNIQUE_ID_KEY
		, CUST_ID
		, CUST_NO
		, CUST_TYP_CD
		, CUST_TYP_NM
		, CUST_1099_RECV_IND
		, CUST_W9_RECV_IND
		, CUST_TAX_EXMT_IND
		, CUST_FRGN_CITZ_IND
		, CUST_TAX_ID_UNAVL_IND
		, CUST_TAX_ID_OVRRD_IND
		, VOID_IND
		, CUST_ROLE_ID_EFF_DT
		, CUST_ROLE_ID_END_DT
		, CUST_ROLE_ID_VAL_STR
		, NAME_CUST_ID
		, CUST_NM_NM
		, CUST_NM_EFF_DT
		, CUST_NM_END_DT
		, CUST_NM_TYP_CD
		, NAME_VOID_IND
		, CASE WHEN DB_CUST_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS DOCUMENT_BLOCK_IND
		, DB_CUST_ID
		, DB_BLK_TYP_CD
		, DB_BLK_END_DT
		, DB_VOID_IND
		, CASE WHEN TB_CUST_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS  THREAT_BEHAVIOR_BLOCK_IND
		, TB_CUST_ID
		, TB_BLK_TYP_CD
		, TB_BLK_END_DT
		, TB_VOID_IND
		, MAILING_STREET_ADDRESS_1
		, MAILING_STREET_ADDRESS_2
		, MAILING_ADDRESS_CITY_NAME
		, MAILING_ADDRESS_STATE_CODE
		, MAILING_ADDRESS_STATE_NAME
		, MAILING_ADDRESS_POSTAL_CODE
		, CASE WHEN LENGTH(MAILING_FORMATTED_ADDRESS_POSTAL_CODE) = 9 THEN LEFT(MAILING_FORMATTED_ADDRESS_POSTAL_CODE, 5) ||'-'|| RIGHT(MAILING_FORMATTED_ADDRESS_POSTAL_CODE, 4) ELSE MAILING_FORMATTED_ADDRESS_POSTAL_CODE END AS MAILING_FORMATTED_ADDRESS_POSTAL_CODE
		, CASE WHEN LENGTH(MAILING_FORMATTED_ADDRESS_ZIP_CODE) = 9 THEN LEFT(MAILING_FORMATTED_ADDRESS_ZIP_CODE, 5) ELSE MAILING_FORMATTED_ADDRESS_ZIP_CODE END AS MAILING_FORMATTED_ADDRESS_ZIP_CODE
		, CASE WHEN LENGTH(MAILING_FORMATTED_ADDRESS_ZIP4_CODE) = 9 THEN RIGHT(MAILING_FORMATTED_ADDRESS_ZIP4_CODE, 4) ELSE NULL END AS MAILING_FORMATTED_ADDRESS_ZIP4_CODE
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
		, PHYSICAL_STREET_ADDRESS_1
		, PHYSICAL_STREET_ADDRESS_2
		, PHYSICAL_ADDRESS_CITY_NAME
		, PHYSICAL_ADDRESS_STATE_CODE
		, PHYSICAL_ADDRESS_STATE_NAME
		, PHYSICAL_ADDRESS_POSTAL_CODE
		, CASE WHEN LENGTH(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE) = 9 AND PHYSICAL_ADDRESS_COUNTRY_NAME = 'UNITED STATES'
      			THEN LEFT(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 5) ||'-'|| RIGHT(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 4)
				WHEN LENGTH(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE) = 6 AND PHYSICAL_ADDRESS_COUNTRY_NAME = 'CANADA' 
				THEN LEFT(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 3) ||' '|| RIGHT(PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE, 3)
				ELSE PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE END AS PHYSICAL_FORMATTED_ADDRESS_POSTAL_CODE
		, CASE WHEN LENGTH(PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE) = 9 AND PHYSICAL_ADDRESS_COUNTRY_NAME = 'UNITED STATES'
			THEN LEFT(PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE, 5)
			ELSE PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE END AS PHYSICAL_FORMATTED_ADDRESS_ZIP_CODE
		, CASE WHEN LENGTH(PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE) = 9 AND PHYSICAL_ADDRESS_COUNTRY_NAME = 'UNITED STATES' THEN RIGHT(PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE, 4) ELSE NULL END AS PHYSICAL_FORMATTED_ADDRESS_ZIP4_CODE
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
		, PH_CUST_ID
		, PHN_NO_TYP_NM
		, CUST_INTRN_CHNL_PRI_IND
		, CUST_INTRN_CHNL_PHN_NO 
from REP