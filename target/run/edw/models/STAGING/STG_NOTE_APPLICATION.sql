

      create or replace  table DEV_EDW.STAGING.STG_NOTE_APPLICATION  as
      (---- SRC LAYER ----
WITH
SRC_NA as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE_APPLICATION_DETAIL_LEVEL ),
SRC_APL as ( SELECT *     from     DEV_VIEWS.PCMP.APPLICATION_DETAIL_LEVEL ),
SRC_CLM as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_CU as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_PP as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_P as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_PERIOD ),
//SRC_NA as ( SELECT *     from     NOTE_APPLICATION_DETAIL_LEVEL) ,
//SRC_APL as ( SELECT *     from     APPLICATION_DETAIL_LEVEL) ,
//SRC_CLM as ( SELECT *     from     CLAIM) ,
//SRC_CU as ( SELECT *     from     CUSTOMER) ,
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_P as ( SELECT *     from     POLICY_PERIOD) ,

---- LOGIC LAYER ----

LOGIC_NA as ( SELECT 
		  NOTE_APP_DTL_LVL_ID                                AS                                NOTE_APP_DTL_LVL_ID 
		, NOTE_ID                                            AS                                            NOTE_ID 
		, upper( TRIM( APP_DTL_LVL_CD ) )                    AS                                     APP_DTL_LVL_CD 
		, AGRE_ID_DRV                                        AS                                        AGRE_ID_DRV 
		, CUST_ID_DRV                                        AS                                        CUST_ID_DRV 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		from SRC_NA
            ),
LOGIC_APL as ( SELECT 
		  upper( TRIM( APP_DTL_LVL_NM ) )                    AS                                     APP_DTL_LVL_NM 
		, upper( TRIM( APP_DTL_LVL_CD ) )                    AS                                     APP_DTL_LVL_CD 
		, upper( APP_DTL_LVL_VOID_IND )                      AS                               APP_DTL_LVL_VOID_IND 
		from SRC_APL
            ),
LOGIC_CLM as ( SELECT 
		  AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, upper( CLM_REL_SNPSHT_IND )                        AS                                 CLM_REL_SNPSHT_IND 
		from SRC_CLM
            ),
LOGIC_CU as ( SELECT 
		  CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		from SRC_CU
            ),
LOGIC_PP as ( SELECT 
		  PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		from SRC_PP
            ),
LOGIC_P as ( SELECT DISTINCT
		  AGRE_ID                                            AS                                            AGRE_ID 
		, upper( TRIM( PLCY_NO ) )                           AS                                            PLCY_NO 
		from SRC_P
            )

---- RENAME LAYER ----
,

RENAME_NA as ( SELECT 
		  NOTE_APP_DTL_LVL_ID                                as                                NOTE_APP_DTL_LVL_ID
		, NOTE_ID                                            as                                            NOTE_ID
		, APP_DTL_LVL_CD                                     as                                     APP_DTL_LVL_CD
		, AGRE_ID_DRV                                        as                                        AGRE_ID_DRV
		, CUST_ID_DRV                                        as                                        CUST_ID_DRV
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
				FROM     LOGIC_NA   ), 
RENAME_APL as ( SELECT 
		  APP_DTL_LVL_NM                                     as                                     APP_DTL_LVL_NM
		, APP_DTL_LVL_CD                                     as                                 APL_APP_DTL_LVL_CD
		, APP_DTL_LVL_VOID_IND                               as                               APP_DTL_LVL_VOID_IND 
				FROM     LOGIC_APL   ), 
RENAME_CLM as ( SELECT 
		  AGRE_ID                                            as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_CLM   ), 
RENAME_CU as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
		, CUST_NO                                            as                                            CUST_NO 
				FROM     LOGIC_CU   ), 
RENAME_PP as ( SELECT 
		  PLCY_PRD_ID                                        as                                     PP_PLCY_PRD_ID
		, PLCY_NO                                            as                                         PP_PLCY_NO 
				FROM     LOGIC_PP   ), 
RENAME_P as ( SELECT 
		  AGRE_ID                                            as                                          P_AGRE_ID
		, PLCY_NO                                            as                                          P_PLCY_NO 
				FROM     LOGIC_P   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_NA                             as ( SELECT * from    RENAME_NA   ),
FILTER_APL                            as ( SELECT * from    RENAME_APL  WHERE APP_DTL_LVL_VOID_IND = 'N'  ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM  WHERE CLM_REL_SNPSHT_IND = 'N'   ),
FILTER_CU                             as ( SELECT * from    RENAME_CU   ),
FILTER_PP                             as ( SELECT * from    RENAME_PP  ),
FILTER_P                              as ( SELECT * from    RENAME_P   ),

---- JOIN LAYER ----

NA as ( SELECT * ,
			COALESCE(PP_PLCY_NO, P_PLCY_NO) as PLCY_NO
				FROM  FILTER_NA
				LEFT JOIN FILTER_APL ON  FILTER_NA.APP_DTL_LVL_CD =  FILTER_APL.APL_APP_DTL_LVL_CD 
								LEFT JOIN FILTER_CLM ON  FILTER_NA.AGRE_ID_DRV =  FILTER_CLM.CLM_AGRE_ID and APP_DTL_LVL_CD like 'CLAIM%'
								LEFT JOIN FILTER_CU ON  FILTER_NA.CUST_ID_DRV =  FILTER_CU.CUST_ID and APP_DTL_LVL_CD like 'CUSTOMER%'
								LEFT JOIN FILTER_PP ON  FILTER_NA.PLCY_PRD_ID =  FILTER_PP.PP_PLCY_PRD_ID and APP_DTL_LVL_CD like 'POLICY%'
								LEFT JOIN FILTER_P ON  FILTER_NA.AGRE_ID_DRV =  FILTER_P.P_AGRE_ID and APP_DTL_LVL_CD like 'POLICY%' )
SELECT * 
from NA
      );
    