

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_INVESTIGATION  as
      (---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.STG_CLAIM ),
SRC_ACP as ( SELECT *     from     STAGING.STG_ACP_STATUS ),
SRC_ACP_DATES as ( SELECT *     from     STAGING.STG_ACP_STATUS ),
SRC_ASGN as ( SELECT *     from     STAGING.STG_ASSIGNMENT ),
SRC_USERS as ( SELECT *     from     STAGING.STG_USERS ),
//SRC_CLM as ( SELECT *     from     STG_CLAIM) ,
//SRC_ACP as ( SELECT *     from     STG_ACP_STATUS) ,
//SRC_ACP_DATES as ( SELECT *     from     STG_ACP_STATUS) ,
//SRC_ASGN as ( SELECT *     from     STG_ASSIGNMENT) ,
//SRC_USERS as ( SELECT *     from     STG_USERS) ,

---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( AGRE_ID )                                    as                                            AGRE_ID 
		, CLM_FST_DCSN_DATE                                  as                                  CLM_FST_DCSN_DATE 
		, TRIM( JUR_TYP_CD )                                 as                                         JUR_TYP_CD 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		from SRC_CLM 
            ),
LOGIC_ACP as ( SELECT 
		  CLM_ACP_STS_ID                                     as                                     CLM_ACP_STS_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_ACP_STS_IND )                            as                                    CLM_ACP_STS_IND 
		, TRIM( CLM_ACP_STS_SYS_IND )                        as                                CLM_ACP_STS_SYS_IND 
		, TRIM( CLM_ACP_PRCS_STS_RSN_TYP_CD )                as                        CLM_ACP_PRCS_STS_RSN_TYP_CD 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD )            as                    RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD 
		, TRIM( CLM_ACP_PRCS_STS_RSN_TYP_NM )                as                        CLM_ACP_PRCS_STS_RSN_TYP_NM 
		, TRIM( CLM_ACP_PRCS_STS_RSN_ACT )                   as                           CLM_ACP_PRCS_STS_RSN_ACT 
		, TRIM( CLM_ACP_PRCS_STS_RSN_DISP )                  as                          CLM_ACP_PRCS_STS_RSN_DISP 
		from SRC_ACP
            ),
LOGIC_ACP_DATES as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		from SRC_ACP_DATES
            ),
LOGIC_ASGN as ( SELECT 
		  TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, ASGN_CNTX_ID                                       as                                       ASGN_CNTX_ID 
		, ASGN_EFF_DT                                        as                                        ASGN_EFF_DT 
		, ASGN_END_DT                                        as                                        ASGN_END_DT 
		, USER_ID                                            as                                            USER_ID 
		, TRIM( ASGN_PRI_OWNR_IND )                          as                                  ASGN_PRI_OWNR_IND 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, TRIM( DRVD_PRI_OWNR_IND )                          as                                  DRVD_PRI_OWNR_IND 
		from SRC_ASGN
            ),
LOGIC_USERS as ( SELECT 
		  USER_ID                                            as                                            USER_ID 
		, TRIM( USER_LGN_NM )                                as                                        USER_LGN_NM 
		, TRIM( USER_DRV_UPCS_NM )                           as                                   USER_DRV_UPCS_NM 
		from SRC_USERS
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_FST_DCSN_DATE                                  as                                  CLM_FST_DCSN_DATE
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_CLM   ), 
RENAME_ACP as ( SELECT 
		  CLM_ACP_STS_ID                                     as                                     CLM_ACP_STS_ID
		, AGRE_ID                                            as                                        ACP_AGRE_ID
		, CLM_ACP_STS_IND                                    as                                    CLM_ACP_STS_IND
		, CLM_ACP_STS_SYS_IND                                as                                CLM_ACP_STS_SYS_IND
		, CLM_ACP_PRCS_STS_RSN_TYP_CD                        as                        CLM_ACP_PRCS_STS_RSN_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD                    as                    RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD
		, CLM_ACP_PRCS_STS_RSN_TYP_NM                        as                        CLM_ACP_PRCS_STS_RSN_TYP_NM
		, CLM_ACP_PRCS_STS_RSN_ACT                           as                           CLM_ACP_PRCS_STS_RSN_ACT
		, CLM_ACP_PRCS_STS_RSN_DISP                          as                          CLM_ACP_PRCS_STS_RSN_DISP 
				FROM     LOGIC_ACP   ), 
RENAME_ACP_DATES as ( SELECT 
		  AGRE_ID                                            as                                  ACP_DATES_AGRE_ID
		, min(AUDIT_USER_CREA_DTM)                                as                                      ACP_START_DTM
		, max(AUDIT_USER_CREA_DTM)                                as                                        ACP_END_DTM 
				FROM     LOGIC_ACP_DATES  Group by AGRE_ID ),
RENAME_ASGN as ( SELECT 
		  APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, ASGN_CNTX_ID                                       as                                       ASGN_CNTX_ID
		, ASGN_EFF_DT                                        as                                        ASGN_EFF_DT
		, ASGN_END_DT                                        as                                        ASGN_END_DT
		, USER_ID                                            as                                       ASGN_USER_ID
		, ASGN_PRI_OWNR_IND                                  as                                  ASGN_PRI_OWNR_IND
		, AUDIT_USER_CREA_DTM                                as                           ASGN_AUDIT_USER_CREA_DTM
		, DRVD_PRI_OWNR_IND                                  as                                  DRVD_PRI_OWNR_IND 
				FROM     LOGIC_ASGN   ), 
RENAME_USERS as ( SELECT 
		  USER_ID                                            as                                            USER_ID
		, USER_LGN_NM                                        as                                        USER_LGN_NM
		, USER_DRV_UPCS_NM                                   as                                   USER_DRV_UPCS_NM 
				FROM     LOGIC_USERS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_ACP                            as ( SELECT * from    RENAME_ACP   ),
FILTER_ACP_DATES                      as ( SELECT * from    RENAME_ACP_DATES   ),
FILTER_ASGN                           as ( SELECT * from    RENAME_ASGN 
                                            WHERE APP_CNTX_TYP_CD = 'CLAIM' AND ASGN_PRI_OWNR_IND = 'Y'
           AND COALESCE(ASGN_END_DT,'2099-12-31') > CURRENT_DATE
 AND DRVD_PRI_OWNR_IND = 'Y'  ),
FILTER_USERS                          as ( SELECT * from    RENAME_USERS   ),

---- JOIN LAYER ----

ASGN as ( SELECT * 
				FROM  FILTER_ASGN
				LEFT JOIN FILTER_USERS ON  FILTER_ASGN.ASGN_USER_ID =  FILTER_USERS.USER_ID  ),
CLM as ( SELECT * 
				FROM  FILTER_CLM
				LEFT JOIN FILTER_ACP ON  FILTER_CLM.AGRE_ID =  FILTER_ACP.ACP_AGRE_ID 
				LEFT JOIN FILTER_ACP_DATES ON  FILTER_CLM.AGRE_ID =  FILTER_ACP_DATES.ACP_DATES_AGRE_ID AND FILTER_ACP.AUDIT_USER_CREA_DTM = ACP_END_DTM 
				LEFT JOIN ASGN ON  FILTER_CLM.AGRE_ID = ASGN.ASGN_CNTX_ID  ),


------ETL LAYER------------
ETL AS(SELECT 
md5(cast(
    
    coalesce(cast(CLM_NO as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,CLM_NO
,AGRE_ID
,CLM_FST_DCSN_DATE
,JUR_TYP_CD
,CLM_REL_SNPSHT_IND
,CLM_ACP_STS_ID
,ACP_AGRE_ID
,CLM_ACP_STS_IND
,CLM_ACP_STS_SYS_IND
,CLM_ACP_PRCS_STS_RSN_TYP_CD
,AUDIT_USER_ID_CREA
,AUDIT_USER_CREA_DTM
,CASE WHEN Coalesce (AUDIT_USER_ID_CREA,0) not in (0,-1) THEN 'Y' ELSE 'N' END AS MANUAL_IND 
,RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD
,CLM_ACP_PRCS_STS_RSN_TYP_NM
,CLM_ACP_PRCS_STS_RSN_ACT
,CLM_ACP_PRCS_STS_RSN_DISP
,ACP_DATES_AGRE_ID
,ACP_START_DTM
,ACP_END_DTM
,APP_CNTX_TYP_CD
,ASGN_CNTX_ID
,ASGN_EFF_DT
,ASGN_END_DT
,ASGN_USER_ID
,ASGN_PRI_OWNR_IND
,ASGN_AUDIT_USER_CREA_DTM
,DRVD_PRI_OWNR_IND
,USER_ID
,USER_LGN_NM
,USER_DRV_UPCS_NM
FROM CLM
qualify ( ROW_NUMBER ()OVER (PARTITION BY AGRE_ID ORDER BY ACP_START_DTM NULLS LAST)) = 1) 

SELECT * FROM ETL
      );
    