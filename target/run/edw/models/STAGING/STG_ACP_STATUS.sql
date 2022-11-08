

      create or replace  table DEV_EDW.STAGING.STG_ACP_STATUS  as
      (---- SRC LAYER ----
WITH
SRC_ACP as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_ACP_STATUS ),
SRC_RSN as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_ACP_PROCESS_STS_RSN_TYP ),
//SRC_ACP as ( SELECT *     from     CLAIM_ACP_STATUS) ,
//SRC_RSN as ( SELECT *     from     CLAIM_ACP_PROCESS_STS_RSN_TYP) ,

---- LOGIC LAYER ----

LOGIC_ACP as ( SELECT 
		  CLM_ACP_STS_ID                                     AS                                     CLM_ACP_STS_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( CLM_ACP_STS_IND )                           AS                                    CLM_ACP_STS_IND 
		, upper( CLM_ACP_STS_SYS_IND )                       AS                                CLM_ACP_STS_SYS_IND 
		, upper( TRIM( CLM_ACP_PRCS_STS_RSN_TYP_CD ) )       AS                        CLM_ACP_PRCS_STS_RSN_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		from SRC_ACP
            ),
LOGIC_RSN as ( SELECT 
		  upper( TRIM( CLM_ACP_PRCS_STS_RSN_TYP_CD ) )       AS                        CLM_ACP_PRCS_STS_RSN_TYP_CD 
		, upper( CLM_ACP_PRCS_STS_RSN_TYP_NM )               AS                        CLM_ACP_PRCS_STS_RSN_TYP_NM 
		, upper( CLM_ACP_PRCS_STS_RSN_ACT )                  AS                           CLM_ACP_PRCS_STS_RSN_ACT 
		, upper( CLM_ACP_PRCS_STS_RSN_DISP )                 AS                          CLM_ACP_PRCS_STS_RSN_DISP 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_RSN
            )

---- RENAME LAYER ----
,

RENAME_ACP as ( SELECT 
		  CLM_ACP_STS_ID                                     as                                     CLM_ACP_STS_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_ACP_STS_IND                                    as                                    CLM_ACP_STS_IND
		, CLM_ACP_STS_SYS_IND                                as                                CLM_ACP_STS_SYS_IND
		, CLM_ACP_PRCS_STS_RSN_TYP_CD                        as                        CLM_ACP_PRCS_STS_RSN_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
				FROM     LOGIC_ACP   ), 
RENAME_RSN as ( SELECT 
		  CLM_ACP_PRCS_STS_RSN_TYP_CD                        as                    RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD
		, CLM_ACP_PRCS_STS_RSN_TYP_NM                        as                        CLM_ACP_PRCS_STS_RSN_TYP_NM
		, CLM_ACP_PRCS_STS_RSN_ACT                           as                           CLM_ACP_PRCS_STS_RSN_ACT
		, CLM_ACP_PRCS_STS_RSN_DISP                          as                          CLM_ACP_PRCS_STS_RSN_DISP
		, VOID_IND                                           as                                       RSN_VOID_IND 
				FROM     LOGIC_RSN   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ACP                            as ( SELECT * from    RENAME_ACP   ),
FILTER_RSN                            as ( SELECT * from    RENAME_RSN 
				WHERE RSN_VOID_IND= 'N'  ),

---- JOIN LAYER ----

ACP as ( SELECT * 
				FROM  FILTER_ACP
				LEFT JOIN FILTER_RSN ON  FILTER_ACP.CLM_ACP_PRCS_STS_RSN_TYP_CD =  FILTER_RSN.RSN_CLM_ACP_PRCS_STS_RSN_TYP_CD  )
SELECT * 
from ACP
      );
    