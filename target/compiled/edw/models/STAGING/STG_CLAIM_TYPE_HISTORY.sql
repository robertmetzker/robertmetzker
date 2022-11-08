---- SRC LAYER ----
WITH
SRC_CTH as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_CLAIM_TYPE_HISTORY ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_TYPE ),
//SRC_CTH as ( SELECT *     from     CLAIM_CLAIM_TYPE_HISTORY) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_CT as ( SELECT *     from     CLAIM_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CTH as ( SELECT 
		  HIST_ID                                            AS                                            HIST_ID 
		, CLM_CLM_TYP_ID                                     AS                                     CLM_CLM_TYP_ID 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, cast( CLM_CLM_TYP_EFF_DT as DATE )                 AS                                 CLM_CLM_TYP_EFF_DT 
		, cast( CLM_CLM_TYP_END_DT as DATE )                 AS                                 CLM_CLM_TYP_END_DT 
		, upper( TRIM( CLM_TYP_CD ) )                        AS                                         CLM_TYP_CD 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DTM                                       AS                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       AS                                       HIST_END_DTM 
		from SRC_CTH
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            AS                                             CLM_NO 
		, AGRE_ID                                            AS                                            AGRE_ID 
		, upper( CLM_REL_SNPSHT_IND )                        AS                                 CLM_REL_SNPSHT_IND 
		from SRC_C
            ),
LOGIC_CT as ( SELECT 
		  upper( TRIM( CLM_TYP_NM ) )                        AS                                         CLM_TYP_NM 
		, upper( TRIM( CLM_TYP_CD ) )                        AS                                         CLM_TYP_CD 
		from SRC_CT
            )

---- RENAME LAYER ----
,

RENAME_CTH as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_CLM_TYP_ID                                     as                                     CLM_CLM_TYP_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_CLM_TYP_EFF_DT                                 as                                 CLM_CLM_TYP_EFF_DT
		, CLM_CLM_TYP_END_DT                                 as                                 CLM_CLM_TYP_END_DT
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
				FROM     LOGIC_CTH   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_CT as ( SELECT 
		  CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CLM_TYP_CD                                         as                                      CT_CLM_TYP_CD 
				FROM     LOGIC_CT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CTH                            as ( SELECT * from    RENAME_CTH   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_CT                             as ( SELECT * from    RENAME_CT   ),

---- JOIN LAYER ----

CTH as ( SELECT * 
				FROM  FILTER_CTH
				INNER JOIN FILTER_C ON  FILTER_CTH.AGRE_ID =  FILTER_C.C_AGRE_ID 
								LEFT JOIN FILTER_CT ON  FILTER_CTH.CLM_TYP_CD =  FILTER_CT.CT_CLM_TYP_CD  )
SELECT * , 
case when CTH.CLM_TYP_CD <> lag (CTH.CLM_TYP_CD) OVER (PARTITION BY CTH.CLM_NO order by CTH.HIST_EFF_DTM) then 'Y' 
     else 'N' end as CHNG_OVR_IND
from CTH