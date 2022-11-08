

      create or replace  table DEV_EDW.STAGING.DST_EOB  as
      (---- SRC LAYER ----
WITH
SRC_EOB as ( SELECT *     from     STAGING.STG_EOB ),
SRC_REF as ( SELECT *     from     STAGING.STG_CAM_REF ),
//SRC_EOB as ( SELECT *     from     STG_EOB) ,
//SRC_REF as ( SELECT *     from     STG_CAM_REF) ,

---- LOGIC LAYER ----

LOGIC_EOB as ( SELECT 
		  EOB_RID                                            AS                                            EOB_RID 
		, TRIM( CODE )                                       AS                                               CODE 
		, TRIM( CATEGORY )                                   AS                                           CATEGORY 
		, TRIM( EOB_TYPE )                                   AS                                           EOB_TYPE 
		, TRIM( APPLIED_BY )                                 AS                                         APPLIED_BY 
		, ENTRY_DATE                                         AS                                         ENTRY_DATE 
		, TRIM( ENTRY_USER_ID )                              AS                                      ENTRY_USER_ID 
		, GEN_EOB_RID                                        AS                                        GEN_EOB_RID 
		, EFFECTIVE_DATE                                     AS                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		, TRIM( SHORT_DESCRIPTION )                          AS                                  SHORT_DESCRIPTION 
		, TRIM( LONG_DESCRIPTION )                           AS                                   LONG_DESCRIPTION 
		, GEN_ENTRY_DATE                                     AS                                     GEN_ENTRY_DATE 
		, TRIM( GEN_ENTRY_USER_ID )                          AS                                  GEN_ENTRY_USER_ID 
		, DLM                                                AS                                                DLM 
		, TRIM( ULM )                                        AS                                                ULM 
		, TRIM( ECAT_REF_DGN )                               AS                                       ECAT_REF_DGN 
		, TRIM( ECAT_REF_IDN )                               AS                                       ECAT_REF_IDN 
		, ECAT_REF_EFF_DTE                                   AS                                   ECAT_REF_EFF_DTE 
		, ECAT_REF_EXP_DTE                                   AS                                   ECAT_REF_EXP_DTE 
		, ECAT_REF_DLM                                       AS                                       ECAT_REF_DLM 
		, TRIM( ECAT_REF_ULM )                               AS                                       ECAT_REF_ULM 
		, TRIM( ECAT_REF_DSC )                               AS                                       ECAT_REF_DSC 
		, ECAT_REF_ENT_DTE                                   AS                                   ECAT_REF_ENT_DTE 
		, TRIM( ECAT_REF_ENT_UID )                           AS                                   ECAT_REF_ENT_UID 
		, ECAT_REF_RID                                       AS                                       ECAT_REF_RID 
		, TRIM( ETYP_REF_DGN )                               AS                                       ETYP_REF_DGN 
		, TRIM( ETYP_REF_IDN )                               AS                                       ETYP_REF_IDN 
		, ETYP_REF_EFF_DTE                                   AS                                   ETYP_REF_EFF_DTE 
		, ETYP_REF_EXP_DTE                                   AS                                   ETYP_REF_EXP_DTE 
		, ETYP_REF_DLM                                       AS                                       ETYP_REF_DLM 
		, TRIM( ETYP_REF_ULM )                               AS                                       ETYP_REF_ULM 
		, TRIM( ETYP_REF_DSC )                               AS                                       ETYP_REF_DSC 
		, ETYP_REF_ENT_DTE                                   AS                                   ETYP_REF_ENT_DTE 
		, TRIM( ETYP_REF_ENT_UID )                           AS                                   ETYP_REF_ENT_UID 
		, ETYP_REF_RID                                       AS                                       ETYP_REF_RID 
		from SRC_EOB
            ),
LOGIC_REF as ( SELECT 
		  TRIM( REF_DGN )                                    AS                                            REF_DGN 
		, TRIM( REF_IDN )                                    AS                                            REF_IDN 
		, REF_EFF_DTE                                        AS                                        REF_EFF_DTE 
		, REF_EXP_DTE                                        AS                                        REF_EXP_DTE 
		, REF_DLM                                            AS                                            REF_DLM 
		, TRIM( REF_ULM )                                    AS                                            REF_ULM 
		, TRIM( REF_DSC )                                    AS                                            REF_DSC 
		, REF_ENT_DTE                                        AS                                        REF_ENT_DTE 
		, TRIM( REF_ENT_UID )                                AS                                        REF_ENT_UID 
		, REF_RID                                            AS                                            REF_RID 
		from SRC_REF
            )

---- RENAME LAYER ----
,

RENAME_EOB as ( SELECT 
		  EOB_RID                                            as                                            EOB_RID
		, CODE                                               as                                               CODE
		, CATEGORY                                           as                                           CATEGORY
		, EOB_TYPE                                           as                                           EOB_TYPE
		, APPLIED_BY                                         as                                         APPLIED_BY
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, GEN_EOB_RID                                        as                                        GEN_EOB_RID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, CASE WHEN EXPIRATION_DATE > CURRENT_DATE THEN NULL ELSE EXPIRATION_DATE END
						                                     as                                    EXPIRATION_DATE
		, SHORT_DESCRIPTION                                  as                                  SHORT_DESCRIPTION
		, LONG_DESCRIPTION                                   as                                   LONG_DESCRIPTION
		, GEN_ENTRY_DATE                                     as                                     GEN_ENTRY_DATE
		, GEN_ENTRY_USER_ID                                  as                                  GEN_ENTRY_USER_ID
		, DLM                                                as                                                DLM
		, ULM                                                as                                                ULM
		, ECAT_REF_DGN                                       as                                       ECAT_REF_DGN
		, ECAT_REF_IDN                                       as                                       ECAT_REF_IDN
		, ECAT_REF_EFF_DTE                                   as                                   ECAT_REF_EFF_DTE
		, ECAT_REF_EXP_DTE                                   as                                   ECAT_REF_EXP_DTE
		, ECAT_REF_DLM                                       as                                       ECAT_REF_DLM
		, ECAT_REF_ULM                                       as                                       ECAT_REF_ULM
		, ECAT_REF_DSC                                       as                                       ECAT_REF_DSC
		, ECAT_REF_ENT_DTE                                   as                                   ECAT_REF_ENT_DTE
		, ECAT_REF_ENT_UID                                   as                                   ECAT_REF_ENT_UID
		, ECAT_REF_RID                                       as                                       ECAT_REF_RID
		, ETYP_REF_DGN                                       as                                       ETYP_REF_DGN
		, ETYP_REF_IDN                                       as                                       ETYP_REF_IDN
		, ETYP_REF_EFF_DTE                                   as                                   ETYP_REF_EFF_DTE
		, ETYP_REF_EXP_DTE                                   as                                   ETYP_REF_EXP_DTE
		, ETYP_REF_DLM                                       as                                       ETYP_REF_DLM
		, ETYP_REF_ULM                                       as                                       ETYP_REF_ULM
		, ETYP_REF_DSC                                       as                                       ETYP_REF_DSC
		, ETYP_REF_ENT_DTE                                   as                                   ETYP_REF_ENT_DTE
		, ETYP_REF_ENT_UID                                   as                                   ETYP_REF_ENT_UID
		, ETYP_REF_RID                                       as                                       ETYP_REF_RID 
				FROM     LOGIC_EOB   ), 
RENAME_REF as ( SELECT 
		  REF_DGN                                            as                                            REF_DGN
		, REF_IDN                                            as                                            REF_IDN
		, REF_EFF_DTE                                        as                                        REF_EFF_DTE
		, REF_EXP_DTE                                        as                                        REF_EXP_DTE
		, REF_DLM                                            as                                            REF_DLM
		, REF_ULM                                            as                                            REF_ULM
		, REF_DSC                                            as                                            REF_DSC
		, REF_ENT_DTE                                        as                                        REF_ENT_DTE
		, REF_ENT_UID                                        as                                        REF_ENT_UID
		, REF_RID                                            as                                            REF_RID 
				FROM     LOGIC_REF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_EOB                            as ( SELECT * from    RENAME_EOB  
				QUALIFY(ROW_NUMBER()OVER (PARTITION BY CODE ORDER BY EFFECTIVE_DATE DESC,NVL(EXPIRATION_DATE, '12/31/2099') DESC))=1  ),
FILTER_REF                            as ( SELECT * from    RENAME_REF 
				WHERE REF_DGN = 'EOA' AND REF_EXP_DTE > CURRENT_DATE  ),

---- JOIN LAYER ----

EOB as ( SELECT * 
				FROM  FILTER_EOB
				LEFT JOIN FILTER_REF ON  FILTER_EOB.APPLIED_BY =  FILTER_REF.REF_IDN  )
SELECT 
md5(cast(
    
    coalesce(cast(CODE as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
 ,  * 
from EOB
      );
    