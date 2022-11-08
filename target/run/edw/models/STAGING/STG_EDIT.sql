

      create or replace  table DEV_EDW.STAGING.STG_EDIT  as
      (---- SRC LAYER ----
WITH
SRC_BASE as ( SELECT *     from     DEV_VIEWS.BASE.EDIT_BASE ),
SRC_GEN as ( SELECT *     from     DEV_VIEWS.BASE.EDIT_GENERAL ),
SRC_R1 as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_R2 as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_R3 as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_R4 as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
//SRC_BASE as ( SELECT *     from     EDIT_BASE) ,
//SRC_GEN as ( SELECT *     from     EDIT_GENERAL) ,
//SRC_R1 as ( SELECT *     from     REF) ,
//SRC_R2 as ( SELECT *     from     REF) ,
//SRC_R3 as ( SELECT *     from     REF) ,
//SRC_R4 as ( SELECT *     from     REF) ,

---- LOGIC LAYER ----

LOGIC_BASE as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		, upper( TRIM( CODE ) )                              AS                                               CODE 
		, upper( TRIM( CATEGORY ) )                          AS                                           CATEGORY 
		, upper( TRIM( APPLIED_BY ) )                        AS                                         APPLIED_BY 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		, upper( DATE_RANGE_TYPE )                           AS                                    DATE_RANGE_TYPE 
		from SRC_BASE
            ),
LOGIC_GEN as ( SELECT 
		  EDIT_RID                                           AS                                           EDIT_RID 
		, cast( EFFECTIVE_DATE as DATE )                     AS                                     EFFECTIVE_DATE 
		, cast( EXPIRATION_DATE as DATE )                    AS                                    EXPIRATION_DATE 
		, upper( TRIM( DISPOSITION ) )                       AS                                        DISPOSITION 
		, upper( TRIM( SHORT_DESCRIPTION ) )                 AS                                  SHORT_DESCRIPTION 
		, upper( TRIM( LONG_DESCRIPTION ) )                  AS                                   LONG_DESCRIPTION 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		, cast( DLM as DATE )                                AS                                                DLM 
		, upper( TRIM( ULM ) )                               AS                                                ULM 
		, upper( TRIM( HPPN_APPLICABLE ) )                   AS                                    HPPN_APPLICABLE 
		, EDITOR_PHASE                                       AS                                       EDITOR_PHASE 
		, PAYMENT_PCT                                        AS                                        PAYMENT_PCT 
		from SRC_GEN
            ),
LOGIC_R1 as ( SELECT 
		  upper( TRIM( REF_DGN ) )                           AS                                            REF_DGN 
		, upper( TRIM( REF_IDN ) )                           AS                                            REF_IDN 
		, cast( REF_EFF_DTE as DATE )                        AS                                        REF_EFF_DTE 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		, cast( REF_DLM as DATE )                            AS                                            REF_DLM 
		, upper( TRIM( REF_ULM ) )                           AS                                            REF_ULM 
		, upper( TRIM( REF_DSC ) )                           AS                                            REF_DSC 
		, cast( REF_ENT_DTE as DATE )                        AS                                        REF_ENT_DTE 
		, upper( TRIM( REF_ENT_UID ) )                       AS                                        REF_ENT_UID 
		, REF_RID                                            AS                                            REF_RID 
		from SRC_R1
            ),
LOGIC_R2 as ( SELECT 
		  upper( TRIM( REF_DGN ) )                           AS                                            REF_DGN 
		, upper( TRIM( REF_IDN ) )                           AS                                            REF_IDN 
		, cast( REF_EFF_DTE as DATE )                        AS                                        REF_EFF_DTE 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		, cast( REF_DLM as DATE )                            AS                                            REF_DLM 
		, upper( TRIM( REF_ULM ) )                           AS                                            REF_ULM 
		, upper( TRIM( REF_DSC ) )                           AS                                            REF_DSC 
		, cast( REF_ENT_DTE as DATE )                        AS                                        REF_ENT_DTE 
		, upper( TRIM( REF_ENT_UID ) )                       AS                                        REF_ENT_UID 
		, REF_RID                                            AS                                            REF_RID 
		from SRC_R2
            ),
LOGIC_R3 as ( SELECT 
		  upper( TRIM( REF_DGN ) )                           AS                                            REF_DGN 
		, upper( TRIM( REF_IDN ) )                           AS                                            REF_IDN 
		, cast( REF_EFF_DTE as DATE )                        AS                                        REF_EFF_DTE 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		, cast( REF_DLM as DATE )                            AS                                            REF_DLM 
		, upper( TRIM( REF_ULM ) )                           AS                                            REF_ULM 
		, upper( TRIM( REF_DSC ) )                           AS                                            REF_DSC 
		, cast( REF_ENT_DTE as DATE )                        AS                                        REF_ENT_DTE 
		, upper( TRIM( REF_ENT_UID ) )                       AS                                        REF_ENT_UID 
		, REF_RID                                            AS                                            REF_RID 
		from SRC_R3
            ),
LOGIC_R4 as ( SELECT 
		  upper( TRIM( REF_DGN ) )                           AS                                            REF_DGN 
		, upper( TRIM( REF_IDN ) )                           AS                                            REF_IDN 
		, cast( REF_EFF_DTE as DATE )                        AS                                        REF_EFF_DTE 
		, cast( REF_EXP_DTE as DATE )                        AS                                        REF_EXP_DTE 
		, cast( REF_DLM as DATE )                            AS                                            REF_DLM 
		, upper( TRIM( REF_ULM ) )                           AS                                            REF_ULM 
		, upper( TRIM( REF_DSC ) )                           AS                                            REF_DSC 
		, cast( REF_ENT_DTE as DATE )                        AS                                        REF_ENT_DTE 
		, upper( TRIM( REF_ENT_UID ) )                       AS                                        REF_ENT_UID 
		, REF_RID                                            AS                                            REF_RID 
		from SRC_R4
            )

---- RENAME LAYER ----
,

RENAME_BASE as ( SELECT 
		  EDIT_RID                                           as                                           EDIT_RID
		, CODE                                               as                                               CODE
		, CATEGORY                                           as                                           CATEGORY
		, APPLIED_BY                                         as                                         APPLIED_BY
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID
		, DATE_RANGE_TYPE                                    as                                    DATE_RANGE_TYPE 
				FROM     LOGIC_BASE   ), 
RENAME_GEN as ( SELECT 
		  EDIT_RID                                           as                                       GEN_EDIT_RID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, DISPOSITION                                        as                                        DISPOSITION
		, SHORT_DESCRIPTION                                  as                                  SHORT_DESCRIPTION
		, LONG_DESCRIPTION                                   as                                   LONG_DESCRIPTION
		, ENTRY_DATE                                         as                                     GEN_ENTRY_DATE
		, ENTRY_USER_ID                                      as                                  GEN_ENTRY_USER_ID
		, DLM                                                as                                                DLM
		, ULM                                                as                                                ULM
		, HPPN_APPLICABLE                                    as                                    HPPN_APPLICABLE
		, EDITOR_PHASE                                       as                                       EDITOR_PHASE
		, PAYMENT_PCT                                        as                                        PAYMENT_PCT 
				FROM     LOGIC_GEN   ), 
RENAME_R1 as ( SELECT 
		  REF_DGN                                            as                                            REF_DGN
		, REF_IDN                                            as                                            REF_IDN
		, REF_EFF_DTE                                        as                                        REF_EFF_DTE
		, REF_EXP_DTE                                        as                                        REF_EXP_DTE
		, REF_DLM                                            as                                            REF_DLM
		, REF_ULM                                            as                                            REF_ULM
		, REF_DSC                                            as                                       CATEGORY_DSC
		, REF_ENT_DTE                                        as                                        REF_ENT_DTE
		, REF_ENT_UID                                        as                                        REF_ENT_UID
		, REF_RID                                            as                                            REF_RID 
				FROM     LOGIC_R1   ), 
RENAME_R2 as ( SELECT 
		  REF_DGN                                            as                                         R2_REF_DGN
		, REF_IDN                                            as                                         R2_REF_IDN
		, REF_EFF_DTE                                        as                                     R2_REF_EFF_DTE
		, REF_EXP_DTE                                        as                                     R2_REF_EXP_DTE
		, REF_DLM                                            as                                         R2_REF_DLM
		, REF_ULM                                            as                                         R2_REF_ULM
		, REF_DSC                                            as                                DATE_RANGE_TYPE_DSC
		, REF_ENT_DTE                                        as                                     R2_REF_ENT_DTE
		, REF_ENT_UID                                        as                                     R2_REF_ENT_UID
		, REF_RID                                            as                                         R2_REF_RID 
				FROM     LOGIC_R2   ), 
RENAME_R3 as ( SELECT 
		  REF_DGN                                            as                                         R3_REF_DGN
		, REF_IDN                                            as                                         R3_REF_IDN
		, REF_EFF_DTE                                        as                                     R3_REF_EFF_DTE
		, REF_EXP_DTE                                        as                                     R3_REF_EXP_DTE
		, REF_DLM                                            as                                         R3_REF_DLM
		, REF_ULM                                            as                                         R3_REF_ULM
		, REF_DSC                                            as                                     APPLIED_BY_DSC
		, REF_ENT_DTE                                        as                                     R3_REF_ENT_DTE
		, REF_ENT_UID                                        as                                     R3_REF_ENT_UID
		, REF_RID                                            as                                         R3_REF_RID 
				FROM     LOGIC_R3   ), 
RENAME_R4 as ( SELECT 
		  REF_DGN                                            as                                         R4_REF_DGN
		, REF_IDN                                            as                                         R4_REF_IDN
		, REF_EFF_DTE                                        as                                     R4_REF_EFF_DTE
		, REF_EXP_DTE                                        as                                     R4_REF_EXP_DTE
		, REF_DLM                                            as                                         R4_REF_DLM
		, REF_ULM                                            as                                         R4_REF_ULM
		, REF_DSC                                            as                                    DISPOSITION_DSC
		, REF_ENT_DTE                                        as                                     R4_REF_ENT_DTE
		, REF_ENT_UID                                        as                                     R4_REF_ENT_UID
		, REF_RID                                            as                                         R4_REF_RID 
				FROM     LOGIC_R4   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_BASE                           as ( SELECT * from    RENAME_BASE   ),
FILTER_GEN                            as ( SELECT * from    RENAME_GEN   ),
FILTER_R1                             as ( SELECT * from    RENAME_R1 
				WHERE REF_DGN = 'EDC' AND REF_EXP_DTE > CURRENT_DATE ),
FILTER_R2                             as ( SELECT * from    RENAME_R2 
				WHERE R2_REF_DGN = 'EET' AND R2_REF_EXP_DTE > CURRENT_DATE  ),
FILTER_R3                             as ( SELECT * from    RENAME_R3 
				WHERE R3_REF_DGN = 'EAP' AND R3_REF_EXP_DTE > CURRENT_DATE  ),
FILTER_R4                             as ( SELECT * from    RENAME_R4 
				WHERE R4_REF_DGN = 'EDS' AND R4_REF_EXP_DTE > CURRENT_DATE  ),

---- JOIN LAYER ----

BASE as ( SELECT * 
				FROM  FILTER_BASE
				LEFT JOIN FILTER_GEN ON  FILTER_BASE.EDIT_RID = FILTER_GEN.GEN_EDIT_RID 
						LEFT JOIN FILTER_R1 ON  FILTER_BASE.CATEGORY =  FILTER_R1.REF_IDN 
						LEFT JOIN FILTER_R2 ON  FILTER_BASE.DATE_RANGE_TYPE =  FILTER_R2.R2_REF_IDN 
						LEFT JOIN FILTER_R3 ON  FILTER_BASE.APPLIED_BY =  FILTER_R3.R3_REF_IDN 
						LEFT JOIN FILTER_R4 ON  FILTER_GEN.DISPOSITION =  FILTER_R4.R4_REF_IDN )
SELECT * 
from BASE
      );
    