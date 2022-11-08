---- SRC LAYER ----
WITH
SRC_BASE as ( SELECT *     from     DEV_VIEWS.BASE.EOB_BASE ),
SRC_GEN as ( SELECT *     from     DEV_VIEWS.BASE.EOB_GENERAL ),
SRC_ECAT as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
SRC_ETYP as ( SELECT *     from     DEV_VIEWS.BASE.REF ),
//SRC_BASE as ( SELECT *     from     BASE.EOB_BASE) ,
//SRC_GEN as ( SELECT *     from     BASE.EOB_GENERAL) ,
//SRC_ECAT as ( SELECT *     from     BASE.REF) ,
//SRC_ETYP as ( SELECT *     from     BASE.REF) ,

---- LOGIC LAYER ----

LOGIC_BASE as ( SELECT 
		  EOB_RID                                            AS                                            EOB_RID 
		, upper( TRIM( CODE ) )                              AS                                               CODE 
		, upper( TRIM( CATEGORY ) )                          AS                                           CATEGORY 
		, upper( TRIM( EOB_TYPE ) )                          AS                                           EOB_TYPE 
		, upper( TRIM( APPLIED_BY ) )                        AS                                         APPLIED_BY 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		from SRC_BASE
            ),
LOGIC_GEN as ( SELECT 
		  EOB_RID                                            AS                                            EOB_RID 
		, cast( EFFECTIVE_DATE as DATE )                     AS                                     EFFECTIVE_DATE 
		, cast( EXPIRATION_DATE as DATE )                    AS                                    EXPIRATION_DATE 
		, upper( TRIM( SHORT_DESCRIPTION ) )                 AS                                  SHORT_DESCRIPTION 
		, upper( TRIM( LONG_DESCRIPTION ) )                  AS                                   LONG_DESCRIPTION 
		, cast( ENTRY_DATE as DATE )                         AS                                         ENTRY_DATE 
		, upper( TRIM( ENTRY_USER_ID ) )                     AS                                      ENTRY_USER_ID 
		, cast( DLM as DATE )                                AS                                                DLM 
		, upper( TRIM( ULM ) )                               AS                                                ULM 
		from SRC_GEN
            ),
LOGIC_ECAT as ( SELECT 
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
		from SRC_ECAT
            ),
LOGIC_ETYP as ( SELECT 
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
		from SRC_ETYP
            )

---- RENAME LAYER ----
,

RENAME_BASE as ( SELECT 
		  EOB_RID                                            as                                            EOB_RID
		, CODE                                               as                                               CODE
		, CATEGORY                                           as                                           CATEGORY
		, EOB_TYPE                                           as                                           EOB_TYPE
		, APPLIED_BY                                         as                                         APPLIED_BY
		, ENTRY_DATE                                         as                                         ENTRY_DATE
		, ENTRY_USER_ID                                      as                                      ENTRY_USER_ID 
				FROM     LOGIC_BASE   ), 
RENAME_GEN as ( SELECT 
		  EOB_RID                                            as                                        GEN_EOB_RID
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE
		, SHORT_DESCRIPTION                                  as                                  SHORT_DESCRIPTION
		, LONG_DESCRIPTION                                   as                                   LONG_DESCRIPTION
		, ENTRY_DATE                                         as                                     GEN_ENTRY_DATE
		, ENTRY_USER_ID                                      as                                  GEN_ENTRY_USER_ID
		, DLM                                                as                                                DLM
		, ULM                                                as                                                ULM 
				FROM     LOGIC_GEN   ), 
RENAME_ECAT as ( SELECT 
		  REF_DGN                                            as                                       ECAT_REF_DGN
		, REF_IDN                                            as                                       ECAT_REF_IDN
		, REF_EFF_DTE                                        as                                   ECAT_REF_EFF_DTE
		, REF_EXP_DTE                                        as                                   ECAT_REF_EXP_DTE
		, REF_DLM                                            as                                       ECAT_REF_DLM
		, REF_ULM                                            as                                       ECAT_REF_ULM
		, REF_DSC                                            as                                       ECAT_REF_DSC
		, REF_ENT_DTE                                        as                                   ECAT_REF_ENT_DTE
		, REF_ENT_UID                                        as                                   ECAT_REF_ENT_UID
		, REF_RID                                            as                                       ECAT_REF_RID 
				FROM     LOGIC_ECAT   ), 
RENAME_ETYP as ( SELECT 
		  REF_DGN                                            as                                       ETYP_REF_DGN
		, REF_IDN                                            as                                       ETYP_REF_IDN
		, REF_EFF_DTE                                        as                                   ETYP_REF_EFF_DTE
		, REF_EXP_DTE                                        as                                   ETYP_REF_EXP_DTE
		, REF_DLM                                            as                                       ETYP_REF_DLM
		, REF_ULM                                            as                                       ETYP_REF_ULM
		, REF_DSC                                            as                                       ETYP_REF_DSC
		, REF_ENT_DTE                                        as                                   ETYP_REF_ENT_DTE
		, REF_ENT_UID                                        as                                   ETYP_REF_ENT_UID
		, REF_RID                                            as                                       ETYP_REF_RID 
				FROM     LOGIC_ETYP   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_BASE                           as ( SELECT * from    RENAME_BASE   ),
FILTER_GEN                            as ( SELECT * from    RENAME_GEN   ),
FILTER_ECAT                           as ( SELECT * from    RENAME_ECAT 
				WHERE ECAT_REF_DGN = 'EOC' AND ECAT_REF_EXP_DTE>CURRENT_DATE  ),
FILTER_ETYP                           as ( SELECT * from    RENAME_ETYP 
				WHERE ETYP_REF_DGN = 'EOT' AND ETYP_REF_EXP_DTE>CURRENT_DATE  ),

---- JOIN LAYER ----

BASE as ( SELECT * 
				FROM  FILTER_BASE
				LEFT JOIN FILTER_GEN ON  FILTER_BASE.EOB_RID =  FILTER_GEN.GEN_EOB_RID 
						LEFT JOIN FILTER_ECAT ON  FILTER_BASE.CATEGORY  =  FILTER_ECAT.ECAT_REF_IDN 
						LEFT JOIN FILTER_ETYP ON  FILTER_BASE.EOB_TYPE =  FILTER_ETYP.ETYP_REF_IDN  )
SELECT * 
from BASE