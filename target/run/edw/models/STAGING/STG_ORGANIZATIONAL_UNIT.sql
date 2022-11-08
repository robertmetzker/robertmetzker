

      create or replace  table DEV_EDW.STAGING.STG_ORGANIZATIONAL_UNIT  as
      (---- SRC LAYER ----
WITH
SRC_O as ( SELECT *     from     DEV_VIEWS.PCMP.ORGANIZATIONAL_UNIT ),
SRC_OT as ( SELECT *     from      DEV_VIEWS.PCMP.ORGANIZATIONAL_UNIT_TYPE ),
//SRC_O as ( SELECT *     from    DEV_VIEWS.PCMP.ORGANIZATIONAL_UNIT ), 
//SRC_OT as ( SELECT *     from    DEV_VIEWS.PCMP.ORGANIZATIONAL_UNIT_TYPE ),

---- LOGIC LAYER ----

LOGIC_O as ( SELECT 
		  ORG_UNT_ID                                         as                                         ORG_UNT_ID 
		, upper( TRIM( ORG_UNT_NM ) )                        as                                         ORG_UNT_NM 
		, upper( TRIM( ORG_UNT_ABRV_NM ) )                   as                                    ORG_UNT_ABRV_NM 
		, ORG_UNT_SRT_NO                                     as                                     ORG_UNT_SRT_NO 
		, cast( ORG_UNT_EFF_DT as DATE )                     as                                     ORG_UNT_EFF_DT 
		, cast( ORG_UNT_END_DT as DATE )                     as                                     ORG_UNT_END_DT 
		, upper( TRIM( ORG_UNT_TYP_CD ) )                    as                                     ORG_UNT_TYP_CD 
		, ORG_UNT_PAR_ID                                     as                                     ORG_UNT_PAR_ID 
		, upper( TRIM( ORG_UNT_CST_CD ) )                    as                                     ORG_UNT_CST_CD 
		, upper( TRIM( ORG_UNT_ISO_CLM_ADMN_OFC_CD ) )       as                        ORG_UNT_ISO_CLM_ADMN_OFC_CD 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( ORG_UNT_VOID_IND ) )                  as                                   ORG_UNT_VOID_IND 
		, upper( TRIM( ORG_UNT_AUTO_COMP_WK_ITEM_IND ) )     as                      ORG_UNT_AUTO_COMP_WK_ITEM_IND 
		from SRC_O
            ),
LOGIC_OT as ( SELECT 
		  upper( TRIM( ORG_UNT_TYP_NM ) )                    as                                     ORG_UNT_TYP_NM 
		, upper( TRIM( ORG_UNT_TYP_ASGN_FNCT_IND ) )         as                          ORG_UNT_TYP_ASGN_FNCT_IND 
		, upper( TRIM( ORG_UNT_VSBL_IND ) )                  as                                   ORG_UNT_VSBL_IND 
		, ORG_UNT_TYP_LVL_NO                                 as                                 ORG_UNT_TYP_LVL_NO 
		, upper( TRIM( ORG_UNT_TYP_CD ) )                    as                                     ORG_UNT_TYP_CD 
		, upper( TRIM( ORG_UNT_TYP_VOID_IND ) )              as                               ORG_UNT_TYP_VOID_IND 
		from SRC_OT
            )

---- RENAME LAYER ----
,

RENAME_O as ( SELECT 
		  ORG_UNT_ID                                         as                                         ORG_UNT_ID
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM
		, ORG_UNT_SRT_NO                                     as                                     ORG_UNT_SRT_NO
		, ORG_UNT_EFF_DT                                     as                                   ORG_UNT_EFF_DATE
		, ORG_UNT_END_DT                                     as                                   ORG_UNT_END_DATE
		, ORG_UNT_TYP_CD                                     as                                     ORG_UNT_TYP_CD
		, ORG_UNT_PAR_ID                                     as                                     ORG_UNT_PAR_ID
		, ORG_UNT_CST_CD                                     as                                     ORG_UNT_CST_CD
		, ORG_UNT_ISO_CLM_ADMN_OFC_CD                        as                        ORG_UNT_ISO_CLM_ADMN_OFC_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, ORG_UNT_VOID_IND                                   as                                   ORG_UNT_VOID_IND
		, ORG_UNT_AUTO_COMP_WK_ITEM_IND                      as                      ORG_UNT_AUTO_COMP_WK_ITEM_IND 
				FROM     LOGIC_O   ), 
RENAME_OT as ( SELECT 
		  ORG_UNT_TYP_NM                                     as                                     ORG_UNT_TYP_NM
		, ORG_UNT_TYP_ASGN_FNCT_IND                          as                          ORG_UNT_TYP_ASGN_FNCT_IND
		, ORG_UNT_VSBL_IND                                   as                                   ORG_UNT_VSBL_IND
		, ORG_UNT_TYP_LVL_NO                                 as                                 ORG_UNT_TYP_LVL_NO
		, ORG_UNT_TYP_CD                                     as                                  OT_ORG_UNT_TYP_CD
		, ORG_UNT_TYP_VOID_IND                               as                               ORG_UNT_TYP_VOID_IND 
				FROM     LOGIC_OT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_O                              as ( SELECT * from    RENAME_O   ),
FILTER_OT                             as ( SELECT * from    RENAME_OT 
                                            WHERE ORG_UNT_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

O as ( SELECT * 
				FROM  FILTER_O
				LEFT JOIN FILTER_OT ON  FILTER_O.ORG_UNT_TYP_CD =  FILTER_OT.OT_ORG_UNT_TYP_CD  )
SELECT 
		  ORG_UNT_ID
		, ORG_UNT_NM
		, ORG_UNT_ABRV_NM
		, ORG_UNT_SRT_NO
		, ORG_UNT_EFF_DATE
		, ORG_UNT_END_DATE
		, ORG_UNT_TYP_CD
		, ORG_UNT_TYP_NM
		, ORG_UNT_TYP_ASGN_FNCT_IND
		, ORG_UNT_VSBL_IND
		, ORG_UNT_TYP_LVL_NO
		, ORG_UNT_PAR_ID
		, ORG_UNT_CST_CD
		, ORG_UNT_ISO_CLM_ADMN_OFC_CD
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, ORG_UNT_VOID_IND
		, ORG_UNT_AUTO_COMP_WK_ITEM_IND 
from O
      );
    