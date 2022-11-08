
  create or replace  view DEV_EDW.STAGING.DSV_ORGANIZATIONAL_UNIT  as (
    

---- SRC LAYER ----
WITH
SRC_O as ( SELECT *     from     STAGING.DST_ORGANIZATIONAL_UNIT ),
//SRC_O as ( SELECT *     from     DST_ORGANIZATIONAL_UNIT) ,

---- LOGIC LAYER ----

LOGIC_O as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID 
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM 
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM 
		, ORG_UNT_SRT_NO                                     as                                     ORG_UNT_SRT_NO 
		, ORG_UNT_EFF_DATE                                   as                                   ORG_UNT_EFF_DATE 
		, ORG_UNT_END_DATE                                   as                                   ORG_UNT_END_DATE 
		, ORG_UNT_TYP_CD                                     as                                     ORG_UNT_TYP_CD 
		, ORG_UNT_TYP_NM                                     as                                     ORG_UNT_TYP_NM 
		, ORG_UNT_TYP_ASGN_FNCT_IND                          as                          ORG_UNT_TYP_ASGN_FNCT_IND 
		, ORG_UNT_VSBL_IND                                   as                                   ORG_UNT_VSBL_IND 
		, ORG_UNT_TYP_LVL_NO                                 as                                 ORG_UNT_TYP_LVL_NO 
		, ORG_UNT_PAR_ID                                     as                                     ORG_UNT_PAR_ID 
		, ORG_UNT_CST_CD                                     as                                     ORG_UNT_CST_CD 
		, ORG_UNT_ISO_CLM_ADMN_OFC_CD                        as                        ORG_UNT_ISO_CLM_ADMN_OFC_CD 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, ORG_UNT_VOID_IND                                   as                                   ORG_UNT_VOID_IND 
		, ORG_UNT_AUTO_COMP_WK_ITEM_IND                      as                      ORG_UNT_AUTO_COMP_WK_ITEM_IND 
		from SRC_O
            )

---- RENAME LAYER ----
,

RENAME_O as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ORG_UNT_ID                                         as                                         ORG_UNT_ID
		, ORG_UNT_NM                                         as                           ORGANIZATIONAL_UNIT_NAME
		, ORG_UNT_ABRV_NM                                    as                                SERVICE_OFFICE_NAME
		, ORG_UNT_SRT_NO                                     as                                     ORG_UNT_SRT_NO
		, ORG_UNT_EFF_DATE                                   as                                   ORG_UNT_EFF_DATE
		, ORG_UNT_END_DATE                                   as                                   ORG_UNT_END_DATE
		, ORG_UNT_TYP_CD                                     as                                     ORG_UNT_TYP_CD
		, ORG_UNT_TYP_NM                                     as                                     ORG_UNT_TYP_NM
		, ORG_UNT_TYP_ASGN_FNCT_IND                          as                          ORG_UNT_TYP_ASGN_FNCT_IND
		, ORG_UNT_VSBL_IND                                   as                                   ORG_UNT_VSBL_IND
		, ORG_UNT_TYP_LVL_NO                                 as                                 ORG_UNT_TYP_LVL_NO
		, ORG_UNT_PAR_ID                                     as                                     ORG_UNT_PAR_ID
		, ORG_UNT_CST_CD                                     as                                     ORG_UNT_CST_CD
		, ORG_UNT_ISO_CLM_ADMN_OFC_CD                        as                        ORG_UNT_ISO_CLM_ADMN_OFC_CD
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, ORG_UNT_VOID_IND                                   as                                   ORG_UNT_VOID_IND
		, ORG_UNT_AUTO_COMP_WK_ITEM_IND                      as                      ORG_UNT_AUTO_COMP_WK_ITEM_IND 
				FROM     LOGIC_O   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_O                              as ( SELECT * from    RENAME_O   ),

---- JOIN LAYER ----

 JOIN_O  as  ( SELECT * 
				FROM  FILTER_O )
 SELECT * FROM  JOIN_O
  );
