

      create or replace  table DEV_EDW.STAGING.STG_POLICY_CONTROL_ELEMENT  as
      (---- SRC LAYER ----
WITH
SRC_PCE as ( SELECT *     from     DEV_VIEWS.PCMP.POLICY_CONTROL_ELEMENT ),
SRC_CET as ( SELECT *     from     DEV_VIEWS.PCMP.CONTROL_ELEMENT_TYPE ),
SRC_CEST as ( SELECT *     from     DEV_VIEWS.PCMP.CONTROL_ELEMENT_SUB_TYPE ),
//SRC_PCE as ( SELECT *     from     POLICY_CONTROL_ELEMENT) ,
//SRC_CET as ( SELECT *     from     CONTROL_ELEMENT_TYPE) ,
//SRC_CEST as ( SELECT *     from     CONTROL_ELEMENT_SUB_TYPE) ,

---- LOGIC LAYER ----

LOGIC_PCE as ( SELECT 
		  PLCY_CTL_ELEM_ID                                   AS                                   PLCY_CTL_ELEM_ID 
		, PLCY_PRD_ID                                        AS                                        PLCY_PRD_ID 
		, upper( TRIM( CTL_ELEM_TYP_CD ) )                   AS                                    CTL_ELEM_TYP_CD 
		, CTL_ELEM_SUB_TYP_ID                                AS                                CTL_ELEM_SUB_TYP_ID 
		, cast( PLCY_CTL_ELEM_EFF_DT as DATE )               AS                               PLCY_CTL_ELEM_EFF_DT 
		, cast( PLCY_CTL_ELEM_END_DT as DATE )               AS                               PLCY_CTL_ELEM_END_DT 
		, upper( PLCY_CTL_ELEM_RN_IND )                      AS                               PLCY_CTL_ELEM_RN_IND 
		, upper( PLCY_CTL_ELEM_NOTE_IND )                    AS                             PLCY_CTL_ELEM_NOTE_IND 
		, upper( MNL_PAY_PLN_OVRRD_IND )                     AS                              MNL_PAY_PLN_OVRRD_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_PCE
            ),
LOGIC_CET as ( SELECT 
		  upper( TRIM( CTL_ELEM_TYP_NM ) )                   AS                                    CTL_ELEM_TYP_NM 
		, upper( TRIM( CTL_ELEM_TYP_CD ) )                   AS                                    CTL_ELEM_TYP_CD 
		, upper( CTL_ELEM_TYP_VOID_IND )                     AS                              CTL_ELEM_TYP_VOID_IND 
		from SRC_CET
            ),
LOGIC_CEST as ( SELECT 
		  upper( TRIM( CTL_ELEM_SUB_TYP_CD ) )               AS                                CTL_ELEM_SUB_TYP_CD 
		, upper( TRIM( CTL_ELEM_SUB_TYP_NM ) )               AS                                CTL_ELEM_SUB_TYP_NM 
		, upper( CTL_ELEM_SUB_TYP_WEB_DSPLY_IND )            AS                     CTL_ELEM_SUB_TYP_WEB_DSPLY_IND 
		, upper( CTL_ELEM_SUB_TYP_WEB_DFLT_IND )             AS                      CTL_ELEM_SUB_TYP_WEB_DFLT_IND 
		, CTL_ELEM_SUB_TYP_ID                                AS                                CTL_ELEM_SUB_TYP_ID 
		, upper( CTL_ELEM_SUB_TYP_VOID_IND )                 AS                          CTL_ELEM_SUB_TYP_VOID_IND 
		from SRC_CEST
            )

---- RENAME LAYER ----
,

RENAME_PCE as ( SELECT 
		  PLCY_CTL_ELEM_ID                                   as                                   PLCY_CTL_ELEM_ID
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
		, CTL_ELEM_SUB_TYP_ID                                as                                CTL_ELEM_SUB_TYP_ID
		, PLCY_CTL_ELEM_EFF_DT                               as                               PLCY_CTL_ELEM_EFF_DT
		, PLCY_CTL_ELEM_END_DT                               as                               PLCY_CTL_ELEM_END_DT
		, PLCY_CTL_ELEM_RN_IND                               as                               PLCY_CTL_ELEM_RN_IND
		, PLCY_CTL_ELEM_NOTE_IND                             as                             PLCY_CTL_ELEM_NOTE_IND
		, MNL_PAY_PLN_OVRRD_IND                              as                              MNL_PAY_PLN_OVRRD_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_PCE   ), 
RENAME_CET as ( SELECT 
		  CTL_ELEM_TYP_NM                                    as                                    CTL_ELEM_TYP_NM
		, CTL_ELEM_TYP_CD                                    as                                CET_CTL_ELEM_TYP_CD
		, CTL_ELEM_TYP_VOID_IND                              as                              CTL_ELEM_TYP_VOID_IND 
				FROM     LOGIC_CET   ), 
RENAME_CEST as ( SELECT 
		  CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD
		, CTL_ELEM_SUB_TYP_NM                                as                                CTL_ELEM_SUB_TYP_NM
		, CTL_ELEM_SUB_TYP_WEB_DSPLY_IND                     as                     CTL_ELEM_SUB_TYP_WEB_DSPLY_IND
		, CTL_ELEM_SUB_TYP_WEB_DFLT_IND                      as                      CTL_ELEM_SUB_TYP_WEB_DFLT_IND
		, CTL_ELEM_SUB_TYP_ID                                as                           CEST_CTL_ELEM_SUB_TYP_ID
		, CTL_ELEM_SUB_TYP_VOID_IND                          as                          CTL_ELEM_SUB_TYP_VOID_IND 
				FROM     LOGIC_CEST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCE                            as ( SELECT * from    RENAME_PCE   ),
FILTER_CET                            as ( SELECT * from    RENAME_CET 
				WHERE CTL_ELEM_TYP_VOID_IND = 'N'  ),
FILTER_CEST                           as ( SELECT * from    RENAME_CEST 
				WHERE CTL_ELEM_SUB_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

PCE as ( SELECT * 
				FROM  FILTER_PCE
				INNER JOIN FILTER_CET ON  FILTER_PCE.CTL_ELEM_TYP_CD =  FILTER_CET.CET_CTL_ELEM_TYP_CD 
						INNER JOIN FILTER_CEST ON  FILTER_PCE.CTL_ELEM_SUB_TYP_ID =  FILTER_CEST.CEST_CTL_ELEM_SUB_TYP_ID  )
SELECT * 
from PCE
      );
    