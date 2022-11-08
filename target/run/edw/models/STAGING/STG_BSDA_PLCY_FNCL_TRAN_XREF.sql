

      create or replace  table DEV_EDW.STAGING.STG_BSDA_PLCY_FNCL_TRAN_XREF  as
      (---- SRC LAYER ----
WITH
SRC_X as ( SELECT *     from     DEV_VIEWS.PCMP.BSDA_PLCY_FNCL_TRAN_XREF ),
//SRC_X as ( SELECT *     from     BSDA_PLCY_FNCL_TRAN_XREF) ,

---- LOGIC LAYER ----

LOGIC_X as ( SELECT 
		  BASAPFT_ID                                         AS                                         BASAPFT_ID 
		, BILL_SCH_DTL_AMT_ID                                AS                                BILL_SCH_DTL_AMT_ID 
		, PFT_ID                                             AS                                             PFT_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_X
            )

---- RENAME LAYER ----
,

RENAME_X as ( SELECT 
		  BASAPFT_ID                                         as                                         BASAPFT_ID
		, BILL_SCH_DTL_AMT_ID                                as                                BILL_SCH_DTL_AMT_ID
		, PFT_ID                                             as                                             PFT_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_X   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_X                              as ( SELECT * from    RENAME_X ),

---- JOIN LAYER ----

 JOIN_X  as  ( SELECT * 
				FROM  FILTER_X )
 SELECT * FROM  JOIN_X
      );
    