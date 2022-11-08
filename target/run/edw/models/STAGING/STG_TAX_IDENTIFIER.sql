

      create or replace  table DEV_EDW.STAGING.STG_TAX_IDENTIFIER  as
      (---- SRC LAYER ----
WITH
SRC_TAX as ( SELECT *     from     DEV_VIEWS.PCMP.TAX_IDENTIFIER ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_TT as ( SELECT *     from     DEV_VIEWS.PCMP.TAX_IDENTIFIER_TYPE ),
//SRC_TAX as ( SELECT *     from     TAX_IDENTIFIER) ,
//SRC_C as ( SELECT *     from     CUSTOMER) ,
//SRC_TT as ( SELECT *     from     TAX_IDENTIFIER_TYPE) ,

---- LOGIC LAYER ----

LOGIC_TAX as ( SELECT 
		  TAX_ID_ID                                          AS                                          TAX_ID_ID 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( TAX_ID_TYP_CD ) )                     AS                                      TAX_ID_TYP_CD 
		, upper( TRIM( TAX_ID_NO ) )                         AS                                          TAX_ID_NO 
		, upper( TRIM( TAX_ID_DRV_UPCS_NO ) )                AS                                 TAX_ID_DRV_UPCS_NO 
		, upper( TRIM( TAX_ID_SEQ_NO ) )                     AS                                      TAX_ID_SEQ_NO 
		, cast( TAX_ID_EFF_DT as DATE )                      AS                                      TAX_ID_EFF_DT 
		, cast( TAX_ID_END_DT as DATE )                      AS                                      TAX_ID_END_DT 
		, upper( TAX_ID_USE_FOR_1099_IND )                   AS                            TAX_ID_USE_FOR_1099_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_TAX
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_C
            ),
LOGIC_TT as ( SELECT 
		  upper( TRIM( TAX_ID_TYP_NM ) )                     AS                                      TAX_ID_TYP_NM 
		, upper( TRIM( TAX_ID_TYP_CD ) )                     AS                                      TAX_ID_TYP_CD 
		, upper( TAX_ID_TYP_VOID_IND )                       AS                                TAX_ID_TYP_VOID_IND 
		from SRC_TT
            )

---- RENAME LAYER ----
,

RENAME_TAX as ( SELECT 
		  TAX_ID_ID                                          as                                          TAX_ID_ID
		, CUST_ID                                            as                                            CUST_ID
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD
		, TAX_ID_NO                                          as                                          TAX_ID_NO
		, TAX_ID_DRV_UPCS_NO                                 as                                 TAX_ID_DRV_UPCS_NO
		, TAX_ID_SEQ_NO                                      as                                      TAX_ID_SEQ_NO
		, TAX_ID_EFF_DT                                      as                                      TAX_ID_EFF_DT
		, TAX_ID_END_DT                                      as                                      TAX_ID_END_DT
		, TAX_ID_USE_FOR_1099_IND                            as                            TAX_ID_USE_FOR_1099_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_TAX   ), 
RENAME_C as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                          C_CUST_ID 
				FROM     LOGIC_C   ), 
RENAME_TT as ( SELECT 
		  TAX_ID_TYP_NM                                      as                                      TAX_ID_TYP_NM
		, TAX_ID_TYP_CD                                      as                                   TT_TAX_ID_TYP_CD
		, TAX_ID_TYP_VOID_IND                                as                                TAX_ID_TYP_VOID_IND 
				FROM     LOGIC_TT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_TAX                            as ( SELECT * from    RENAME_TAX   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_TT                             as ( SELECT * from    RENAME_TT   ),

---- JOIN LAYER ----

TAX as ( SELECT * 
				FROM  FILTER_TAX
				LEFT JOIN FILTER_C ON  FILTER_TAX.CUST_ID =  FILTER_C.C_CUST_ID 
								LEFT JOIN FILTER_TT ON  FILTER_TAX.TAX_ID_TYP_CD =  FILTER_TT.TT_TAX_ID_TYP_CD  )
SELECT * 
from TAX
      );
    