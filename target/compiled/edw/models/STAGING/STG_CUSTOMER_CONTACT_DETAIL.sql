---- SRC LAYER ----
WITH
SRC_CCD as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_CONTACT_DETAIL ),
SRC_CCDT as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_CONTACT_DETAIL_TYPE ),
SRC_PNT as ( SELECT *     from     DEV_VIEWS.PCMP.PHONE_NUMBER_TYPE ),
//SRC_CCD as ( SELECT *     from     CUSTOMER_CONTACT_DETAIL) ,
//SRC_CCDT as ( SELECT *     from     CUSTOMER_CONTACT_DETAIL_TYPE) ,
//SRC_PNT as ( SELECT *     from     PHONE_NUMBER_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CCD as ( SELECT 
		  CUST_CNTC_DTL_ID                                   AS                                   CUST_CNTC_DTL_ID 
		, CUST_CNTC_ID                                       AS                                       CUST_CNTC_ID 
		, upper( TRIM( CUST_CNTC_DTL_TYP_CD ) )              AS                               CUST_CNTC_DTL_TYP_CD 
		, upper( TRIM( CUST_CNTC_DTL_SUB_TYP ) )             AS                              CUST_CNTC_DTL_SUB_TYP 
		, upper( TRIM( CUST_CNTC_DTL_VAL_1 ) )               AS                                CUST_CNTC_DTL_VAL_1 
		, upper( TRIM( CUST_CNTC_DTL_VAL_2 ) )               AS                                CUST_CNTC_DTL_VAL_2 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( CUST_CNTC_DTL_VOID_IND )                    AS                             CUST_CNTC_DTL_VOID_IND 
		from SRC_CCD
            ),
LOGIC_CCDT as ( SELECT 
		  upper( TRIM( CUST_CNTC_DTL_TYP_NM ) )              AS                               CUST_CNTC_DTL_TYP_NM 
		, upper( TRIM( CUST_CNTC_DTL_TYP_CD ) )              AS                               CUST_CNTC_DTL_TYP_CD 
		, upper( CUST_CNTC_DTL_TYP_VOID_IND )                AS                         CUST_CNTC_DTL_TYP_VOID_IND 
		from SRC_CCDT
            ),
LOGIC_PNT as ( SELECT 
		  upper( TRIM( PHN_NO_TYP_NM ) )                     AS                                      PHN_NO_TYP_NM 
		, upper( TRIM( PHN_NO_TYP_CD ) )                     AS                                      PHN_NO_TYP_CD 
		, upper( PHN_NO_TYP_VOID_IND )                       AS                                PHN_NO_TYP_VOID_IND 
		from SRC_PNT
            )

---- RENAME LAYER ----
,

RENAME_CCD as ( SELECT 
		  CUST_CNTC_DTL_ID                                   as                                   CUST_CNTC_DTL_ID
		, CUST_CNTC_ID                                       as                                       CUST_CNTC_ID
		, CUST_CNTC_DTL_TYP_CD                               as                               CUST_CNTC_DTL_TYP_CD
		, CUST_CNTC_DTL_SUB_TYP                              as                              CUST_CNTC_DTL_SUB_TYP
		, CUST_CNTC_DTL_VAL_1                                as                                CUST_CNTC_DTL_VAL_1
		, CUST_CNTC_DTL_VAL_2                                as                                CUST_CNTC_DTL_VAL_2
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, CUST_CNTC_DTL_VOID_IND                             as                             CUST_CNTC_DTL_VOID_IND 
				FROM     LOGIC_CCD   ), 
RENAME_CCDT as ( SELECT 
		  CUST_CNTC_DTL_TYP_NM                               as                               CUST_CNTC_DTL_TYP_NM
		, CUST_CNTC_DTL_TYP_CD                               as                          CCDT_CUST_CNTC_DTL_TYP_CD
		, CUST_CNTC_DTL_TYP_VOID_IND                         as                         CUST_CNTC_DTL_TYP_VOID_IND 
				FROM     LOGIC_CCDT   ), 
RENAME_PNT as ( SELECT 
		  PHN_NO_TYP_NM                                      as                                      PHN_NO_TYP_NM
		, PHN_NO_TYP_CD                                      as                                      PHN_NO_TYP_CD
		, PHN_NO_TYP_VOID_IND                                as                                PHN_NO_TYP_VOID_IND 
				FROM     LOGIC_PNT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CCD                            as ( SELECT * from    RENAME_CCD   ),
FILTER_CCDT                           as ( SELECT * from    RENAME_CCDT 
				WHERE CUST_CNTC_DTL_TYP_VOID_IND = 'N'  ),
FILTER_PNT                            as ( SELECT * from    RENAME_PNT 
				WHERE PHN_NO_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CCD as ( SELECT * 
				FROM  FILTER_CCD
				LEFT JOIN FILTER_CCDT ON  FILTER_CCD.CUST_CNTC_DTL_TYP_CD =  FILTER_CCDT.CCDT_CUST_CNTC_DTL_TYP_CD 
								LEFT JOIN FILTER_PNT ON  FILTER_CCD.CUST_CNTC_DTL_SUB_TYP =  FILTER_PNT.PHN_NO_TYP_CD  )
SELECT * 
from CCD