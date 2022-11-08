---- SRC LAYER ----
WITH
SRC_CC as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_CONTACT ),
SRC_CUST as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CONTACT_TYPE ),
//SRC_CC as ( SELECT *     from     CUSTOMER_CONTACT) ,
//SRC_CUST as ( SELECT *     from     CUSTOMER) ,
//SRC_CT as ( SELECT *     from     CONTACT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CC as ( SELECT 
		  CUST_CNTC_ID                                       AS                                       CUST_CNTC_ID 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( CNTC_TYP_CD ) )                       AS                                        CNTC_TYP_CD 
		, upper( TRIM( CUST_CNTC_NM ) )                      AS                                       CUST_CNTC_NM 
		, cast( CUST_CNTC_EFF_DT as DATE )                   AS                                   CUST_CNTC_EFF_DT 
		, cast( CUST_CNTC_END_DT as DATE )                   AS                                   CUST_CNTC_END_DT 
		, upper( TRIM( CUST_CNTC_CMT ) )                     AS                                      CUST_CNTC_CMT 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( CUST_CNTC_VOID_IND )                        AS                                 CUST_CNTC_VOID_IND 
		from SRC_CC
            ),
LOGIC_CUST as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_CUST
            ),
LOGIC_CT as ( SELECT 
		  upper( TRIM( CNTC_TYP_NM ) )                       AS                                        CNTC_TYP_NM 
		, upper( TRIM( CNTC_TYP_CD ) )                       AS                                        CNTC_TYP_CD 
		, upper( CNTC_TYP_VOID_IND )                         AS                                  CNTC_TYP_VOID_IND 
		from SRC_CT
            )

---- RENAME LAYER ----
,

RENAME_CC as ( SELECT 
		  CUST_CNTC_ID                                       as                                       CUST_CNTC_ID
		, CUST_ID                                            as                                            CUST_ID
		, CNTC_TYP_CD                                        as                                        CNTC_TYP_CD
		, CUST_CNTC_NM                                       as                                       CUST_CNTC_NM
		, CUST_CNTC_EFF_DT                                   as                                   CUST_CNTC_EFF_DT
		, CUST_CNTC_END_DT                                   as                                   CUST_CNTC_END_DT
		, CUST_CNTC_CMT                                      as                                      CUST_CNTC_CMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, CUST_CNTC_VOID_IND                                 as                                 CUST_CNTC_VOID_IND 
				FROM     LOGIC_CC   ), 
RENAME_CUST as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                       CUST_CUST_ID 
				FROM     LOGIC_CUST   ), 
RENAME_CT as ( SELECT 
		  CNTC_TYP_NM                                        as                                        CNTC_TYP_NM
		, CNTC_TYP_CD                                        as                                     CT_CNTC_TYP_CD
		, CNTC_TYP_VOID_IND                                  as                                  CNTC_TYP_VOID_IND 
				FROM     LOGIC_CT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CC                             as ( SELECT * from    RENAME_CC   ),
FILTER_CUST                           as ( SELECT * from    RENAME_CUST   ),
FILTER_CT                             as ( SELECT * from    RENAME_CT 
				WHERE CNTC_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CC as ( SELECT * 
				FROM  FILTER_CC
				INNER JOIN FILTER_CUST ON  FILTER_CC.CUST_ID =  FILTER_CUST.CUST_CUST_ID 
								LEFT JOIN FILTER_CT ON  FILTER_CC.CNTC_TYP_CD =  FILTER_CT.CT_CNTC_TYP_CD  )
SELECT * 
from CC