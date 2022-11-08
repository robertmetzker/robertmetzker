---- SRC LAYER ----
WITH
SRC_CR as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE ),
SRC_CRT as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE_TYPE ),
//SRC_CR as ( SELECT *     from     CUSTOMER_ROLE) ,
//SRC_CRT as ( SELECT *     from     CUSTOMER_ROLE_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CR as ( SELECT 
		  ROLE_ID                                            AS                                            ROLE_ID 
		, upper( TRIM( ROLE_TYP_CD ) )                       AS                                        ROLE_TYP_CD 
		, CUST_ID                                            AS                                            CUST_ID 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( VOID_IND ) )                          AS                                           VOID_IND 
		from SRC_CR
            ),
LOGIC_CRT as ( SELECT 
		  upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		, upper( TRIM( CUST_ROLE_TYP_NM ) )                  AS                                   CUST_ROLE_TYP_NM 
		, upper( TRIM( CUST_ROLE_TYP_VOID_IND ) )            AS                             CUST_ROLE_TYP_VOID_IND 
		from SRC_CRT
            )

---- RENAME LAYER ----
,

RENAME_CR as ( SELECT 
		  ROLE_ID                                            as                                            ROLE_ID
		, ROLE_TYP_CD                                        as                                        ROLE_TYP_CD
		, CUST_ID                                            as                                            CUST_ID
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CR   ), 
RENAME_CRT as ( SELECT 
		  CUST_ROLE_TYP_CD                                   as                                   CUST_ROLE_TYP_CD
		, CUST_ROLE_TYP_NM                                   as                                   CUST_ROLE_TYP_NM
		, CUST_ROLE_TYP_VOID_IND                             as                             CUST_ROLE_TYP_VOID_IND 
				FROM     LOGIC_CRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CR                             as ( SELECT * from    RENAME_CR   ),
FILTER_CRT                            as ( SELECT * from    RENAME_CRT   ),

---- JOIN LAYER ----

CR as ( SELECT * 
				FROM  FILTER_CR
				LEFT JOIN  FILTER_CRT ON  FILTER_CR.ROLE_TYP_CD =  FILTER_CRT.CUST_ROLE_TYP_CD  )
SELECT * 
from CR