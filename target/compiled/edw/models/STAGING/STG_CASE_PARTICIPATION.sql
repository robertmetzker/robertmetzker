---- SRC LAYER ----
WITH
SRC_CP as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PARTICIPANT ),
SRC_CS as ( SELECT *     from     DEV_VIEWS.PCMP.CASES ),
SRC_CPT as ( SELECT *     from     DEV_VIEWS.PCMP.CASE_PARTICIPATION_TYPE ),
SRC_CUST as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
//SRC_CP as ( SELECT *     from     CASE_PARTICIPANT) ,
//SRC_CS as ( SELECT *     from     CASES) ,
//SRC_CPT as ( SELECT *     from     CASE_PARTICIPATION_TYPE) ,
//SRC_CUST as ( SELECT *     from     CUSTOMER) ,

---- LOGIC LAYER ----

LOGIC_CP as ( SELECT 
		  CASE_PTCP_ID                                       AS                                       CASE_PTCP_ID 
		, CASE_ID                                            AS                                            CASE_ID 
		, upper( TRIM( CASE_PTCP_TYP_CD ) )                  AS                                   CASE_PTCP_TYP_CD 
		, upper( CASE_PTCP_PRI_IND )                         AS                                  CASE_PTCP_PRI_IND 
		, upper( CASE_PTCP_DFLT_IND )                        AS                                 CASE_PTCP_DFLT_IND 
		, CUST_ID                                            AS                                            CUST_ID 
		, USER_ID                                            AS                                            USER_ID 
		, PTCP_ID                                            AS                                            PTCP_ID 
		, CASE_PTCP_EFF_DT                                   AS                                   CASE_PTCP_EFF_DT 
		, CASE_PTCP_END_DT                                   AS                                   CASE_PTCP_END_DT 
		, upper( TRIM( CASE_PTCP_NM ) )                      AS                                       CASE_PTCP_NM 
		, upper( TRIM( CASE_PTCP_STR_1 ) )                   AS                                    CASE_PTCP_STR_1 
		, upper( TRIM( CASE_PTCP_STR_2 ) )                   AS                                    CASE_PTCP_STR_2 
		, upper( TRIM( CASE_PTCP_CITY ) )                    AS                                     CASE_PTCP_CITY 
		, upper( TRIM( CASE_PTCP_CNTY ) )                    AS                                     CASE_PTCP_CNTY 
		, upper( TRIM( CASE_PTCP_STT ) )                     AS                                      CASE_PTCP_STT 
		, upper( TRIM( CASE_PTCP_ZIP ) )                     AS                                      CASE_PTCP_ZIP 
		, upper( TRIM( CASE_PTCP_DFLT_PTCP_TYP_NM ) )        AS                         CASE_PTCP_DFLT_PTCP_TYP_NM 
		, upper( TRIM( CASE_PTCP_PHN_NO ) )                  AS                                   CASE_PTCP_PHN_NO 
		, upper( TRIM( CASE_PTCP_FAX_NO ) )                  AS                                   CASE_PTCP_FAX_NO 
		, upper( TRIM( CASE_PTCP_EMAIL_ADDR ) )              AS                               CASE_PTCP_EMAIL_ADDR 
		, upper( TRIM( CASE_PTCP_CNTRY ) )                   AS                                    CASE_PTCP_CNTRY 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CP
            ),
LOGIC_CS as ( SELECT 
		  upper( TRIM( CASE_NO ) )                           AS                                            CASE_NO 
		, CASE_ID                                            AS                                            CASE_ID 
		from SRC_CS
            ),
LOGIC_CPT as ( SELECT 
		  upper( TRIM( CASE_PTCP_TYP_NM ) )                  AS                                   CASE_PTCP_TYP_NM 
		, upper( TRIM( CASE_PTCP_TYP_CD ) )                  AS                                   CASE_PTCP_TYP_CD 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CPT
            ),
LOGIC_CUST as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_CUST
            )

---- RENAME LAYER ----
,

RENAME_CP as ( SELECT 
		  CASE_PTCP_ID                                       as                                       CASE_PTCP_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_PTCP_TYP_CD                                   as                                   CASE_PTCP_TYP_CD
		, CASE_PTCP_PRI_IND                                  as                                  CASE_PTCP_PRI_IND
		, CASE_PTCP_DFLT_IND                                 as                                 CASE_PTCP_DFLT_IND
		, CUST_ID                                            as                                            CUST_ID
		, USER_ID                                            as                                            USER_ID
		, PTCP_ID                                            as                                            PTCP_ID
		, CASE_PTCP_EFF_DT                                   as                                   CASE_PTCP_EFF_DT
		, CASE_PTCP_END_DT                                   as                                   CASE_PTCP_END_DT
		, CASE_PTCP_NM                                       as                                       CASE_PTCP_NM
		, CASE_PTCP_STR_1                                    as                                    CASE_PTCP_STR_1
		, CASE_PTCP_STR_2                                    as                                    CASE_PTCP_STR_2
		, CASE_PTCP_CITY                                     as                                     CASE_PTCP_CITY
		, CASE_PTCP_CNTY                                     as                                     CASE_PTCP_CNTY
		, CASE_PTCP_STT                                      as                                      CASE_PTCP_STT
		, CASE_PTCP_ZIP                                      as                                      CASE_PTCP_ZIP
		, CASE_PTCP_DFLT_PTCP_TYP_NM                         as                         CASE_PTCP_DFLT_PTCP_TYP_NM
		, CASE_PTCP_PHN_NO                                   as                                   CASE_PTCP_PHN_NO
		, CASE_PTCP_FAX_NO                                   as                                   CASE_PTCP_FAX_NO
		, CASE_PTCP_EMAIL_ADDR                               as                               CASE_PTCP_EMAIL_ADDR
		, CASE_PTCP_CNTRY                                    as                                    CASE_PTCP_CNTRY
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CP   ), 
RENAME_CS as ( SELECT 
		  CASE_NO                                            as                                            CASE_NO
		, CASE_ID                                            as                                         CS_CASE_ID 
				FROM     LOGIC_CS   ), 
RENAME_CPT as ( SELECT 
		  CASE_PTCP_TYP_NM                                   as                                   CASE_PTCP_TYP_NM
		, CASE_PTCP_TYP_CD                                   as                               CPT_CASE_PTCP_TYP_CD
		, VOID_IND                                           as                                       CPT_VOID_IND 
				FROM     LOGIC_CPT   ), 
RENAME_CUST as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                       CUST_CUST_ID 
				FROM     LOGIC_CUST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CP                             as ( SELECT * from    RENAME_CP   ),
FILTER_CPT                            as ( SELECT * from    RENAME_CPT 
				WHERE CPT_VOID_IND = 'N'  ),
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_CUST                           as ( SELECT * from    RENAME_CUST   ),

---- JOIN LAYER ----

CP as ( SELECT * 
				FROM  FILTER_CP
				LEFT JOIN FILTER_CPT ON  FILTER_CP.CASE_PTCP_TYP_CD =  FILTER_CPT.CPT_CASE_PTCP_TYP_CD 
								LEFT JOIN FILTER_CS ON  FILTER_CP.CASE_ID =  FILTER_CS.CS_CASE_ID 
								LEFT JOIN FILTER_CUST ON  FILTER_CP.CUST_ID =  FILTER_CUST.CUST_CUST_ID  )
SELECT * 
from CP