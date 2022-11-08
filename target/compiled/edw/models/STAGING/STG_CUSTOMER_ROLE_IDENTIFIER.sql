---- SRC LAYER ----
WITH
SRC_CRI as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE_IDENTIFIER ),
SRC_IT as ( SELECT *     from     DEV_VIEWS.PCMP.IDENTIFIER_TYPE ),
SRC_CRT as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE_TYPE ),
//SRC_CRI as ( SELECT *     from     CUSTOMER_ROLE_IDENTIFIER) ,
//SRC_IT as ( SELECT *     from     IDENTIFIER_TYPE) ,
//SRC_CRT as ( SELECT *     from     CUSTOMER_ROLE_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CRI as ( SELECT 
		  CUST_ROLE_ID_ID                                    AS                                    CUST_ROLE_ID_ID 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( ID_TYP_CD ) )                         AS                                          ID_TYP_CD 
		, upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		, upper( TRIM( LOB_TYP_CD ) )                        AS                                         LOB_TYP_CD 
		, STT_ID                                             AS                                             STT_ID 
		, upper( TRIM( LIC_TYP_CD ) )                        AS                                         LIC_TYP_CD 
		, cast( CUST_ROLE_ID_EFF_DT as DATE )                AS                                CUST_ROLE_ID_EFF_DT 
		, cast( CUST_ROLE_ID_END_DT as DATE )                AS                                CUST_ROLE_ID_END_DT 
		, upper( TRIM( CUST_ROLE_ID_VAL_STR ) )              AS                               CUST_ROLE_ID_VAL_STR 
		, upper( TRIM( CUST_ROLE_ID_VAL_UPCS_STR ) )         AS                          CUST_ROLE_ID_VAL_UPCS_STR 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CRI
            ),
LOGIC_IT as ( SELECT 
		  upper( TRIM( ID_TYP_CD ) )                         AS                                          ID_TYP_CD 
		, upper( TRIM( ID_TYP_NM ) )                         AS                                          ID_TYP_NM 
		, upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		, CNTRY_ID                                           AS                                           CNTRY_ID 
		, upper( TRIM( DATA_TYP_CD ) )                       AS                                        DATA_TYP_CD 
		, ID_TYP_LNGTH                                       AS                                       ID_TYP_LNGTH 
		, upper( ID_TYP_REQ_IND )                            AS                                     ID_TYP_REQ_IND 
		, upper( ID_TYP_LOB_REQ_IND )                        AS                                 ID_TYP_LOB_REQ_IND 
		, upper( ID_TYP_STT_REQ_IND )                        AS                                 ID_TYP_STT_REQ_IND 
		, upper( ID_TYP_EFF_DT_REQ_IND )                     AS                              ID_TYP_EFF_DT_REQ_IND 
		, upper( ID_TYP_END_DT_REQ_IND )                     AS                              ID_TYP_END_DT_REQ_IND 
		, upper( ID_TYP_RPET_IND )                           AS                                    ID_TYP_RPET_IND 
		, upper( ID_TYP_STT_RPET_IND )                       AS                                ID_TYP_STT_RPET_IND 
		, upper( ID_TYP_LIC_TYP_STT_RPET_IND )               AS                        ID_TYP_LIC_TYP_STT_RPET_IND 
		, upper( ID_TYP_OVRLP_DT_IND )                       AS                                ID_TYP_OVRLP_DT_IND 
		, upper( ID_TYP_STT_OVRLP_DT_IND )                   AS                            ID_TYP_STT_OVRLP_DT_IND 
		, upper( ID_TYP_LIC_STT_OVRLP_DT_IND )               AS                        ID_TYP_LIC_STT_OVRLP_DT_IND 
		, upper( ID_TYP_UNQ_IND )                            AS                                     ID_TYP_UNQ_IND 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_IT
            ),
LOGIC_CRT as ( SELECT 
		  upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		, upper( TRIM( CUST_ROLE_TYP_NM ) )                  AS                                   CUST_ROLE_TYP_NM 
		, upper( CUST_ROLE_TYP_VOID_IND )                    AS                             CUST_ROLE_TYP_VOID_IND 
		from SRC_CRT
            )

---- RENAME LAYER ----
,

RENAME_CRI as ( SELECT 
		  CUST_ROLE_ID_ID                                    as                                    CUST_ROLE_ID_ID
		, CUST_ID                                            as                                            CUST_ID
		, ID_TYP_CD                                          as                                          ID_TYP_CD
		, CUST_ROLE_TYP_CD                                   as                                   CUST_ROLE_TYP_CD
		, LOB_TYP_CD                                         as                                         LOB_TYP_CD
		, STT_ID                                             as                                             STT_ID
		, LIC_TYP_CD                                         as                                         LIC_TYP_CD
		, CUST_ROLE_ID_EFF_DT                                as                                CUST_ROLE_ID_EFF_DT
		, CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT
		, CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR
		, CUST_ROLE_ID_VAL_UPCS_STR                          as                          CUST_ROLE_ID_VAL_UPCS_STR
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                       CRI_VOID_IND 
				FROM     LOGIC_CRI   ), 
RENAME_IT as ( SELECT 
		  ID_TYP_CD                                          as                                       IT_ID_TYP_CD
		, ID_TYP_NM                                          as                                          ID_TYP_NM
		, CUST_ROLE_TYP_CD                                   as                                IT_CUST_ROLE_TYP_CD
		, CNTRY_ID                                           as                                           CNTRY_ID
		, DATA_TYP_CD                                        as                                        DATA_TYP_CD
		, ID_TYP_LNGTH                                       as                                       ID_TYP_LNGTH
		, ID_TYP_REQ_IND                                     as                                     ID_TYP_REQ_IND
		, ID_TYP_LOB_REQ_IND                                 as                                 ID_TYP_LOB_REQ_IND
		, ID_TYP_STT_REQ_IND                                 as                                 ID_TYP_STT_REQ_IND
		, ID_TYP_EFF_DT_REQ_IND                              as                              ID_TYP_EFF_DT_REQ_IND
		, ID_TYP_END_DT_REQ_IND                              as                              ID_TYP_END_DT_REQ_IND
		, ID_TYP_RPET_IND                                    as                                    ID_TYP_RPET_IND
		, ID_TYP_STT_RPET_IND                                as                                ID_TYP_STT_RPET_IND
		, ID_TYP_LIC_TYP_STT_RPET_IND                        as                        ID_TYP_LIC_TYP_STT_RPET_IND
		, ID_TYP_OVRLP_DT_IND                                as                                ID_TYP_OVRLP_DT_IND
		, ID_TYP_STT_OVRLP_DT_IND                            as                            ID_TYP_STT_OVRLP_DT_IND
		, ID_TYP_LIC_STT_OVRLP_DT_IND                        as                        ID_TYP_LIC_STT_OVRLP_DT_IND
		, ID_TYP_UNQ_IND                                     as                                     ID_TYP_UNQ_IND
		, VOID_IND                                           as                                        IT_VOID_IND 
				FROM     LOGIC_IT   ), 
RENAME_CRT as ( SELECT 
		  CUST_ROLE_TYP_CD                                   as                               CRT_CUST_ROLE_TYP_CD
		, CUST_ROLE_TYP_NM                                   as                                   CUST_ROLE_TYP_NM
		, CUST_ROLE_TYP_VOID_IND                             as                             CUST_ROLE_TYP_VOID_IND 
				FROM     LOGIC_CRT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CRI                            as ( SELECT * from    RENAME_CRI   ),
FILTER_IT                             as ( SELECT * from    RENAME_IT   ),
FILTER_CRT                            as ( SELECT * from    RENAME_CRT   ),

---- JOIN LAYER ----

CRI as ( SELECT * 
				FROM  FILTER_CRI
				LEFT JOIN FILTER_IT ON  FILTER_CRI.ID_TYP_CD =  FILTER_IT.IT_ID_TYP_CD 
						LEFT JOIN FILTER_CRT ON  FILTER_CRI.CUST_ROLE_TYP_CD =  FILTER_CRT.CRT_CUST_ROLE_TYP_CD  )
SELECT * 
from CRI