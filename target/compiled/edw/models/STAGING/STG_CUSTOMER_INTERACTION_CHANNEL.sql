---- SRC LAYER ----
WITH
SRC_CIC as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_INTERACTION_CHANNEL ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_ICT as ( SELECT *     from     DEV_VIEWS.PCMP.INTERACTION_CHANNEL_TYPE ),
SRC_CRT as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE_TYPE ),
SRC_PNT as ( SELECT *     from     DEV_VIEWS.PCMP.PHONE_NUMBER_TYPE ),
//SRC_CIC as ( SELECT *     from     CUSTOMER_INTERACTION_CHANNEL) ,
//SRC_C as ( SELECT *     from     CUSTOMER) ,
//SRC_ICT as ( SELECT *     from     INTERACTION_CHANNEL_TYPE) ,
//SRC_CRT as ( SELECT *     from     CUSTOMER_ROLE_TYPE) ,
//SRC_PNT as ( SELECT *     from     PHONE_NUMBER_TYPE) ,

---- LOGIC LAYER ----

LOGIC_CIC as ( SELECT 
		  CUST_INTRN_CHNL_ID                                 AS                                 CUST_INTRN_CHNL_ID 
		, CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( INTRN_CHNL_TYP_CD ) )                 AS                                  INTRN_CHNL_TYP_CD 
		, upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		, upper( TRIM( PHN_NO_TYP_CD ) )                     AS                                      PHN_NO_TYP_CD 
		, upper( TRIM( CUST_INTRN_CHNL_ADDR ) )              AS                               CUST_INTRN_CHNL_ADDR 
		, upper( TRIM( CUST_INTRN_CHNL_PHN_NO ) )            AS                             CUST_INTRN_CHNL_PHN_NO 
		, upper( TRIM( CUST_INTRN_CHNL_PHN_NO_EXT ) )        AS                         CUST_INTRN_CHNL_PHN_NO_EXT 
		, upper( TRIM( CUST_INTRN_CHNL_COMT ) )              AS                               CUST_INTRN_CHNL_COMT 
		, upper( CUST_INTRN_CHNL_PRI_IND )                   AS                            CUST_INTRN_CHNL_PRI_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( CUST_INTRN_CHNL_UPDT_SRC ) )          AS                           CUST_INTRN_CHNL_UPDT_SRC 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CIC
            ),
LOGIC_C as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_C
            ),
LOGIC_ICT as ( SELECT 
		  upper( TRIM( INTRN_CHNL_TYP_NM ) )                 AS                                  INTRN_CHNL_TYP_NM 
		, upper( TRIM( INTRN_CHNL_TYP_CD ) )                 AS                                  INTRN_CHNL_TYP_CD 
		, upper( INTRN_CHNL_TYP_VOID_IND )                   AS                            INTRN_CHNL_TYP_VOID_IND 
		from SRC_ICT
            ),
LOGIC_CRT as ( SELECT 
		  upper( TRIM( CUST_ROLE_TYP_NM ) )                  AS                                   CUST_ROLE_TYP_NM 
		, upper( TRIM( CUST_ROLE_TYP_CD ) )                  AS                                   CUST_ROLE_TYP_CD 
		, upper( CUST_ROLE_TYP_VOID_IND )                    AS                             CUST_ROLE_TYP_VOID_IND 
		from SRC_CRT
            ),
LOGIC_PNT as ( SELECT 
		  upper( TRIM( PHN_NO_TYP_NM ) )                     AS                                      PHN_NO_TYP_NM 
		, upper( TRIM( PHN_NO_TYP_CD ) )                     AS                                      PHN_NO_TYP_CD 
		, upper( PHN_NO_TYP_VOID_IND )                       AS                                PHN_NO_TYP_VOID_IND 
		from SRC_PNT
            )

---- RENAME LAYER ----
,

RENAME_CIC as ( SELECT 
		  CUST_INTRN_CHNL_ID                                 as                                 CUST_INTRN_CHNL_ID
		, CUST_ID                                            as                                            CUST_ID
		, INTRN_CHNL_TYP_CD                                  as                                  INTRN_CHNL_TYP_CD
		, CUST_ROLE_TYP_CD                                   as                                   CUST_ROLE_TYP_CD
		, PHN_NO_TYP_CD                                      as                                      PHN_NO_TYP_CD
		, CUST_INTRN_CHNL_ADDR                               as                               CUST_INTRN_CHNL_ADDR
		, CUST_INTRN_CHNL_PHN_NO                             as                             CUST_INTRN_CHNL_PHN_NO
		, CUST_INTRN_CHNL_PHN_NO_EXT                         as                         CUST_INTRN_CHNL_PHN_NO_EXT
		, CUST_INTRN_CHNL_COMT                               as                               CUST_INTRN_CHNL_COMT
		, CUST_INTRN_CHNL_PRI_IND                            as                            CUST_INTRN_CHNL_PRI_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, CUST_INTRN_CHNL_UPDT_SRC                           as                           CUST_INTRN_CHNL_UPDT_SRC
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CIC   ), 
RENAME_C as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                          C_CUST_ID 
				FROM     LOGIC_C   ), 
RENAME_ICT as ( SELECT 
		  INTRN_CHNL_TYP_NM                                  as                                  INTRN_CHNL_TYP_NM
		, INTRN_CHNL_TYP_CD                                  as                              ICT_INTRN_CHNL_TYP_CD
		, INTRN_CHNL_TYP_VOID_IND                            as                            INTRN_CHNL_TYP_VOID_IND 
				FROM     LOGIC_ICT   ), 
RENAME_CRT as ( SELECT 
		  CUST_ROLE_TYP_NM                                   as                                   CUST_ROLE_TYP_NM
		, CUST_ROLE_TYP_CD                                   as                               CRT_CUST_ROLE_TYP_CD
		, CUST_ROLE_TYP_VOID_IND                             as                             CUST_ROLE_TYP_VOID_IND 
				FROM     LOGIC_CRT   ), 
RENAME_PNT as ( SELECT 
		  PHN_NO_TYP_NM                                      as                                      PHN_NO_TYP_NM
		, PHN_NO_TYP_CD                                      as                                  PNT_PHN_NO_TYP_CD
		, PHN_NO_TYP_VOID_IND                                as                                PHN_NO_TYP_VOID_IND 
				FROM     LOGIC_PNT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CIC                            as ( SELECT * from    RENAME_CIC   ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_ICT                            as ( SELECT * from    RENAME_ICT 
				WHERE INTRN_CHNL_TYP_VOID_IND = 'N'  ),
FILTER_CRT                            as ( SELECT * from    RENAME_CRT 
				WHERE CUST_ROLE_TYP_VOID_IND = 'N'  ),
FILTER_PNT                            as ( SELECT * from    RENAME_PNT 
				WHERE PHN_NO_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CIC as ( SELECT * 
				FROM  FILTER_CIC
				INNER JOIN FILTER_C ON  FILTER_CIC.CUST_ID =  FILTER_C.C_CUST_ID 
								LEFT JOIN FILTER_ICT ON  FILTER_CIC.INTRN_CHNL_TYP_CD =  FILTER_ICT.ICT_INTRN_CHNL_TYP_CD 
								LEFT JOIN FILTER_CRT ON  FILTER_CIC.CUST_ROLE_TYP_CD =  FILTER_CRT.CRT_CUST_ROLE_TYP_CD 
								LEFT JOIN FILTER_PNT ON  FILTER_CIC.PHN_NO_TYP_CD =  FILTER_PNT.PNT_PHN_NO_TYP_CD  )
SELECT * 
from CIC