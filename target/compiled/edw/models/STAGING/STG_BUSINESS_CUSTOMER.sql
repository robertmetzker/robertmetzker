---- SRC LAYER ----
WITH
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_B as ( SELECT *     from     DEV_VIEWS.PCMP.BUSINESS ),
SRC_CO as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_OWNERSHIP ),
SRC_OT as ( SELECT *     from     DEV_VIEWS.PCMP.OWNERSHIP_TYPE ),
SRC_T  as ( SELECT *     FROM     DEV_VIEWS.PCMP.TAX_IDENTIFIER ),
//SRC_C as ( SELECT *     from     CUSTOMER) ,
//SRC_B as ( SELECT *     from     BUSINESS) ,
//SRC_CO as ( SELECT *     from     CUSTOMER_OWNERSHIP) ,
//SRC_OT as ( SELECT *     from     OWNERSHIP_TYPE) ,

---- LOGIC LAYER ----


LOGIC_C as ( SELECT 
		  CUST_ID                                            AS                                            CUST_ID 
		, upper( TRIM( CUST_NO ) )                           AS                                            CUST_NO 
		, upper( TRIM( CUST_TYP_CD ) )                       AS                                        CUST_TYP_CD 
		, upper( CUST_TAX_EXMT_IND )                         AS                                  CUST_TAX_EXMT_IND 
		, upper( CUST_TAX_ID_OVRRD_IND )                     AS                              CUST_TAX_ID_OVRRD_IND 
		, upper( CUST_TAX_ID_UNAVL_IND )                     AS                              CUST_TAX_ID_UNAVL_IND 
		, upper( CUST_1099_RECV_IND )                        AS                                 CUST_1099_RECV_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_C
            ),
LOGIC_B as ( SELECT 
		  cast( BUSN_CS_OPER_DTM as DATE )                   AS                                   BUSN_CS_OPER_DTM 
		, BUSN_YR_EST_DTM                                    AS                                    BUSN_YR_EST_DTM 
		, CUST_ID                                            AS                                            CUST_ID 
		from SRC_B
            ),
LOGIC_CO as ( SELECT 
		  upper( TRIM( OWNSHP_TYP_CD ) )                     AS                                      OWNSHP_TYP_CD 
		, upper( TRIM( CUST_OWNSHP_TYP_DESC ) )              AS                               CUST_OWNSHP_TYP_DESC 
		, cast( CUST_OWNSHP_EFF_DT as DATE )                 AS                                 CUST_OWNSHP_EFF_DT 
		, CUST_ID                                            AS                                            CUST_ID 
		, cast( CUST_OWNSHP_END_DT as DATE )                 AS                                 CUST_OWNSHP_END_DT 
		, upper( VOID_IND )                                  AS                                           VOID_IND 
		from SRC_CO
            ),
LOGIC_OT as ( SELECT 
		  upper( TRIM( OWNSHP_TYP_NM ) )                     AS                                      OWNSHP_TYP_NM 
		, upper( TRIM( OWNSHP_TYP_CD ) )                     AS                                      OWNSHP_TYP_CD 
		from SRC_OT
            ),
LOGIC_T as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, upper( TRIM( TAX_ID_TYP_CD ) )                     as                                      TAX_ID_TYP_CD 
		, upper( TRIM( TAX_ID_NO ) )                         as                                          TAX_ID_NO 
		, upper( TRIM( TAX_ID_SEQ_NO ) )                     as                                      TAX_ID_SEQ_NO 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, cast( TAX_ID_EFF_DT as DATE )                      as                                      TAX_ID_EFF_DT 
		, cast( TAX_ID_END_DT as DATE )                      as                                      TAX_ID_END_DT 
		FROM SRC_T
            )			

---- RENAME LAYER ----
,

RENAME_C as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
		, CUST_NO                                            as                                            CUST_NO
		, CUST_TYP_CD                                        as                                        CUST_TYP_CD
		, CUST_TAX_EXMT_IND                                  as                                  CUST_TAX_EXMT_IND
		, CUST_TAX_ID_OVRRD_IND                              as                              CUST_TAX_ID_OVRRD_IND
		, CUST_TAX_ID_UNAVL_IND                              as                              CUST_TAX_ID_UNAVL_IND
		, CUST_1099_RECV_IND                                 as                                 CUST_1099_RECV_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_C   ), 
RENAME_B as ( SELECT 
		  BUSN_CS_OPER_DTM                                   as                                   BUSN_CS_OPER_DTM
		, BUSN_YR_EST_DTM                                    as                                    BUSN_YR_EST_DTM
		, CUST_ID                                            as                                          B_CUST_ID 
				FROM     LOGIC_B   ), 
RENAME_CO as ( SELECT 
		  OWNSHP_TYP_CD                                      as                                      OWNSHP_TYP_CD
		, CUST_OWNSHP_TYP_DESC                               as                               CUST_OWNSHP_TYP_DESC
		, CUST_OWNSHP_EFF_DT                                 as                                 CUST_OWNSHP_EFF_DT
		, CUST_ID                                            as                                         CO_CUST_ID
		, CUST_OWNSHP_END_DT                                 as                                 CUST_OWNSHP_END_DT
		, VOID_IND                                           as                                        CO_VOID_IND 
				FROM     LOGIC_CO   ), 
RENAME_OT as ( SELECT 
		  OWNSHP_TYP_NM                                      as                                      OWNSHP_TYP_NM
		, OWNSHP_TYP_CD                                      as                                   OT_OWNSHP_TYP_CD 
				FROM     LOGIC_OT   ),
RENAME_T          as ( SELECT 
		  CUST_ID                                            as                                          T_CUST_ID
		, TAX_ID_TYP_CD                                      as                                      TAX_ID_TYP_CD
		, TAX_ID_NO                                          as                                          TAX_ID_NO
		, TAX_ID_SEQ_NO                                      as                                      TAX_ID_SEQ_NO
		, VOID_IND                                           as                                         T_VOID_IND
		, TAX_ID_EFF_DT                                      as                                      TAX_ID_EFF_DT
		, TAX_ID_END_DT                                      as                                      TAX_ID_END_DT 
				FROM     LOGIC_T   )				

---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * from    RENAME_C   ),
FILTER_B                              as ( SELECT * from    RENAME_B   ),
FILTER_CO                             as ( SELECT * from    RENAME_CO 
				WHERE CO_VOID_IND = 'N' AND CUST_OWNSHP_END_DT IS NULL  ),
FILTER_T                              as ( SELECT * FROM    RENAME_T 
                                            WHERE T_VOID_IND = 'N' AND TAX_ID_END_DT IS NULL  ),				
FILTER_OT                             as ( SELECT * from    RENAME_OT   ),

---- JOIN LAYER ----

CO as ( SELECT * 
				FROM  FILTER_CO
				LEFT JOIN FILTER_OT ON  FILTER_CO.OWNSHP_TYP_CD =  FILTER_OT.OT_OWNSHP_TYP_CD  ),
C as ( SELECT * 
				FROM  FILTER_C
				INNER JOIN FILTER_B ON  FILTER_C.CUST_ID =  FILTER_B.B_CUST_ID 
						LEFT JOIN CO ON  FILTER_C.CUST_ID = CO.CO_CUST_ID 
						LEFT JOIN FILTER_T ON  FILTER_C.CUST_ID =  FILTER_T.T_CUST_ID )
SELECT * 
from C