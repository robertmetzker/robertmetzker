

      create or replace  table DEV_EDW.STAGING.STG_REPRESENTATIVE  as
      (---- SRC LAYER ----
WITH
SRC_CUST as ( SELECT *     from    DEV_VIEWS.PCMP.CUSTOMER ),
SRC_ROLE_ID as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_ROLE_IDENTIFIER ),
SRC_CT as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_TYPE ),
//SRC_CUST as ( SELECT *     from     CUSTOMER) ,
//SRC_ROLE_ID as ( SELECT *     from     CUSTOMER_ROLE_IDENTIFIER) ,
//SRC_CT as ( SELECT *     from     CUSTOMER_TYPE) ,

---- LOGIC LAYER ----


LOGIC_CUST as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, upper( TRIM( CUST_TYP_CD ) )                       as                                        CUST_TYP_CD 
		, upper( CUST_1099_RECV_IND )                        as                                 CUST_1099_RECV_IND 
		, upper( CUST_W9_RECV_IND )                          as                                   CUST_W9_RECV_IND 
		, upper( CUST_TAX_EXMT_IND )                         as                                  CUST_TAX_EXMT_IND 
		, upper( CUST_FRGN_CITZ_IND )                        as                                 CUST_FRGN_CITZ_IND 
		, upper( CUST_TAX_ID_UNAVL_IND )                     as                              CUST_TAX_ID_UNAVL_IND 
		, upper( CUST_TAX_ID_OVRRD_IND )                     as                              CUST_TAX_ID_OVRRD_IND 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CUST
            ),

LOGIC_ROLE_ID as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, CUST_ROLE_ID_EFF_DT                                as                                CUST_ROLE_ID_EFF_DT 
		, CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT 
		, upper( TRIM( CUST_ROLE_ID_VAL_STR ) )              as                               CUST_ROLE_ID_VAL_STR 
		, upper( TRIM( ID_TYP_CD ) )                         as                                          ID_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_ROLE_ID
            ),

LOGIC_CT as ( SELECT 
		  upper( TRIM( CUST_TYP_CD ) )                       as                                        CUST_TYP_CD 
		, upper( TRIM( CUST_TYP_NM ) )                       as                                        CUST_TYP_NM 
		, upper( CUST_TYP_VOID_IND )                         as                                  CUST_TYP_VOID_IND 
		from SRC_CT
            )

---- RENAME LAYER ----
,

RENAME_CUST as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID
		, CUST_NO                                            as                                            CUST_NO
		, CUST_TYP_CD                                        as                                        CUST_TYP_CD
		, CUST_1099_RECV_IND                                 as                                 CUST_1099_RECV_IND
		, CUST_W9_RECV_IND                                   as                                   CUST_W9_RECV_IND
		, CUST_TAX_EXMT_IND                                  as                                  CUST_TAX_EXMT_IND
		, CUST_FRGN_CITZ_IND                                 as                                 CUST_FRGN_CITZ_IND
		, CUST_TAX_ID_UNAVL_IND                              as                              CUST_TAX_ID_UNAVL_IND
		, CUST_TAX_ID_OVRRD_IND                              as                              CUST_TAX_ID_OVRRD_IND
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CUST   ), 
RENAME_ROLE_ID as ( SELECT 
		  CUST_ID                                            as                                    ROLE_ID_CUST_ID
		, CUST_ROLE_ID_EFF_DT                                as                                CUST_ROLE_ID_EFF_DT
		, CUST_ROLE_ID_END_DT                                as                                CUST_ROLE_ID_END_DT
		, CUST_ROLE_ID_VAL_STR                               as                               CUST_ROLE_ID_VAL_STR
		, ID_TYP_CD                                          as                                          ID_TYP_CD
		, VOID_IND                                           as                                   ROLE_ID_VOID_IND 
				FROM     LOGIC_ROLE_ID   ), 
RENAME_CT as ( SELECT 
		  CUST_TYP_CD                                        as                                     CT_CUST_TYP_CD
		, CUST_TYP_NM                                        as                                        CUST_TYP_NM
		, CUST_TYP_VOID_IND                                  as                                  CUST_TYP_VOID_IND 
				FROM     LOGIC_CT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CUST                           as ( SELECT * from    RENAME_CUST   ),
FILTER_ROLE_ID                        as ( SELECT * from    RENAME_ROLE_ID 
                                            WHERE RENAME_ROLE_ID.ID_TYP_CD = 'REP' and RENAME_ROLE_ID.ROLE_ID_VOID_IND = 'N'  ),
FILTER_CT                             as ( SELECT * from    RENAME_CT 
                                            WHERE CUST_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

CUST as ( SELECT * 
				FROM  FILTER_CUST
				INNER JOIN FILTER_ROLE_ID ON  FILTER_CUST.CUST_ID =  FILTER_ROLE_ID.ROLE_ID_CUST_ID
				LEFT JOIN FILTER_CT ON  FILTER_CUST.CUST_TYP_CD =  FILTER_CT.CT_CUST_TYP_CD  )
SELECT 
		  CUST_ID
		, CUST_NO
		, CUST_TYP_CD
		, CUST_1099_RECV_IND
		, CUST_W9_RECV_IND
		, CUST_TAX_EXMT_IND
		, CUST_FRGN_CITZ_IND
		, CUST_TAX_ID_UNAVL_IND
		, CUST_TAX_ID_OVRRD_IND
		, VOID_IND
		, ROLE_ID_CUST_ID
		, CUST_ROLE_ID_EFF_DT
		, CUST_ROLE_ID_END_DT
		, CUST_ROLE_ID_VAL_STR
		, ID_TYP_CD
		, ROLE_ID_VOID_IND
		, CT_CUST_TYP_CD
		, CUST_TYP_NM
		, CUST_TYP_VOID_IND 
from CUST
      );
    