---- SRC LAYER ----
WITH
SRC_CPPPT as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PTCP_PROV_PTCP_TYP ),
SRC_CPP as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PARTICIPATION_PROV ),
SRC_CP as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PARTICIPATION ),
SRC_P as ( SELECT *     from     DEV_VIEWS.PCMP.PARTICIPATION ),
SRC_C as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_PPT as ( SELECT *     from     DEV_VIEWS.PCMP.PROVIDER_PARTICIPATING_TYPE ),
SRC_CUST as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_PRMRY as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PTCP_PROV_PTCP_TYP ),
//SRC_CPPPT as ( SELECT *     from     CLAIM_PTCP_PROV_PTCP_TYP) ,
//SRC_CPP as ( SELECT *     from     CLAIM_PARTICIPATION_PROV) ,
//SRC_CP as ( SELECT *     from     CLAIM_PARTICIPATION) ,
//SRC_P as ( SELECT *     from     PARTICIPATION) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_PPT as ( SELECT *     from     PROVIDER_PARTICIPATING_TYPE) ,
//SRC_CUST as ( SELECT *     from     CUSTOMER) ,
//SRC_PRMRY as ( SELECT *     from     CLAIM_PTCP_PROV_PTCP_TYP) ,

---- LOGIC LAYER ----


LOGIC_CPPPT as ( SELECT 
		  CPPPT_ID                                           as                                           CPPPT_ID 
		, cast( CPPPT_EFF_DT as DATE )                       as                                       CPPPT_EFF_DT 
		, cast( CPPPT_END_DT as DATE )                       as                                       CPPPT_END_DT 
		, upper( TRIM( PROV_PTCP_TYP_CD ) )                  as                                   PROV_PTCP_TYP_CD 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, CLM_PTCP_PROV_ID                                   as                                   CLM_PTCP_PROV_ID 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CPPPT
            ),

LOGIC_CPP as ( SELECT 
		  CLM_PTCP_PROV_ID                                   as                                   CLM_PTCP_PROV_ID 
		, CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CPP
            ),

LOGIC_CP as ( SELECT 
		  CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, cast( CLM_PTCP_EFF_DT as DATE )                    as                                    CLM_PTCP_EFF_DT 
		, cast( CLM_PTCP_END_DT as DATE )                    as                                    CLM_PTCP_END_DT 
		, upper( TRIM( CLM_PTCP_DTL_COMT ) )                 as                                  CLM_PTCP_DTL_COMT 
		, upper( TRIM( CLM_PTCP_PRI_IND ) )                  as                                   CLM_PTCP_PRI_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, PTCP_ID                                            as                                            PTCP_ID 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_CP
            ),

LOGIC_P as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CUST_ID                                            as                                            CUST_ID 
		, upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		from SRC_P
            ),

LOGIC_C as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, upper( TRIM( CLM_REL_SNPSHT_IND ) )                as                                 CLM_REL_SNPSHT_IND 
		from SRC_C
            ),

LOGIC_PPT as ( SELECT 
		  upper( TRIM( PROV_PTCP_TYP_NM ) )                  as                                   PROV_PTCP_TYP_NM 
		, upper( TRIM( PROV_PTCP_TYP_CD ) )                  as                                   PROV_PTCP_TYP_CD 
		from SRC_PPT
            ),

LOGIC_CUST as ( SELECT 
		  upper( TRIM( CUST_NO ) )                           as                                            CUST_NO 
		, CUST_ID                                            as                                            CUST_ID 
		from SRC_CUST
            ),

LOGIC_PRMRY as ( SELECT 
		  CLM_PTCP_PROV_ID                                   as                                   CLM_PTCP_PROV_ID
		, cast( CPPPT_EFF_DT as DATE )                       as                                       CPPPT_EFF_DT 
		, CPPPT_END_DT                                       as                                       CPPPT_END_DT 
		, CPPPT_ID                                           as                                           CPPPT_ID 
		, upper( TRIM( PROV_PTCP_TYP_CD ) )                  as                                   PROV_PTCP_TYP_CD 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_PRMRY
            )

---- RENAME LAYER ----
,

RENAME_CPPPT as ( SELECT 
		  CPPPT_ID                                           as                                           CPPPT_ID
		, CPPPT_EFF_DT                                       as                                     CPPPT_EFF_DATE
		, CPPPT_END_DT                                       as                                     CPPPT_END_DATE
		, PROV_PTCP_TYP_CD                                   as                                   PROV_PTCP_TYP_CD
		, AUDIT_USER_ID_CREA                                 as                           CPPPT_AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                          CPPPT_AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                           CPPPT_AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                          CPPPT_AUDIT_USER_UPDT_DTM
		, CLM_PTCP_PROV_ID                                   as                             CPPPT_CLM_PTCP_PROV_ID
		, VOID_IND                                           as                                     CPPPT_VOID_IND 
				FROM     LOGIC_CPPPT   ), 
RENAME_CPP as ( SELECT 
		  CLM_PTCP_PROV_ID                                   as                                   CLM_PTCP_PROV_ID
		, CLM_PTCP_ID                                        as                                    CPP_CLM_PTCP_ID
		, VOID_IND                                           as                                       CPP_VOID_IND 
				FROM     LOGIC_CPP   ), 
RENAME_CP as ( SELECT 
		  CLM_PTCP_ID                                        as                                        CLM_PTCP_ID
		, CLM_PTCP_EFF_DT                                    as                                  CLM_PTCP_EFF_DATE
		, CLM_PTCP_END_DT                                    as                                  CLM_PTCP_END_DATE
		, CLM_PTCP_DTL_COMT                                  as                                   CLM_PTCP_DTL_CMT
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
		, AUDIT_USER_ID_CREA                                 as                              CP_AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                             CP_AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                              CP_AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                             CP_AUDIT_USER_UPDT_DTM
		, PTCP_ID                                            as                                         CP_PTCP_ID
		, VOID_IND                                           as                                        CP_VOID_IND 
				FROM     LOGIC_CP   ), 
RENAME_P as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CUST_ID                                            as                                            CUST_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD 
				FROM     LOGIC_P   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_PPT as ( SELECT 
		  PROV_PTCP_TYP_NM                                   as                                   PROV_PTCP_TYP_NM
		, PROV_PTCP_TYP_CD                                   as                               PPT_PROV_PTCP_TYP_CD 
				FROM     LOGIC_PPT   ), 
RENAME_CUST as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                       CUST_CUST_ID 
				FROM     LOGIC_CUST   ), 
RENAME_PRMRY as ( SELECT 
		  CLM_PTCP_PROV_ID                                   as                             PRMRY_CLM_PTCP_PROV_ID
		, CPPPT_EFF_DT                                       as                                 PRMRY_CPPPT_EFF_DT
		, CPPPT_END_DT                                       as                                 PRMRY_CPPPT_END_DT
		, CPPPT_ID                                           as                                     PRMRY_CPPPT_ID
		, PROV_PTCP_TYP_CD                                   as                             PRMRY_PROV_PTCP_TYP_CD
		, VOID_IND                                           as                                     PRMRY_VOID_IND 
				FROM     LOGIC_PRMRY   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_P                              as ( SELECT * from    RENAME_P 
                                            WHERE PTCP_TYP_CD = 'PROV'  ),
FILTER_CP                             as ( SELECT * from    RENAME_CP 
                                            WHERE CP_VOID_IND = 'N'  ),
FILTER_C                              as ( SELECT * from    RENAME_C 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_CPP                            as ( SELECT * from    RENAME_CPP 
                                            WHERE CPP_VOID_IND = 'N'  ),
FILTER_CPPPT                          as ( SELECT * from    RENAME_CPPPT 
                                            WHERE CPPPT_VOID_IND= 'N'  ),
FILTER_PRMRY                          as ( SELECT * from    RENAME_PRMRY 
                                            WHERE PRMRY_PROV_PTCP_TYP_CD = 'PRIMARYTREATING' AND PRMRY_VOID_IND = 'N' AND PRMRY_CPPPT_END_DT IS NULL  ),
FILTER_PPT                            as ( SELECT * from    RENAME_PPT   ),
FILTER_CUST                           as ( SELECT * from    RENAME_CUST   ),

---- JOIN LAYER ----

CPPPT as ( SELECT * 
				FROM  FILTER_CPPPT
				INNER JOIN FILTER_PPT ON  FILTER_CPPPT.PROV_PTCP_TYP_CD =  FILTER_PPT.PPT_PROV_PTCP_TYP_CD  ),
CPP as ( SELECT * 
				FROM  FILTER_CPP
				INNER JOIN CPPPT ON  FILTER_CPP.CLM_PTCP_PROV_ID = CPPPT_CLM_PTCP_PROV_ID 
						LEFT JOIN FILTER_PRMRY ON  FILTER_CPP.CLM_PTCP_PROV_ID =  FILTER_PRMRY.PRMRY_CLM_PTCP_PROV_ID  AND CPPPT.CPPPT_ID = FILTER_PRMRY.PRMRY_CPPPT_ID  ),
CP as ( SELECT * 
				FROM  FILTER_CP
				INNER JOIN CPP ON  FILTER_CP.CLM_PTCP_ID = CPP.CPP_CLM_PTCP_ID  ),
P as ( SELECT * 
				FROM  FILTER_P
				INNER JOIN CP ON  FILTER_P.PTCP_ID = CP.CP_PTCP_ID 
						INNER JOIN FILTER_C ON  FILTER_P.AGRE_ID =  FILTER_C.C_AGRE_ID 
						INNER JOIN FILTER_CUST ON  FILTER_P.CUST_ID =  FILTER_CUST.CUST_CUST_ID  )
SELECT 
CPPPT_ID
, CLM_PTCP_PROV_ID
, CLM_PTCP_ID
, PTCP_ID
, CLM_NO
, CLM_PTCP_EFF_DATE
, CLM_PTCP_END_DATE
, CPPPT_EFF_DATE
, CPPPT_END_DATE
, CASE WHEN PROV_PTCP_TYP_CD = 'PRIMARYTREATING' AND CPPPT_END_DATE IS NULL 
		AND ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY PRMRY_PROV_PTCP_TYP_CD, PRMRY_CPPPT_EFF_DT ) =1
 		THEN 'Y' ELSE 'N'  END AS CRNT_PHYS_OF_RCRD_IND
, AGRE_ID
, CUST_ID
, CLM_PTCP_DTL_CMT
, CLM_PTCP_PRI_IND
, PROV_PTCP_TYP_CD
, PROV_PTCP_TYP_NM
, CPPPT_AUDIT_USER_ID_CREA
, CPPPT_AUDIT_USER_CREA_DTM
, CPPPT_AUDIT_USER_ID_UPDT
, CPPPT_AUDIT_USER_UPDT_DTM
, CP_AUDIT_USER_ID_CREA
, CP_AUDIT_USER_CREA_DTM
, CP_AUDIT_USER_ID_UPDT
, CP_AUDIT_USER_UPDT_DTM
, CUST_NO
from P