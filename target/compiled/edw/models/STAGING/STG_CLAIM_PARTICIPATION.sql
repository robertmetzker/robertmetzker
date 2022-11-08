---- SRC LAYER ----
WITH
SRC_PAR as ( SELECT *     from     DEV_VIEWS.PCMP.PARTICIPATION ),
SRC_CP as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PARTICIPATION ),
SRC_CN as ( SELECT *     from     DEV_VIEWS.PCMP.CUSTOMER_NAME ),
SRC_CUST as ( SELECT *     from    DEV_VIEWS.PCMP.CUSTOMER ),
SRC_CLM as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM ),
SRC_PTYP as ( SELECT *     from    DEV_VIEWS.PCMP.PARTICIPATION_TYPE ),
SRC_ATTY as ( SELECT *     from     DEV_VIEWS.PCMP.CLAIM_PARTICIPATION_ATTY ),
SRC_PB as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_PARTICIPATION_BNFCY ),
SRC_CPD as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_PARTICIPATION_DEP ),
SRC_CPDR as ( SELECT *     from    DEV_VIEWS.PCMP.CLAIM_PARTICIPATION_DEP_REP ),
SRC_CPI as ( SELECT *     from    STAGING.STG_CLAIM_PARTICIPATION_INSRD_BWC ),
//SRC_PAR as ( SELECT *     from     PARTICIPATION) ,
//SRC_CP as ( SELECT *     from     CLAIM_PARTICIPATION) ,
//SRC_CN as ( SELECT *     from     CUSTOMER_NAME) ,
//SRC_CUST as ( SELECT *     from     CUSTOMER) ,
//SRC_CLM as ( SELECT *     from     CLAIM) ,
//SRC_PTYP as ( SELECT *     from     PARTICIPATION_TYPE) ,
//SRC_ATTY as ( SELECT *     from     CLAIM_PARTICIPATION_ATTY) ,
//SRC_PB as ( SELECT *     from     CLAIM_PARTICIPATION_BNFCY) ,
//SRC_CPD as ( SELECT *     from     CLAIM_PARTICIPATION_DEP) ,
//SRC_CPDR as ( SELECT *     from     CLAIM_PARTICIPATION_DEP_REP) ,
//SRC_CPI as ( SELECT *     from     CLAIM_PARTICIPATION_INSRD) ,

---- LOGIC LAYER ----

LOGIC_PAR as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		, CUST_ID                                            as                                            CUST_ID 
		, upper( PTCP_NOTE_IND )                             as                                      PTCP_NOTE_IND 
		from SRC_PAR
            ),
LOGIC_CP as ( SELECT 
		  CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, PTCP_ID                                            as                                            PTCP_ID 
		, CLM_PTCP_EFF_DT                                    as                                    CLM_PTCP_EFF_DT 
		, CLM_PTCP_END_DT                                    as                                    CLM_PTCP_END_DT 
		, upper( TRIM( CLM_PTCP_DTL_COMT ) )                 as                                  CLM_PTCP_DTL_COMT 
		, RLT_ID                                             as                                             RLT_ID 
		, upper( CLM_PTCP_PRI_IND )                          as                                   CLM_PTCP_PRI_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 

		from SRC_CP
            ),
LOGIC_CN as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, upper( TRIM( CUST_NM_TYP_CD ) )                    as                                     CUST_NM_TYP_CD 
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT 
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT 
		, upper( TRIM( CUST_NM_NM ) )                        as                                         CUST_NM_NM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CN
            ),
LOGIC_CUST as ( SELECT 
		  CUST_ID                                            as                                            CUST_ID 
		, upper( TRIM( CUST_NO ) )                           as                                            CUST_NO 
		from SRC_CUST
            ),
LOGIC_CLM as ( SELECT 
		  AGRE_ID                                            as                                            AGRE_ID 
		, upper( CLM_REL_SNPSHT_IND )                        as                                 CLM_REL_SNPSHT_IND 
		, upper( TRIM( CLM_NO ) )                            as                                             CLM_NO 
		from SRC_CLM
            ),
LOGIC_PTYP as ( SELECT 
		  upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		, upper( TRIM( PTCP_TYP_NM ) )                       as                                        PTCP_TYP_NM 
		, upper( PTCP_TYP_VOID_IND )                         as                                  PTCP_TYP_VOID_IND 
		from SRC_PTYP
            ),
LOGIC_ATTY as ( SELECT DISTINCT
		  CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, upper( TRIM( REP_TYP_CD ) )                        as                                         REP_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_ATTY
            ),
LOGIC_PB as ( SELECT 
		  CLM_PTCP_BNFCY_ID                                  as                                  CLM_PTCP_BNFCY_ID 
		, CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, upper( TRIM( BNFCY_TYP_CD ) )                      as                                       BNFCY_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_PB
            ),
LOGIC_CPD as ( SELECT 
		  CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, upper( CLM_PTCP_DEP_INSCHL_IND )                   as                            CLM_PTCP_DEP_INSCHL_IND 
		, upper( TRIM( LVL_OF_SUPT_TYP_CD ) )                as                                 LVL_OF_SUPT_TYP_CD 
		, upper( TRIM( CLM_PTCP_DEP_TYP_CD ) )               as                                CLM_PTCP_DEP_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CPD
            ),
LOGIC_CPDR as ( SELECT 
		  CLM_PTCP_DEP_REP_ID                                as                                CLM_PTCP_DEP_REP_ID 
		, CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, upper( TRIM( DEP_REP_TYP_CD ) )                    as                                     DEP_REP_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CPDR
            ),
LOGIC_CPI as ( SELECT 
		  CLM_PTCP_ID                                        as                                        CLM_PTCP_ID 
		, upper( CLM_PTCP_INSRD_SELF_INSRD_IND )             as                      CLM_PTCP_INSRD_SELF_INSRD_IND 
		, upper( CLM_PTCP_INSRD_CNTS_IND )                   as                            CLM_PTCP_INSRD_CNTS_IND 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, upper( CLM_PTCP_INSRD_PEO_IND )                    as                             CLM_PTCP_INSRD_PEO_IND 
		, CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID 
		from SRC_CPI
            )

---- RENAME LAYER ----
,

RENAME_PAR as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, CUST_ID                                            as                                            CUST_ID
		, PTCP_NOTE_IND                                      as                                      PTCP_NOTE_IND 
				FROM     LOGIC_PAR   ), 
RENAME_CP as ( SELECT 
		  CLM_PTCP_ID                                        as                                     CP_CLM_PTCP_ID
		, PTCP_ID                                            as                                         CP_PTCP_ID
		, CLM_PTCP_EFF_DT                                    as                                    CLM_PTCP_EFF_DT
		, CLM_PTCP_END_DT                                    as                                    CLM_PTCP_END_DT
		, CLM_PTCP_DTL_COMT                                  as                                  CLM_PTCP_DTL_COMT
		, RLT_ID                                             as                                             RLT_ID
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                        CP_VOID_IND
				FROM     LOGIC_CP   ), 
RENAME_CN as ( SELECT 
		  CUST_ID                                            as                                         CN_CUST_ID
		, CUST_NM_TYP_CD                                     as                                     CUST_NM_TYP_CD
		, CUST_NM_EFF_DT                                     as                                     CUST_NM_EFF_DT
		, CUST_NM_END_DT                                     as                                     CUST_NM_END_DT
		, CUST_NM_NM                                         as                                         CUST_NM_NM
		, VOID_IND                                           as                                        CN_VOID_IND 
				FROM     LOGIC_CN   ), 
RENAME_CUST as ( SELECT 
		  CUST_ID                                            as                                       CUST_CUST_ID
		, CUST_NO                                            as                                            CUST_NO 
				FROM     LOGIC_CUST   ), 
RENAME_CLM as ( SELECT 
		  AGRE_ID                                            as                                        CLM_AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, CLM_NO                                             as                                             CLM_NO 
				FROM     LOGIC_CLM   ), 
RENAME_PTYP as ( SELECT 
		  PTCP_TYP_CD                                        as                                   PTYP_PTCP_TYP_CD
		, PTCP_TYP_NM                                        as                                        PTCP_TYP_NM
		, PTCP_TYP_VOID_IND                                  as                                  PTCP_TYP_VOID_IND 
				FROM     LOGIC_PTYP   ), 
RENAME_ATTY as ( SELECT 
		  CLM_PTCP_ID                                        as                                   ATTY_CLM_PTCP_ID
		, REP_TYP_CD                                         as                                         REP_TYP_CD
		, VOID_IND                                           as                                      ATTY_VOID_IND 
				FROM     LOGIC_ATTY   ), 
RENAME_PB as ( SELECT 
		  CLM_PTCP_BNFCY_ID                                  as                                  CLM_PTCP_BNFCY_ID
		, CLM_PTCP_ID                                        as                                     PB_CLM_PTCP_ID
		, BNFCY_TYP_CD                                       as                                       BNFCY_TYP_CD
		, VOID_IND                                           as                                        PB_VOID_IND 
				FROM     LOGIC_PB   ), 
RENAME_CPD as ( SELECT 
		  CLM_PTCP_ID                                        as                                    CPD_CLM_PTCP_ID
		, CLM_PTCP_DEP_INSCHL_IND                            as                            CLM_PTCP_DEP_INSCHL_IND
		, LVL_OF_SUPT_TYP_CD                                 as                                 LVL_OF_SUPT_TYP_CD
		, CLM_PTCP_DEP_TYP_CD                                as                                CLM_PTCP_DEP_TYP_CD
		, VOID_IND                                           as                                       CPD_VOID_IND 
				FROM     LOGIC_CPD   ), 
RENAME_CPDR as ( SELECT 
		  CLM_PTCP_DEP_REP_ID                                as                                CLM_PTCP_DEP_REP_ID
		, CLM_PTCP_ID                                        as                                   CPDR_CLM_PTCP_ID
		, DEP_REP_TYP_CD                                     as                                     DEP_REP_TYP_CD
		, VOID_IND                                           as                                      CPDR_VOID_IND 
				FROM     LOGIC_CPDR   ), 
RENAME_CPI as ( SELECT 
		  CLM_PTCP_ID                                        as                                    CPI_CLM_PTCP_ID
		, CLM_PTCP_INSRD_SELF_INSRD_IND                      as                      CLM_PTCP_INSRD_SELF_INSRD_IND
		, CLM_PTCP_INSRD_CNTS_IND                            as                            CLM_PTCP_INSRD_CNTS_IND
		, VOID_IND                                           as                                       CPI_VOID_IND
		, CLM_PTCP_INSRD_PEO_IND                             as                             CLM_PTCP_INSRD_PEO_IND 
		, CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID 
				FROM     LOGIC_CPI   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PAR                            as ( SELECT * from    RENAME_PAR   ),
FILTER_CP                             as ( SELECT * from    RENAME_CP   ),
FILTER_CN                             as ( SELECT * from    RENAME_CN   ),
FILTER_CUST                           as ( SELECT * from    RENAME_CUST   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_PTYP                           as ( SELECT * from    RENAME_PTYP   ),
FILTER_ATTY                           as ( SELECT * from    RENAME_ATTY WHERE ATTY_VOID_IND = 'N' ),
FILTER_PB                             as ( SELECT * from    RENAME_PB   ),
FILTER_CPD                            as ( SELECT * from    RENAME_CPD   ),
FILTER_CPDR                           as ( SELECT * from    RENAME_CPDR   ),
FILTER_CPI                            as ( SELECT * from    RENAME_CPI   ),

---- JOIN LAYER ----

CP as ( SELECT * 
				FROM  FILTER_CP
				LEFT OUTER JOIN FILTER_PB ON  FILTER_CP.CP_CLM_PTCP_ID =  FILTER_PB.PB_CLM_PTCP_ID AND FILTER_PB.PB_VOID_IND = 'N' 
				LEFT OUTER JOIN FILTER_CPD ON  FILTER_CP.CP_CLM_PTCP_ID =  FILTER_CPD.CPD_CLM_PTCP_ID AND FILTER_CPD.CPD_VOID_IND = 'N' 
			    LEFT OUTER JOIN FILTER_CPDR ON  FILTER_CP.CP_CLM_PTCP_ID =  FILTER_CPDR.CPDR_CLM_PTCP_ID AND FILTER_CPDR.CPDR_VOID_IND = 'N' 
				LEFT OUTER JOIN FILTER_CPI ON  FILTER_CP.CP_CLM_PTCP_ID =  FILTER_CPI.CPI_CLM_PTCP_ID
				LEFT OUTER JOIN FILTER_ATTY ON  FILTER_CP.CP_CLM_PTCP_ID =  FILTER_ATTY.ATTY_CLM_PTCP_ID  ),
 
-- SELECT * FROM CP
                
PAR as ( SELECT * 
				FROM  FILTER_PAR
				INNER JOIN CP ON  FILTER_PAR.PTCP_ID = CP.CP_PTCP_ID 
				INNER JOIN FILTER_CN ON  FILTER_PAR.CUST_ID =  FILTER_CN.CN_CUST_ID AND FILTER_CN.CUST_NM_TYP_CD in ('PRSN_NM', 'BUSN_LEGAL_NM') AND FILTER_CN.CN_VOID_IND = 'N' 
                                                AND FILTER_CN.CUST_NM_EFF_DT <= SYSDATE() AND (FILTER_CN.CUST_NM_END_DT > SYSDATE() or FILTER_CN.CUST_NM_END_DT is null) 
				INNER JOIN FILTER_CUST ON  FILTER_PAR.CUST_ID =  FILTER_CUST.CUST_CUST_ID 
				INNER JOIN FILTER_CLM ON  FILTER_PAR.AGRE_ID =  FILTER_CLM.CLM_AGRE_ID AND FILTER_CLM.CLM_REL_SNPSHT_IND = 'N' 
				LEFT OUTER JOIN FILTER_PTYP ON  FILTER_PAR.PTCP_TYP_CD =  FILTER_PTYP.PTYP_PTCP_TYP_CD )

---- ETL LAYER ----

SELECT 
   PTCP_ID
 , AGRE_ID
 , PTCP_TYP_CD
 , CUST_ID
 , PTCP_NOTE_IND
 , CP_CLM_PTCP_ID
 , CP_PTCP_ID
 , CASE WHEN PTCP_TYP_CD = 'INSRD' THEN DATE(AUDIT_USER_CREA_DTM) ELSE DATE(CLM_PTCP_EFF_DT) END AS CLM_INSRD_PTCP_EFF_DT
 , CASE WHEN PTCP_TYP_CD = 'INSRD' THEN DATE(AUDIT_USER_UPDT_DTM) ELSE DATE(CLM_PTCP_END_DT) END AS CLM_INSRD_PTCP_END_DT
 , CLM_PTCP_DTL_COMT
 , RLT_ID
 , CLM_PTCP_PRI_IND
 , AUDIT_USER_ID_CREA
 , AUDIT_USER_CREA_DTM
 , AUDIT_USER_ID_UPDT
 , AUDIT_USER_UPDT_DTM
 , CP_VOID_IND
 , CN_CUST_ID
 , CUST_NM_TYP_CD
 , CUST_NM_EFF_DT
 , CUST_NM_END_DT
 , CUST_NM_NM
 , CN_VOID_IND
 , CUST_CUST_ID
 , CUST_NO
 , CLM_AGRE_ID
 , CLM_REL_SNPSHT_IND
 , CLM_NO
 , PTYP_PTCP_TYP_CD
 , PTCP_TYP_NM
 , PTCP_TYP_VOID_IND
 , ATTY_CLM_PTCP_ID
 , REP_TYP_CD
 , ATTY_VOID_IND
 , CLM_PTCP_BNFCY_ID
 , PB_CLM_PTCP_ID
 , BNFCY_TYP_CD
 , PB_VOID_IND
 , CPD_CLM_PTCP_ID
 , CLM_PTCP_DEP_INSCHL_IND
 , LVL_OF_SUPT_TYP_CD
 , CLM_PTCP_DEP_TYP_CD
 , CPD_VOID_IND
 , CLM_PTCP_DEP_REP_ID
 , CPDR_CLM_PTCP_ID
 , DEP_REP_TYP_CD
 , CPDR_VOID_IND
 , CPI_CLM_PTCP_ID
 , CLM_PTCP_INSRD_SELF_INSRD_IND
 , CLM_PTCP_INSRD_CNTS_IND
 , CPI_VOID_IND
 , CLM_PTCP_INSRD_PEO_IND 
 , CLM_PTCP_INSRD_ID
 , CAST (CLM_PTCP_EFF_DT AS DATE) AS CLM_PTCP_EFF_DT
 , CAST (CLM_PTCP_END_DT AS DATE) AS CLM_PTCP_END_DT
from PAR