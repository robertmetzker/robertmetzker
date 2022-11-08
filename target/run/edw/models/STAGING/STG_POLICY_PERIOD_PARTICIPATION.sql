

      create or replace  table DEV_EDW.STAGING.STG_POLICY_PERIOD_PARTICIPATION  as
      (---- SRC LAYER ----
WITH
SRC_PAR as ( SELECT *     from      DEV_VIEWS.PCMP.PARTICIPATION ),
SRC_PTYP as ( SELECT *     from      DEV_VIEWS.PCMP.PARTICIPATION_TYPE ),
SRC_A as ( SELECT *     from      DEV_VIEWS.PCMP.AGREEMENT ),
SRC_PP as ( SELECT *     from      DEV_VIEWS.PCMP.POLICY_PERIOD ),
SRC_CUST as ( SELECT *     FROM     DEV_VIEWS.PCMP.CUSTOMER ),
SRC_PPP as ( SELECT *     from      DEV_VIEWS.PCMP.POLICY_PERIOD_PARTICIPATION ),
SRC_PPPI as ( SELECT *     from      DEV_VIEWS.PCMP.POLICY_PERIOD_PTCP_INS ),
SRC_BPPPI as ( SELECT *     from      DEV_VIEWS.PCMP.BWC_POLICY_PERIOD_PTCP_INS ),
SRC_PPPIE as ( SELECT *     from      DEV_VIEWS.PCMP.POLICY_PERIOD_PTCP_INCL_EXCL ),
SRC_CT as ( SELECT *     from      DEV_VIEWS.PCMP.COVERED_TYPE ),
SRC_TT as ( SELECT *     from      DEV_VIEWS.PCMP.TITLE_TYPE ),
SRC_PPPER as ( SELECT *     from      DEV_VIEWS.PCMP.PLCY_PRD_PTCP_EMPLR_REP ),
SRC_ERT as ( SELECT *     from      DEV_VIEWS.PCMP.EMPLOYER_REP_TYPE ),
SRC_PGRE as ( SELECT *     from      DEV_VIEWS.PCMP.BWC_PTCP_GRP_RTRO_EXPRN_RT ),
//SRC_PAR as ( SELECT *     from     PARTICIPATION) ,
//SRC_PTYP as ( SELECT *     from     PARTICIPATION_TYPE) ,
//SRC_A as ( SELECT *     from     AGREEMENT) ,
//SRC_PP as ( SELECT *     from     POLICY_PERIOD) ,
//SRC_CUST as ( SELECT *     FROM     CUSTOMER) ,
//SRC_PPP as ( SELECT *     from     POLICY_PERIOD_PARTICIPATION) ,
//SRC_PPPI as ( SELECT *     from     POLICY_PERIOD_PTCP_INS) ,
//SRC_BPPPI as ( SELECT *     from     BWC_POLICY_PERIOD_PTCP_INS) ,
//SRC_PPPIE as ( SELECT *     from     POLICY_PERIOD_PTCP_INCL_EXCL) ,
//SRC_CT as ( SELECT *     from     COVERED_TYPE) ,
//SRC_TT as ( SELECT *     from     TITLE_TYPE) ,
//SRC_PPPER as ( SELECT *     from     PLCY_PRD_PTCP_EMPLR_REP) ,
//SRC_ERT as ( SELECT *     from     EMPLOYER_REP_TYPE) ,
//SRC_PGRE as ( SELECT *     from     BWC_PTCP_GRP_RTRO_EXPRN_RT) ,

---- LOGIC LAYER ----


LOGIC_PAR as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID 
		, upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CUST_ID                                            as                                            CUST_ID 
		, upper( TRIM( PTCP_NOTE_IND ) )                     as                                      PTCP_NOTE_IND
			from SRC_PAR
            ),

LOGIC_PTYP as ( SELECT 
		  upper( TRIM( PTCP_TYP_NM ) )                       as                                        PTCP_TYP_NM 
		, upper( TRIM( PTCP_TYP_CD ) )                       as                                        PTCP_TYP_CD 
		from SRC_PTYP
            ),

LOGIC_A as ( SELECT 
		  upper( TRIM( AGRE_TYP_CD ) )                       as                                        AGRE_TYP_CD 
		, AGRE_ID                                            as                                            AGRE_ID 
		from SRC_A
            ),

LOGIC_PP as ( SELECT 
		  upper( TRIM( PLCY_NO ) )                           as                                            PLCY_NO 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		from SRC_PP
		    ),

 LOGIC_CUST as ( SELECT 
		  TRIM( CUST_NO )                                    as                                            CUST_NO 
		, CUST_ID                                            as                                            CUST_ID 
		FROM SRC_CUST
            ),          

LOGIC_PPP as ( SELECT 
		  cast( PLCY_PRD_PTCP_EFF_DT as DATE )               as                               PLCY_PRD_PTCP_EFF_DT 
		, cast( PLCY_PRD_PTCP_END_DT as DATE )               as                               PLCY_PRD_PTCP_END_DT 
		, upper( TRIM( PLCY_PRD_PTCP_RN_IND ) )              as                               PLCY_PRD_PTCP_RN_IND 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		, PTCP_ID                                            as                                            PTCP_ID 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		from SRC_PPP
            ),

LOGIC_PPPI as ( SELECT 
		  EXTRNL_NO                                          as                                          EXTRNL_NO 
		, upper( TRIM( PLCY_PRD_PTCP_INS_PRI_IND ) )         as                          PLCY_PRD_PTCP_INS_PRI_IND 
		, PTCP_ID                                            as                                            PTCP_ID 
		from SRC_PPPI
            ),

LOGIC_BPPPI as ( SELECT 
		  cast( PLCY_PRD_PTCP_INS_FST_HIRE_DT as DATE )      as                      PLCY_PRD_PTCP_INS_FST_HIRE_DT 
		, upper( TRIM( PLCY_PRD_PTCP_INS_CERT_IND ) )        as                         PLCY_PRD_PTCP_INS_CERT_IND
		, PTCP_ID                                            as                                            PTCP_ID  
		from SRC_BPPPI
            ),

LOGIC_PPPIE as ( SELECT 
		  upper( TRIM( PPPIE_COV_IND ) )                     as                                      PPPIE_COV_IND 
		, upper( TRIM( COV_TYP_CD ) )                        as                                         COV_TYP_CD 
		, upper( TRIM( TTL_TYP_CD ) )                        as                                         TTL_TYP_CD 
		, PTCP_ID                                            as                                            PTCP_ID 
		from SRC_PPPIE
            ),

LOGIC_CT as ( SELECT 
		  upper( TRIM( COV_TYP_NM ) )                        as                                         COV_TYP_NM 
		, upper( TRIM( COV_TYP_CD ) )                        as                                         COV_TYP_CD 
		from SRC_CT
            ),

LOGIC_TT as ( SELECT 
		  upper( TRIM( TTL_TYP_NM ) )                        as                                         TTL_TYP_NM 
		, upper( TRIM( TTL_TYP_CD ) )                        as                                         TTL_TYP_CD 
		from SRC_TT
            ),

LOGIC_PPPER as ( SELECT 
		  upper( TRIM( EMPLR_REP_TYP_CD ) )                  as                                   EMPLR_REP_TYP_CD 
		, PTCP_ID                                            as                                            PTCP_ID 
		from SRC_PPPER
            ),

LOGIC_ERT as ( SELECT 
		  upper( TRIM( EMPLR_REP_TYP_NM ) )                  as                                   EMPLR_REP_TYP_NM 
		, upper( TRIM( EMPLR_REP_TYP_CD ) )                  as                                   EMPLR_REP_TYP_CD 
		from SRC_ERT
            ),

LOGIC_PGRE as ( SELECT 
		  upper( TRIM( PGRER_GRP_NO ) )                      as                                       PGRER_GRP_NO 
		, PTCP_ID                                            as                                            PTCP_ID 
		from SRC_PGRE
            )

---- RENAME LAYER ----
,

RENAME_PAR as ( SELECT 
		  PTCP_ID                                            as                                            PTCP_ID
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, AGRE_ID                                            as                                            AGRE_ID
		, CUST_ID                                            as                                            CUST_ID
		, PTCP_NOTE_IND                                      as                                      PTCP_NOTE_IND
				FROM     LOGIC_PAR   ), 
RENAME_PTYP as ( SELECT 
		  PTCP_TYP_NM                                        as                                        PTCP_TYP_NM
		, PTCP_TYP_CD                                        as                                   PTYP_PTCP_TYP_CD 
				FROM     LOGIC_PTYP   ), 
RENAME_A as ( SELECT 
		  AGRE_TYP_CD                                        as                                        AGRE_TYP_CD
		, AGRE_ID                                            as                                          A_AGRE_ID 
				FROM     LOGIC_A   ), 
RENAME_PP as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, VOID_IND                                           as                                        PP_VOID_IND
				FROM     LOGIC_PP   ),
RENAME_CUST       as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, CUST_ID                                            as                                       CUST_CUST_ID 
				FROM     LOGIC_CUST   ), 				 
RENAME_PPP as ( SELECT 
		  PLCY_PRD_PTCP_EFF_DT                               as                             PLCY_PRD_PTCP_EFF_DATE
		, PLCY_PRD_PTCP_END_DT                               as                             PLCY_PRD_PTCP_END_DATE
		, PLCY_PRD_PTCP_RN_IND                               as                               PLCY_PRD_PTCP_RN_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                       PPP_VOID_IND
		, PTCP_ID                                            as                                        PPP_PTCP_ID
		, PLCY_PRD_ID                                        as                                    PPP_PLCY_PRD_ID 
				FROM     LOGIC_PPP   ), 
RENAME_PPPI as ( SELECT 
		  EXTRNL_NO                                          as                                        BUSN_SEQ_NO
		, PLCY_PRD_PTCP_INS_PRI_IND                          as                          PLCY_PRD_PTCP_INS_PRI_IND
		, PTCP_ID                                            as                                       PPPI_PTCP_ID 
				FROM     LOGIC_PPPI   ), 
RENAME_BPPPI as ( SELECT 
		  PLCY_PRD_PTCP_INS_FST_HIRE_DT                      as                    PLCY_PRD_PTCP_INS_FST_HIRE_DATE
		, PLCY_PRD_PTCP_INS_CERT_IND                         as                                     INSRD_CERT_IND 
		, PTCP_ID                                            as                                      BPPPI_PTCP_ID
				FROM     LOGIC_BPPPI   ), 
RENAME_PPPIE as ( SELECT 
		  PPPIE_COV_IND                                      as                                      PPPIE_COV_IND
		, COV_TYP_CD                                         as                                         COV_TYP_CD
		, TTL_TYP_CD                                         as                                         TTL_TYP_CD
		, PTCP_ID                                            as                                      PPPIE_PTCP_ID
				FROM     LOGIC_PPPIE   ), 
RENAME_CT as ( SELECT 
		  COV_TYP_NM                                         as                                         COV_TYP_NM
		, COV_TYP_CD                                         as                                      CT_COV_TYP_CD 
				FROM     LOGIC_CT   ), 
RENAME_TT as ( SELECT 
		  TTL_TYP_NM                                         as                                         TTL_TYP_NM
		, TTL_TYP_CD                                         as                                      TT_TTL_TYP_CD 
				FROM     LOGIC_TT   ), 
RENAME_PPPER as ( SELECT 
		  EMPLR_REP_TYP_CD                                   as                                   EMPLR_REP_TYP_CD
		, PTCP_ID                                            as                                      PPPER_PTCP_ID 
				FROM     LOGIC_PPPER   ), 
RENAME_ERT as ( SELECT 
		  EMPLR_REP_TYP_NM                                   as                                   EMPLR_REP_TYP_NM
		, EMPLR_REP_TYP_CD                                   as                               ERT_EMPLR_REP_TYP_CD 
				FROM     LOGIC_ERT   ), 
RENAME_PGRE as ( SELECT 
		  PGRER_GRP_NO                                       as                                       PGRER_GRP_NO
		, PTCP_ID                                            as                                       PGRE_PTCP_ID 
				FROM     LOGIC_PGRE   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PAR                            as ( SELECT * from    RENAME_PAR   ),
FILTER_A                              as ( SELECT * from    RENAME_A   ),
FILTER_PPP                            as ( SELECT * from    RENAME_PPP 
                                            WHERE PPP_VOID_IND = 'N'  ),
FILTER_PP                             as ( SELECT * from    RENAME_PP 
                                            WHERE PP_VOID_IND = 'N' AND PLCY_NO IS NOT NULL  ),
FILTER_PTYP                           as ( SELECT * from    RENAME_PTYP   ),
FILTER_PPPI                           as ( SELECT * from    RENAME_PPPI   ),
FILTER_BPPPI                          as ( SELECT * from    RENAME_BPPPI   ),
FILTER_PPPIE                          as ( SELECT * from    RENAME_PPPIE   ),
FILTER_CT                             as ( SELECT * from    RENAME_CT   ),
FILTER_TT                             as ( SELECT * from    RENAME_TT   ),
FILTER_PPPER                          as ( SELECT * from    RENAME_PPPER   ),
FILTER_ERT                            as ( SELECT * from    RENAME_ERT   ),
FILTER_PGRE                           as ( SELECT * from    RENAME_PGRE   ),
FILTER_CUST                           as ( SELECT * FROM    RENAME_CUST   ),

---- JOIN LAYER ----

PPPI as ( SELECT * 
				FROM  FILTER_PPPI
				LEFT OUTER JOIN FILTER_BPPPI ON  FILTER_PPPI.PPPI_PTCP_ID =  FILTER_BPPPI.BPPPI_PTCP_ID  ),
PPPIE as ( SELECT * 
				FROM  FILTER_PPPIE 
						LEFT OUTER JOIN FILTER_CT ON  FILTER_PPPIE.COV_TYP_CD =  FILTER_CT.CT_COV_TYP_CD 
								LEFT OUTER JOIN FILTER_TT ON  FILTER_PPPIE.TTL_TYP_CD =  FILTER_TT.TT_TTL_TYP_CD  ),
PAR as ( SELECT * 
				FROM  FILTER_PAR
				INNER JOIN FILTER_A ON  FILTER_PAR.AGRE_ID =  FILTER_A.A_AGRE_ID 
				INNER JOIN FILTER_PPP ON  FILTER_PAR.PTCP_ID =  FILTER_PPP.PPP_PTCP_ID 
				INNER JOIN FILTER_PP ON  FILTER_PPP.PPP_PLCY_PRD_ID =  FILTER_PP.PLCY_PRD_ID
				LEFT OUTER JOIN FILTER_PTYP ON  FILTER_PAR.PTCP_TYP_CD =  FILTER_PTYP.PTYP_PTCP_TYP_CD 
						LEFT OUTER JOIN PPPI ON  FILTER_PAR.PTCP_ID = PPPI.PPPI_PTCP_ID 
						LEFT OUTER JOIN PPPIE ON  FILTER_PAR.PTCP_ID = PPPIE.PPPIE_PTCP_ID 
								LEFT OUTER JOIN FILTER_PPPER ON  FILTER_PAR.PTCP_ID =  FILTER_PPPER.PPPER_PTCP_ID 
									LEFT OUTER JOIN FILTER_ERT ON  FILTER_PPPER.EMPLR_REP_TYP_CD =  FILTER_ERT.ERT_EMPLR_REP_TYP_CD
								LEFT OUTER JOIN FILTER_PGRE ON  FILTER_PAR.PTCP_ID =  FILTER_PGRE.PGRE_PTCP_ID  
								INNER JOIN FILTER_CUST ON  FILTER_PAR.CUST_ID =  FILTER_CUST.CUST_CUST_ID )
SELECT 
PTCP_ID
, PTCP_TYP_CD
, PTCP_TYP_NM
, AGRE_ID
, AGRE_TYP_CD
, PLCY_NO
, PLCY_PRD_ID
, CUST_ID
, CUST_NO
, PLCY_PRD_PTCP_EFF_DATE
, PLCY_PRD_PTCP_END_DATE
, PLCY_PRD_PTCP_RN_IND
, BUSN_SEQ_NO
, PLCY_PRD_PTCP_INS_PRI_IND
, PLCY_PRD_PTCP_INS_FST_HIRE_DATE
, INSRD_CERT_IND
, PPPIE_COV_IND
, COV_TYP_CD
, COV_TYP_NM
, TTL_TYP_CD
, TTL_TYP_NM
, EMPLR_REP_TYP_CD
, EMPLR_REP_TYP_NM
, PGRER_GRP_NO
, PTCP_NOTE_IND
, AUDIT_USER_ID_CREA
, AUDIT_USER_CREA_DTM
, AUDIT_USER_ID_UPDT
, AUDIT_USER_UPDT_DTM
, CASE WHEN 
      CURRENT_DATE BETWEEN PLCY_PRD_PTCP_EFF_DATE AND nvl(PLCY_PRD_PTCP_END_DATE, CURRENT_DATE) THEN 'Y'  
      ELSE 'N'
      END AS CRNT_PLCY_PRD_PTCP_IND
from PAR
      );
    