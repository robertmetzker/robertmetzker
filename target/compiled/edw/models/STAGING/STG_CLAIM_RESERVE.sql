---- SRC LAYER ----
WITH
SRC_CR as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_RESERVE ),
SRC_C as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM ),
SRC_SRC as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_RESERVE_SOURCE_TYPE ),
SRC_STS as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_RESERVE_STATUS_TYPE ),
SRC_CX as ( SELECT *     from      DEV_VIEWS.PCMP.CLM_RSRV_STS_CRSR_XREF ),
SRC_RSN as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_RESERVE_STATUS_RSN_TYP ),
//SRC_CR as ( SELECT *     from     CLAIM_RESERVE) ,
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_SRC as ( SELECT *     from     CLAIM_RESERVE_SOURCE_TYPE) ,
//SRC_STS as ( SELECT *     from     CLAIM_RESERVE_STATUS_TYPE) ,
//SRC_CX as ( SELECT *     from     CLM_RSRV_STS_CRSR_XREF) ,
//SRC_RSN as ( SELECT *     from     CLAIM_RESERVE_STATUS_RSN_TYP) ,

---- LOGIC LAYER ----


LOGIC_CR as ( SELECT 
		  CLM_RSRV_ID                                        as                                        CLM_RSRV_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, upper( TRIM( CLM_RSRV_SRC_TYP_CD ) )               as                                CLM_RSRV_SRC_TYP_CD 
		, upper( TRIM( CLM_RSRV_STS_TYP_CD ) )               as                                CLM_RSRV_STS_TYP_CD 
		, CRSCRSRX_ID                                        as                                        CRSCRSRX_ID 
		, CLM_RSRV_NO                                        as                                        CLM_RSRV_NO 
		, cast( CLM_RSRV_STS_EFF_DT as DATE )                as                                CLM_RSRV_STS_EFF_DT 
		, cast( CLM_RSRV_STS_END_DT as DATE )                as                                CLM_RSRV_STS_END_DT 
		, upper( TRIM( CLM_RSRV_STS_COMT ) )                 as                                  CLM_RSRV_STS_COMT 
		, upper( CLM_RSRV_NOTE_IND )                         as                                  CLM_RSRV_NOTE_IND 
		, upper( TRIM( CLM_RSRV_COMT ) )                     as                                      CLM_RSRV_COMT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CR
            ),

LOGIC_C as ( SELECT 
		  upper( TRIM( CLM_NO ) )                            as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		from SRC_C
            ),

LOGIC_SRC as ( SELECT 
		  upper( TRIM( CLM_RSRV_SRC_TYP_NM ) )               as                                CLM_RSRV_SRC_TYP_NM 
		, upper( TRIM( CLM_RSRV_SRC_TYP_CD ) )               as                                CLM_RSRV_SRC_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_SRC
            ),

LOGIC_STS as ( SELECT 
		  upper( TRIM( CLM_RSRV_STS_TYP_NM ) )               as                                CLM_RSRV_STS_TYP_NM 
		, upper( TRIM( CLM_RSRV_STS_TYP_CD ) )               as                                CLM_RSRV_STS_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_STS
            ),

LOGIC_CX as ( SELECT 
		  upper( TRIM( CLM_RSRV_STS_RSN_TYP_CD ) )           as                            CLM_RSRV_STS_RSN_TYP_CD 
		, CRSCRSRX_ID                                        as                                        CRSCRSRX_ID 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_CX
            ),

LOGIC_RSN as ( SELECT 
		  upper( TRIM( CLM_RSRV_STS_RSN_TYP_NM ) )           as                            CLM_RSRV_STS_RSN_TYP_NM 
		, upper( TRIM( CLM_RSRV_STS_RSN_TYP_CD ) )           as                            CLM_RSRV_STS_RSN_TYP_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		from SRC_RSN
            )

---- RENAME LAYER ----
,

RENAME_CR as ( SELECT 
		  CLM_RSRV_ID                                        as                                        CLM_RSRV_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_RSRV_SRC_TYP_CD                                as                                CLM_RSRV_SRC_TYP_CD
		, CLM_RSRV_STS_TYP_CD                                as                                CLM_RSRV_STS_TYP_CD
		, CRSCRSRX_ID                                        as                                        CRSCRSRX_ID
		, CLM_RSRV_NO                                        as                                        CLM_RSRV_NO
		, CLM_RSRV_STS_EFF_DT                                as                                CLM_RSRV_STS_EFF_DT
		, CLM_RSRV_STS_END_DT                                as                                CLM_RSRV_STS_END_DT
		, CLM_RSRV_STS_COMT                                  as                                  CLM_RSRV_STS_COMT
		, CLM_RSRV_NOTE_IND                                  as                                  CLM_RSRV_NOTE_IND
		, CLM_RSRV_COMT                                      as                                      CLM_RSRV_COMT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND 
				FROM     LOGIC_CR   ), 
RENAME_C as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                          C_AGRE_ID 
				FROM     LOGIC_C   ), 
RENAME_SRC as ( SELECT 
		  CLM_RSRV_SRC_TYP_NM                                as                                CLM_RSRV_SRC_TYP_NM
		, CLM_RSRV_SRC_TYP_CD                                as                            SRC_CLM_RSRV_SRC_TYP_CD
		, VOID_IND                                           as                                       SRC_VOID_IND 
				FROM     LOGIC_SRC   ), 
RENAME_STS as ( SELECT 
		  CLM_RSRV_STS_TYP_NM                                as                                CLM_RSRV_STS_TYP_NM
		, CLM_RSRV_STS_TYP_CD                                as                            STS_CLM_RSRV_STS_TYP_CD
		, VOID_IND                                           as                                       STS_VOID_IND 
				FROM     LOGIC_STS   ), 
RENAME_CX as ( SELECT 
		  CLM_RSRV_STS_RSN_TYP_CD                            as                            CLM_RSRV_STS_RSN_TYP_CD
		, CRSCRSRX_ID                                        as                                     CX_CRSCRSRX_ID
		, VOID_IND                                           as                                        CX_VOID_IND 
				FROM     LOGIC_CX   ), 
RENAME_RSN as ( SELECT 
		  CLM_RSRV_STS_RSN_TYP_NM                            as                            CLM_RSRV_STS_RSN_TYP_NM
		, CLM_RSRV_STS_RSN_TYP_CD                            as                        RSN_CLM_RSRV_STS_RSN_TYP_CD
		, VOID_IND                                           as                                       RSN_VOID_IND 
				FROM     LOGIC_RSN   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CR                             as ( SELECT * from    RENAME_CR   ),
FILTER_SRC                            as ( SELECT * from    RENAME_SRC 
                                            WHERE SRC_VOID_IND = 'N'  ),
FILTER_STS                            as ( SELECT * from    RENAME_STS 
                                            WHERE STS_VOID_IND = 'N'  ),
FILTER_CX                             as ( SELECT * from    RENAME_CX 
                                            WHERE CX_VOID_IND = 'N'  ),
FILTER_RSN                            as ( SELECT * from    RENAME_RSN 
                                            WHERE RSN_VOID_IND = 'N'  ),
FILTER_C                              as ( SELECT * from    RENAME_C   ),

---- JOIN LAYER ----

CX as ( SELECT * 
				FROM  FILTER_CX
				LEFT JOIN FILTER_RSN ON  FILTER_CX.CLM_RSRV_STS_RSN_TYP_CD =  FILTER_RSN.RSN_CLM_RSRV_STS_RSN_TYP_CD  ),
CR as ( SELECT * 
				FROM  FILTER_CR
				LEFT JOIN FILTER_SRC ON  FILTER_CR.CLM_RSRV_SRC_TYP_CD =  FILTER_SRC.SRC_CLM_RSRV_SRC_TYP_CD 
								LEFT JOIN FILTER_STS ON  FILTER_CR.CLM_RSRV_STS_TYP_CD =  FILTER_STS.STS_CLM_RSRV_STS_TYP_CD 
						LEFT JOIN CX ON  FILTER_CR.CRSCRSRX_ID = CX.CX_CRSCRSRX_ID 
								LEFT JOIN FILTER_C ON  FILTER_CR.AGRE_ID =  FILTER_C.C_AGRE_ID  )
SELECT * 
from CR