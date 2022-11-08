---- SRC LAYER ----
WITH
SRC_N as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE ),
SRC_NT as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE_TYPE ),
SRC_NCT as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE_CATEGORY_TYPE ),
SRC_NCA as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE_CALL_TYPE ),
SRC_NCF as ( SELECT *     from     DEV_VIEWS.PCMP.NOTE_CALL_FROM_TYPE ),
//SRC_N as ( SELECT *     from     NOTE) ,
//SRC_NT as ( SELECT *     from     NOTE_TYPE) ,
//SRC_NCT as ( SELECT *     from     NOTE_CATEGORY_TYPE) ,
//SRC_NCA as ( SELECT *     from     NOTE_CALL_TYPE) ,
//SRC_NCF as ( SELECT *     from     NOTE_CALL_FROM_TYPE) ,

---- LOGIC LAYER ----

LOGIC_N as ( SELECT 
		  NOTE_ID                                            AS                                            NOTE_ID 
		, upper( TRIM( APP_DTL_LVL_CD ) )                    AS                                     APP_DTL_LVL_CD 
		, upper( TRIM( NOTE_TYP_CD ) )                       AS                                        NOTE_TYP_CD 
		, upper( TRIM( NOTE_CTG_TYP_CD ) )                   AS                                    NOTE_CTG_TYP_CD 
		, upper( TRIM( NOTE_SBJ_TEXT ) )                     AS                                      NOTE_SBJ_TEXT 
		, upper( TRIM( NOTE_CALL_TYP_CD ) )                  AS                                   NOTE_CALL_TYP_CD 
		, upper( TRIM( NOTE_CALL_FR_TYP_CD ) )               AS                                NOTE_CALL_FR_TYP_CD 
		, upper( TRIM( NOTE_CLLR_PRSN_NM ) )                 AS                                  NOTE_CLLR_PRSN_NM 
		, upper( NOTE_HI_PRTY_IND )                          AS                                   NOTE_HI_PRTY_IND 
		, upper( NOTE_NO_PRNT_IND )                          AS                                   NOTE_NO_PRNT_IND 
		, upper( NOTE_PUB_IND )                              AS                                       NOTE_PUB_IND 
		, upper( NOTE_CNFDTL_IND )                           AS                                    NOTE_CNFDTL_IND 
		, upper( TRIM( NOTE_EDI_REF_NO ) )                   AS                                    NOTE_EDI_REF_NO 
		, NOTE_TYP_TMPL_ID                                   AS                                   NOTE_TYP_TMPL_ID 
		, upper( NOTE_VOID_IND )                             AS                                      NOTE_VOID_IND 
		, AUDIT_USER_ID_CREA                                 AS                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                AS                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 AS                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                AS                                AUDIT_USER_UPDT_DTM 
		from SRC_N
            ),
LOGIC_NT as ( SELECT 
		  upper( TRIM( NOTE_TYP_NM ) )                       AS                                        NOTE_TYP_NM 
		, upper( TRIM( NOTE_TYP_CD ) )                       AS                                        NOTE_TYP_CD 
		, upper( NOTE_TYP_VOID_IND )                         AS                                  NOTE_TYP_VOID_IND 
		from SRC_NT
            ),
LOGIC_NCT as ( SELECT 
		  upper( TRIM( NOTE_CTG_TYP_NM ) )                   AS                                    NOTE_CTG_TYP_NM 
		, upper( TRIM( NOTE_CTG_TYP_CD ) )                   AS                                    NOTE_CTG_TYP_CD 
		, upper( NOTE_CTG_TYP_VOID_IND )                     AS                              NOTE_CTG_TYP_VOID_IND 
		from SRC_NCT
            ),
LOGIC_NCA as ( SELECT 
		  upper( TRIM( NOTE_CALL_TYP_NM ) )                  AS                                   NOTE_CALL_TYP_NM 
		, upper( TRIM( NOTE_CALL_TYP_CD ) )                  AS                                   NOTE_CALL_TYP_CD 
		, upper( NOTE_CALL_TYP_VOID_IND )                    AS                             NOTE_CALL_TYP_VOID_IND 
		from SRC_NCA
            ),
LOGIC_NCF as ( SELECT 
		  upper( TRIM( NOTE_CALL_FR_TYP_NM ) )               AS                                NOTE_CALL_FR_TYP_NM 
		, upper( TRIM( NOTE_CALL_FR_TYP_CD ) )               AS                                NOTE_CALL_FR_TYP_CD 
		, upper( NOTE_CALL_FR_TYP_VOID_IND )                 AS                          NOTE_CALL_FR_TYP_VOID_IND 
		from SRC_NCF
            )

---- RENAME LAYER ----
,

RENAME_N as ( SELECT 
		  NOTE_ID                                            as                                            NOTE_ID
		, APP_DTL_LVL_CD                                     as                                     APP_DTL_LVL_CD
		, NOTE_TYP_CD                                        as                                        NOTE_TYP_CD
		, NOTE_CTG_TYP_CD                                    as                                    NOTE_CTG_TYP_CD
		, NOTE_SBJ_TEXT                                      as                                      NOTE_SBJ_TEXT
		, NOTE_CALL_TYP_CD                                   as                                   NOTE_CALL_TYP_CD
		, NOTE_CALL_FR_TYP_CD                                as                                NOTE_CALL_FR_TYP_CD
		, NOTE_CLLR_PRSN_NM                                  as                                  NOTE_CLLR_PRSN_NM
		, NOTE_HI_PRTY_IND                                   as                                   NOTE_HI_PRTY_IND
		, NOTE_NO_PRNT_IND                                   as                                   NOTE_NO_PRNT_IND
		, NOTE_PUB_IND                                       as                                       NOTE_PUB_IND
		, NOTE_CNFDTL_IND                                    as                                    NOTE_CNFDTL_IND
		, NOTE_EDI_REF_NO                                    as                                    NOTE_EDI_REF_NO
		, NOTE_TYP_TMPL_ID                                   as                                   NOTE_TYP_TMPL_ID
		, NOTE_VOID_IND                                      as                                      NOTE_VOID_IND
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_N   ), 
RENAME_NT as ( SELECT 
		  NOTE_TYP_NM                                        as                                        NOTE_TYP_NM
		, NOTE_TYP_CD                                        as                                     NT_NOTE_TYP_CD
		, NOTE_TYP_VOID_IND                                  as                                  NOTE_TYP_VOID_IND 
				FROM     LOGIC_NT   ), 
RENAME_NCT as ( SELECT 
		  NOTE_CTG_TYP_NM                                    as                                    NOTE_CTG_TYP_NM
		, NOTE_CTG_TYP_CD                                    as                                NCT_NOTE_CTG_TYP_CD
		, NOTE_CTG_TYP_VOID_IND                              as                              NOTE_CTG_TYP_VOID_IND 
				FROM     LOGIC_NCT   ), 
RENAME_NCA as ( SELECT 
		  NOTE_CALL_TYP_NM                                   as                                   NOTE_CALL_TYP_NM
		, NOTE_CALL_TYP_CD                                   as                               NCA_NOTE_CALL_TYP_CD
		, NOTE_CALL_TYP_VOID_IND                             as                             NOTE_CALL_TYP_VOID_IND 
				FROM     LOGIC_NCA   ), 
RENAME_NCF as ( SELECT 
		  NOTE_CALL_FR_TYP_NM                                as                                NOTE_CALL_FR_TYP_NM
		, NOTE_CALL_FR_TYP_CD                                as                            NCF_NOTE_CALL_FR_TYP_CD
		, NOTE_CALL_FR_TYP_VOID_IND                          as                          NOTE_CALL_FR_TYP_VOID_IND 
				FROM     LOGIC_NCF   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_N                              as ( SELECT * from    RENAME_N   ),
FILTER_NT                             as ( SELECT * from    RENAME_NT 
				WHERE NOTE_TYP_VOID_IND = 'N'  ),
FILTER_NCT                            as ( SELECT * from    RENAME_NCT 
				WHERE NOTE_CTG_TYP_VOID_IND = 'N'  ),
FILTER_NCA                            as ( SELECT * from    RENAME_NCA 
				WHERE NOTE_CALL_TYP_VOID_IND = 'N'  ),
FILTER_NCF                            as ( SELECT * from    RENAME_NCF 
				WHERE NOTE_CALL_FR_TYP_VOID_IND = 'N'  ),

---- JOIN LAYER ----

N as ( SELECT * 
				FROM  FILTER_N
				LEFT JOIN FILTER_NT ON  FILTER_N.NOTE_TYP_CD =  FILTER_NT.NT_NOTE_TYP_CD 
								LEFT JOIN FILTER_NCT ON  FILTER_N.NOTE_CTG_TYP_CD =  FILTER_NCT.NCT_NOTE_CTG_TYP_CD 
								LEFT JOIN FILTER_NCA ON  FILTER_N.NOTE_CALL_TYP_CD =  FILTER_NCA.NCA_NOTE_CALL_TYP_CD 
								LEFT JOIN FILTER_NCF ON  FILTER_N.NOTE_CALL_FR_TYP_CD =  FILTER_NCF.NCF_NOTE_CALL_FR_TYP_CD  )
SELECT * 
from N