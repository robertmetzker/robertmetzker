---- SRC LAYER ----
WITH
SRC_CSH            as ( SELECT *     FROM     DEV_VIEWS.PCMP.CLAIM_ICD_STATUS_HISTORY ),
SRC_C              as ( SELECT *     FROM     DEV_VIEWS.PCMP.CLAIM ),
SRC_IR             as ( SELECT *     FROM     DEV_VIEWS.DBDWQP00.TDDICDC ),
SRC_BCIS           as ( SELECT *     FROM     DEV_VIEWS.PCMP.BWC_CLAIM_ICD_STATUS_HISTORY ),
SRC_ILT            as ( SELECT *     FROM     DEV_VIEWS.PCMP.BWC_ICD_LOCATION_TYPE ),
SRC_IT             as ( SELECT *     FROM     DEV_VIEWS.PCMP.BWC_ICD_SITE_TYPE ),
SRC_IST            as ( SELECT *     FROM     DEV_VIEWS.PCMP.ICD_STATUS_TYPE ),
SRC_IA             as ( SELECT *     FROM     DEV_VIEWS.PCMP.ICD_ADDTNL_INFO ),
SRC_ICD            as ( SELECT *     FROM     DEV_VIEWS.PCMP.ICD ),
SRC_OI             as ( SELECT *     FROM     DEV_VIEWS.DBDWQP00.TDDOIFM ),
SRC_OB             as ( SELECT *     FROM     DEV_VIEWS.DBDWQP00.TDDOBLI ),
SRC_ICD_CRNT       as ( SELECT *     FROM     DEV_VIEWS.PCMP.ICD ),
//SRC_CSH            as ( SELECT *     FROM     CLAIM_ICD_STATUS_HISTORY) ,
//SRC_C              as ( SELECT *     FROM     CLAIM) ,
//SRC_IR             as ( SELECT *     FROM     TDDICDC) ,
//SRC_BCIS           as ( SELECT *     FROM     BWC_CLAIM_ICD_STATUS_HISTORY) ,
//SRC_ILT            as ( SELECT *     FROM     BWC_ICD_LOCATION_TYPE) ,
//SRC_IT             as ( SELECT *     FROM     BWC_ICD_SITE_TYPE) ,
//SRC_IST            as ( SELECT *     FROM     ICD_STATUS_TYPE) ,
//SRC_IA             as ( SELECT *     FROM     ICD_ADDTNL_INFO) ,
//SRC_ICD            as ( SELECT *     FROM     ICD) ,
//SRC_OI             as ( SELECT *     FROM     TDDOIFM) ,
//SRC_OB             as ( SELECT *     FROM     TDDOBLI) ,
//SRC_ICD_CRNT       as ( SELECT *     FROM     ICD) ,

---- LOGIC LAYER ----



LOGIC_CSH as ( SELECT 
		  TRIM( HIST_ID )                                    as                                            HIST_ID 
		, TRIM( AGRE_ID )                                    as                                            AGRE_ID 
		, TRIM( CLM_ICD_STS_ID )                             as                                     CLM_ICD_STS_ID 
		, upper( CLM_ICD_STS_PRI_IND )                       as                                CLM_ICD_STS_PRI_IND 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    as                                     ICD_STS_TYP_CD 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, TRIM( ICD_ID )                                     as                                             ICD_ID 
		FROM SRC_CSH
            ),

LOGIC_C as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
        , AGRE_ID                                            as                                            AGRE_ID
		, upper( CLM_REL_SNPSHT_IND )                        as                                 CLM_REL_SNPSHT_IND 
		FROM SRC_C
            ),

LOGIC_IR as ( SELECT 
		  upper( TRIM( ICD_CODE  ) )                         as                                           ICD_CODE 
		, substr(ICDV_CODE,position('-', ICDV_CODE, 1)+1)    as                                      ICD_VRSN_NMBR 
		, upper( TRIM( PSYCH_ICD_FLAG ) )                    as                                     PSYCH_ICD_FLAG 
		, upper( TRIM( ICD_PSC_FLAG ) )                      as                                       ICD_PSC_FLAG 
		, upper( TRIM( ICDV_CODE ) )                         as                                          ICDV_CODE 
		, upper( TRIM( ICD_DESC ) )                          as                                           ICD_DESC 
		FROM SRC_IR
            ),

LOGIC_BCIS as ( SELECT 
		  upper( TRIM( CLM_ICD_DESC ) )                      as                                       CLM_ICD_DESC 
		, upper( TRIM( ICD_LOC_TYP_CD ) )                    as                                     ICD_LOC_TYP_CD 
		, upper( TRIM( ICD_SITE_TYP_CD ) )                   as                                    ICD_SITE_TYP_CD 
		, CASE WHEN startswith(upper(ICD_LOC_TYP_CD), 'C') THEN NULL
              ELSE LEFT(upper(ICD_LOC_TYP_CD),1) END         as                                    ICD_LOC_TYP_IND 
		, HIST_ID                                            as                                            HIST_ID 
		FROM SRC_BCIS
            ),

LOGIC_ILT as ( SELECT 
		  upper( TRIM( ICD_LOC_TYP_NM ) )                    as                                     ICD_LOC_TYP_NM 
		, upper( TRIM( ICD_LOC_TYP_CD ) )                    as                                     ICD_LOC_TYP_CD 
		FROM SRC_ILT
            ),

LOGIC_IT as ( SELECT 
		  upper( TRIM( ICD_SITE_TYP_NM ) )                   as                                    ICD_SITE_TYP_NM 
		, upper( TRIM( ICD_SITE_TYP_CD ) )                   as                                    ICD_SITE_TYP_CD 
		FROM SRC_IT
            ),

LOGIC_IST as ( SELECT 
		  upper( TRIM( ICD_STS_TYP_NM ) )                    as                                     ICD_STS_TYP_NM 
		, upper( TRIM( ICD_STS_TYP_CD ) )                    as                                     ICD_STS_TYP_CD 
		FROM SRC_IST
            ),

LOGIC_IA as ( SELECT 
		  upper( TRIM( ICD_CTRPH_IND ))                      as                                      ICD_CTRPH_IND 
		, upper( TRIM( ICD_CD ) )                            as                                             ICD_CD 
		FROM SRC_IA
            ),

LOGIC_ICD as ( SELECT 
		  ICD_ID                                             as                                             ICD_ID 
		, upper( TRIM( ICD_VER_CD ) )                        as                                         ICD_VER_CD 
		, upper( TRIM( ICD_CD ) )                            as                                             ICD_CD 
		FROM SRC_ICD
            ),

LOGIC_OI as ( SELECT 
		  upper( TRIM( ICDC_PRMRY_CODE ) )                   as                                    ICDC_PRMRY_CODE 
		, upper( TRIM( OIFM_CRNT_MAX_FLAG ) )                as                                 OIFM_CRNT_MAX_FLAG 
		, upper( TRIM( ICDC_MPD_CODE ) )                     as                                      ICDC_MPD_CODE 
		FROM SRC_OI
            ),

LOGIC_OB as ( SELECT DISTINCT
		  upper( TRIM( ICDC_CODE ) )                         as                                          ICDC_CODE 
		, cast( OBLI_VRSN_END_DATE as DATE )                 as                                 OBLI_VRSN_END_DATE 
		, upper( TRIM( OBLC_BDY_LCTN_CODE ) )                as                                 OBLC_BDY_LCTN_CODE 
		, upper( TRIM( OBLC_BDY_LCTN_TEXT ) )                as                                 OBLC_BDY_LCTN_TEXT 
		FROM SRC_OB
            ),

LOGIC_ICD_CRNT as ( SELECT 
		  upper( TRIM( ICD_CD ) )                            as                                             ICD_CD 
		, upper( TRIM( ICD_VER_CD ) )                        as                                         ICD_VER_CD 
		, upper( VOID_IND )                                  as                                           VOID_IND 
		, cast( ICD_EFF_DT as DATE )                         as                                         ICD_EFF_DT 
		, upper( TRIM( ICD_SHR_DESC ) )                      as                                       ICD_SHR_DESC 
		FROM SRC_ICD_CRNT
            )

---- RENAME LAYER ----
,

RENAME_CSH        as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, AGRE_ID::numeric(31,0)                             as                                        CSH_AGRE_ID
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM
		, HIST_END_DTM                                       as                                       HIST_END_DTM
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, ICD_ID                                             as                                             ICD_ID 
				FROM     LOGIC_CSH   ), 
RENAME_C          as ( SELECT 
		  CLM_NO                                             as                                             CLM_NO
        , AGRE_ID                                            as                                            AGRE_ID
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
				FROM     LOGIC_C   ), 
RENAME_IR         as ( SELECT 
		  ICD_CODE                                           as                                           ICD_CODE
		, ICDV_CODE                                          as                                          ICDV_CODE
		, ICD_VRSN_NMBR                                      as                                      ICD_VRSN_NMBR
		, ICD_DESC                                           as                                           ICD_DESC
		, PSYCH_ICD_FLAG                                     as                                     PSYCH_ICD_FLAG
		, ICD_PSC_FLAG                                       as                                       ICD_PSC_FLAG
				FROM     LOGIC_IR   ), 
RENAME_BCIS       as ( SELECT 
		  CLM_ICD_DESC                                       as                                       CLM_ICD_DESC
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
        , ICD_LOC_TYP_IND                                    as                                    ICD_LOC_TYP_IND
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, HIST_ID                                            as                                       BCIS_HIST_ID
				FROM     LOGIC_BCIS   ), 
RENAME_ILT        as ( SELECT 
		  ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_LOC_TYP_CD                                     as                                 ILT_ICD_LOC_TYP_CD 
				FROM     LOGIC_ILT   ), 
RENAME_IT         as ( SELECT 
		  ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, ICD_SITE_TYP_CD                                    as                                 IT_ICD_SITE_TYP_CD 
				FROM     LOGIC_IT   ), 
RENAME_IST        as ( SELECT 
		  ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, ICD_STS_TYP_CD                                     as                                 IST_ICD_STS_TYP_CD 
				FROM     LOGIC_IST   ), 
RENAME_IA         as ( SELECT 
		  ICD_CTRPH_IND                                      as                                      ICD_CTRPH_IND
		, ICD_CD                                             as                                          IA_ICD_CD 
				FROM     LOGIC_IA   ), 
RENAME_ICD        as ( SELECT 
		  ICD_ID                                             as                                         ICD_ICD_ID
		, ICD_VER_CD                                         as                                     ICD_ICD_VER_CD
		, ICD_CD                                             as                                       ICD_ICD_CODE 
				FROM     LOGIC_ICD   ), 
RENAME_OI         as ( SELECT 
		  ICDC_PRMRY_CODE                                    as                                 OI_ICDC_PRMRY_CODE
		, OIFM_CRNT_MAX_FLAG                                 as                                 OIFM_CRNT_MAX_FLAG
		, ICDC_MPD_CODE                                      as                                      ICDC_MPD_CODE 
				FROM     LOGIC_OI   ), 
RENAME_OB         as ( SELECT 
		  ICDC_CODE                                          as                                       OB_ICDC_CODE
		, OBLI_VRSN_END_DATE                                 as                                 OBLI_VRSN_END_DATE
		, OBLC_BDY_LCTN_CODE                                 as                                 OBLC_BDY_LCTN_CODE
		, OBLC_BDY_LCTN_TEXT                                 as                                 OBLC_BDY_LCTN_TEXT 
				FROM     LOGIC_OB   ), 
RENAME_ICD_CRNT   as ( SELECT 
		  ICD_CD                                             as                                    ICD_CRNT_ICD_CD
		, ICD_VER_CD                                         as                                ICD_CRNT_ICD_VER_CD
		, VOID_IND                                           as                                  ICD_CRNT_VOID_IND
		, ICD_EFF_DT                                         as                                ICD_CRNT_ICD_EFF_DT
		, ICD_SHR_DESC                                       as                                       ICD_SHR_DESC 
				FROM     LOGIC_ICD_CRNT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * FROM    RENAME_C 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_CSH                            as ( SELECT * FROM    RENAME_CSH   ),
FILTER_BCIS                           as ( SELECT * FROM    RENAME_BCIS   ),
FILTER_ICD                            as ( SELECT * FROM    RENAME_ICD   ),
FILTER_ILT                            as ( SELECT * FROM    RENAME_ILT   ),
FILTER_IST                            as ( SELECT * FROM    RENAME_IST   ),
FILTER_IT                             as ( SELECT * FROM    RENAME_IT   ),
FILTER_IR                             as ( SELECT * FROM    RENAME_IR   ),
FILTER_ICD_CRNT                       as ( SELECT * FROM    RENAME_ICD_CRNT 
                                            WHERE ICD_CRNT_VOID_IND='N'
                                             QUALIFY(ICD_CRNT_ICD_EFF_DT = MAX(ICD_CRNT_ICD_EFF_DT) OVER (PARTITION BY ICD_CRNT_ICD_CD,ICD_CRNT_ICD_VER_CD  ORDER BY ICD_CRNT_ICD_EFF_DT DESC  )) ),
FILTER_IA                             as ( SELECT * FROM    RENAME_IA   ),
FILTER_OI                             as ( SELECT * FROM    RENAME_OI 
                                            WHERE OIFM_CRNT_MAX_FLAG = 'Y'  ),
FILTER_OB                             as ( SELECT * FROM    RENAME_OB 
                                            WHERE OBLI_VRSN_END_DATE > CURRENT_DATE  ),


---- JOIN LAYER ----

IR as ( SELECT * , NVL(ICD_VRSN_NMBR, ICD_CRNT_ICD_VER_CD) AS ICD_VER_NO
				FROM  FILTER_IR
                  FULL OUTER JOIN FILTER_ICD_CRNT ON  FILTER_IR.ICD_CODE =  FILTER_ICD_CRNT.ICD_CRNT_ICD_CD AND FILTER_IR.ICD_VRSN_NMBR::INTEGER = FILTER_ICD_CRNT.ICD_CRNT_ICD_VER_CD::INTEGER
						LEFT JOIN FILTER_IA ON  FILTER_ICD_CRNT.ICD_CRNT_ICD_CD =  FILTER_IA.IA_ICD_CD ),

C as ( SELECT * 
				FROM  IR
                INNER JOIN FILTER_ICD ON IR.ICD_CODE = FILTER_ICD.ICD_ICD_CODE AND IR.ICD_VER_NO::INTEGER = FILTER_ICD.ICD_ICD_VER_CD::INTEGER
				LEFT JOIN FILTER_OI ON  IR.ICD_CODE =  FILTER_OI.OI_ICDC_PRMRY_CODE 
				LEFT JOIN FILTER_OB ON  IR.ICD_CODE =  FILTER_OB.OB_ICDC_CODE  
				INNER JOIN FILTER_CSH ON FILTER_CSH.ICD_ID =  FILTER_ICD.ICD_ICD_ID
                INNER JOIN FILTER_BCIS ON  FILTER_CSH.HIST_ID = FILTER_BCIS.BCIS_HIST_ID 
                INNER JOIN FILTER_C ON  FILTER_CSH.CSH_AGRE_ID = FILTER_C.AGRE_ID 
				LEFT JOIN FILTER_ILT ON  FILTER_BCIS.ICD_LOC_TYP_CD =  FILTER_ILT.ILT_ICD_LOC_TYP_CD 
				LEFT JOIN FILTER_IT ON  FILTER_BCIS.ICD_SITE_TYP_CD =  FILTER_IT.IT_ICD_SITE_TYP_CD 
                LEFT JOIN FILTER_IST ON  FILTER_CSH.ICD_STS_TYP_CD =  FILTER_IST.IST_ICD_STS_TYP_CD  ) 

SELECT 
		  HIST_ID::numeric(31,0) AS HIST_ID
		, CLM_NO
		, CSH_AGRE_ID AS AGRE_ID
		, CLM_ICD_STS_ID::numeric(31,0) AS CLM_ICD_STS_ID
		, CLM_ICD_STS_PRI_IND::char(1) AS CLM_ICD_STS_PRI_IND
	    , upper( NVL(ICD_CODE,ICD_CRNT_ICD_CD) )  AS  ICD_CODE 
		, upper( NVL(ICDC_MPD_CODE,ICD_CODE) )  AS   ICDC_MPD9_CODE 
		, CASE WHEN ICDV_CODE IS NULL THEN 'ICD-' || ICD_CRNT_ICD_VER_CD ELSE ICDV_CODE 
		 END  AS ICDV_CODE  
		, ICD_VER_NO::Integer AS ICD_VER_NO
		, 'ICD-9'  AS  ICDV_MPD9_VRSN_CODE 
		, upper( NVL(ICD_DESC,ICD_SHR_DESC) )  AS  ICD_DESC 
		, CLM_ICD_DESC
		, ICD_LOC_TYP_IND
		, ICD_LOC_TYP_CD
		, NULLIF(TRIM(ICD_LOC_TYP_NM),'') AS ICD_LOC_TYP_NM
		, ICD_SITE_TYP_CD
		, NULLIF(TRIM(ICD_SITE_TYP_NM),'') AS ICD_SITE_TYP_NM
		, ICD_STS_TYP_CD
		, ICD_STS_TYP_NM
		, HIST_EFF_DTM
		, HIST_END_DTM
		, ICD_CTRPH_IND::Char(1) AS ICD_CTRPH_IND 
		, PSYCH_ICD_FLAG::Char(1) AS PSYCH_ICD_FLAG
		, ICD_PSC_FLAG::Char(1) AS ICD_PSC_FLAG
		, 'CS' AS SOURCE
	    , NVL2(HIST_END_DTM, 'N','Y') AS CRNT_CLAIM_INJURY_FLAG
		, AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM
		, VOID_IND::Char(1) AS VOID_IND
		,  CASE WHEN ICD_VER_NO = 10 AND OBLC_BDY_LCTN_CODE <> ICD_LOC_TYP_IND  THEN ICD_LOC_TYP_IND
                     WHEN ICD_VER_NO = 10 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_IND 
                     WHEN ICD_VER_NO = 9 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_IND ELSE OBLC_BDY_LCTN_CODE
					 END AS OBLC_BDY_LCTN_CODE
		, NULLIF(TRIM(CASE WHEN ICD_VER_NO = 10 AND OBLC_BDY_LCTN_CODE <> ICD_LOC_TYP_IND  THEN ICD_LOC_TYP_NM  
                     WHEN ICD_VER_NO = 10 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_NM 
                     WHEN ICD_VER_NO = 9 AND  OBLC_BDY_LCTN_CODE IS NULL THEN ICD_LOC_TYP_NM ELSE OBLC_BDY_LCTN_TEXT 
					 END),'') AS OBLC_BDY_LCTN_TEXT 
FROM C