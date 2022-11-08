---- SRC LAYER ----
WITH
SRC_PREA as ( SELECT *     from     STAGING.STG_TMPPREA ),
SRC_PRDT as ( SELECT *     from     STAGING.STG_TMPPRDT ),
SRC_PRED as ( SELECT *     from     STAGING.STG_TMPPRED ),
SRC_PT as ( SELECT *     from     STAGING.STG_PROVIDER_TYPE ),
SRC_SPEC as ( SELECT *     from     STAGING.STG_PROVIDER_CREDENTIALS ),
SRC_PC as ( SELECT *     from     STAGING.STG_PROVIDER_CONTACT ),
SRC_PA as ( SELECT *     from     STAGING.STG_PROVIDER_ADDRESS ),
SRC_MA as ( SELECT *     from     STAGING.STG_PROVIDER_ADDRESS ),
SRC_NPI as ( SELECT *     from     STAGING.STG_PROVIDER_NPI ),
SRC_MDCR as ( SELECT *     from     STAGING.STG_PROVIDER_CREDENTIALS ),
SRC_OPSI as ( SELECT *     from     STAGING.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION ),
SRC_ES as ( SELECT *     from     STAGING.STG_PROVIDER_ENROLLMENT_STATUS ),
SRC_CS as ( SELECT *     from     STAGING.STG_PROVIDER_CERTIFICATION_STATUS ),
SRC_PRG as ( SELECT *     from     STAGING.STG_PROVIDER_PROGRAM_FOCUS ),
SRC_PDS as ( SELECT *     from     STAGING.STG_PROVIDER_DEP_SERVICE ),
SRC_MEM as ( SELECT *     from     STAGING.STG_DEP_MEMBERS ),
SRC_SOS as ( SELECT *     from     STAGING.STG_DEP_SIGN_OFF_SHEET ),
//SRC_PREA as ( SELECT *     from     STG_TMPPREA) ,
//SRC_PRDT as ( SELECT *     from     STG_TMPPRDT) ,
//SRC_PRED as ( SELECT *     from     STG_TMPPRED) ,
//SRC_PT as ( SELECT *     from     STG_PROVIDER_TYPE) ,
//SRC_SPEC as ( SELECT *     from     STG_PROVIDER_CREDENTIALS) ,
//SRC_PC as ( SELECT *     from     STG_PROVIDER_CONTACT) ,
//SRC_PA as ( SELECT *     from     STG_PROVIDER_ADDRESS) ,
//SRC_MA as ( SELECT *     from     STG_PROVIDER_ADDRESS) ,
//SRC_NPI as ( SELECT *     from     STG_PROVIDER_NPI) ,
//SRC_MDCR as ( SELECT *     from     STG_PROVIDER_CREDENTIALS) ,
//SRC_OPSI as ( SELECT *     from     STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION) ,
//SRC_ES as ( SELECT *     from     STG_PROVIDER_ENROLLMENT_STATUS) ,
//SRC_CS as ( SELECT *     from     STG_PROVIDER_CERTIFICATION_STATUS) ,
//SRC_PRG as ( SELECT *     from     STG_PROVIDER_PROGRAM_FOCUS) ,
//SRC_PDS as ( SELECT *     from     STG_PROVIDER_DEP_SERVICE) ,
//SRC_MEM as ( SELECT *     from     STG_DEP_MEMBERS) ,
//SRC_SOS as ( SELECT *     from     STG_DEP_SIGN_OFF_SHEET) ,


---- LOGIC LAYER ----

LOGIC_PREA as ( SELECT 
		  PRVDR_BASE_NMBR::varchar||lpad(PRVDR_SFX_NMBR::varchar,4,'0') AS 							  PEACH_NUMBER 
		, left(PRVDR_BASE_NMBR::varchar,3)||'-'||right(PRVDR_BASE_NMBR::varchar,4)||'-'||lpad(PRVDR_SFX_NMBR::varchar,4,'0') AS PEACH_FORMATTED_NUMBER
		, cast( PRVDR_BASE_NMBR as TEXT )                    AS                                    PRVDR_BASE_NMBR 
		, PRVDR_SFX_NMBR                                     AS                                     PRVDR_SFX_NMBR 
		, PRVDR_ID                                           AS                                           PRVDR_ID  
		, REPLACE(DCTVT_DTTM, '10000-01-01 00:00:00', '2999-12-31 00:00:00')::TIMESTAMP   AS DCTVT_DTTM  
		from SRC_PREA
            ),
LOGIC_PRDT as ( SELECT 
		  upper( TRIM( PRVDR_LEGAL_NAME ) )                  AS                                   PRVDR_LEGAL_NAME 
		, upper( TRIM( PRSN_IND ) )                          AS                                           PRSN_IND 
		, upper( TRIM( GNDR_CODE ) )                         AS                                          GNDR_CODE 
		, upper( TRIM( BIRTH_DATE ) )                        AS                                         BIRTH_DATE 
		, upper( TRIM( DEATH_DATE ) )                        AS                                         DEATH_DATE 
		, upper( TRIM( BSNS_TYPE_CODE ) )                    AS                                     BSNS_TYPE_CODE 
		, PRDT_CRT_DTTM                                      AS                                      PRDT_CRT_DTTM 
		, upper( TRIM( CRT_USER_CODE ) )                     AS                                      CRT_USER_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( DCTVT_USER_CODE ) )                   AS                                    DCTVT_USER_CODE 
		, upper( TRIM( PRCT_TYPE_CODE ) )                    AS                                     PRCT_TYPE_CODE 
		, upper( TRIM( PRCT_TYPE_NAME ) )                    AS                                     PRCT_TYPE_NAME 
		, PRVDR_ID                                           AS                                           PRVDR_ID 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		from SRC_PRDT
            ),
LOGIC_PRED as ( SELECT 
		  upper( TRIM( DBA_NAME ) )                          AS                                           DBA_NAME 
		, TRIM( NEW_PATIENT_CODE )                           AS                                   NEW_PATIENT_CODE 
		, TRIM( NEW_PATIENT_DESC )                           AS                                   NEW_PATIENT_DESC 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, PRED_CRT_DTTM                                      AS                                      PRED_CRT_DTTM 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_PRED
            ),
LOGIC_PT as ( SELECT 
		  upper( TRIM( PRVDR_TYPE_CODE ) )                   AS                                    PRVDR_TYPE_CODE 
		, upper( TRIM( PRVDR_TYPE_NAME ) )                   AS                                    PRVDR_TYPE_NAME 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_PT
            ),
LOGIC_SPEC as ( SELECT 
		  UPPER( MDCL_SPC_TYPE_NAME)                         as                                 MDCL_SPC_TYPE_NAME
		, TRIM( PRVDR_BASE_NMBR )                            as                                    PRVDR_BASE_NMBR 
		, TRIM( PRVDR_SFX_NMBR )                             as                                     PRVDR_SFX_NMBR 
		, PCRD_DCTVT_DTTM                                    as                                    PCRD_DCTVT_DTTM 
		, CRDD_DCTVT_DTTM                                    as                                    CRDD_DCTVT_DTTM 
		, EXPRT_DATE                                         as                                         EXPRT_DATE 
		, upper( TRIM( MDCL_SPC_LVL_CODE ) )                 as                                  MDCL_SPC_LVL_CODE 
		from SRC_SPEC
            ),
LOGIC_PC as ( SELECT 
		  upper( TRIM( PHONE_NUMBER ) )                      AS                                       PHONE_NUMBER 
		, upper( TRIM( FAX_NUMBER ) )                        AS                                         FAX_NUMBER 
		, upper( TRIM( EMAIL_ADRS ) )                        AS                                         EMAIL_ADRS 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, upper( TRIM( CNTCT_PRP_TYP_CODE ) )               AS                                CNTCT_PRP_TYPE_CODE 
		, TMPCNDT_DCTVT_DTTM                                 AS                                 TMPCNDT_DCTVT_DTTM 
		, ADRS_TYPE_LCT_NMBR                                 AS                                 ADRS_TYPE_LCT_NMBR 
		, PECN_CRT_DTTM                                      AS                                      PECN_CRT_DTTM 
		from SRC_PC
            ),
LOGIC_PA as ( SELECT 
		  upper( TRIM( LINE_1_ADRS ) )                       AS                                        LINE_1_ADRS 
		, upper( TRIM( LINE_2_ADRS ) )                       AS                                        LINE_2_ADRS 
		, NULLIF(upper( TRIM( CITY_NAME ) ),'')                         AS                               CITY_NAME 
		, NULLIF(upper( TRIM( STATE_CODE ) ) ,'')                       AS                              STATE_CODE 
		, NULLIF(upper( TRIM( ZIP_CODE_NMBR ) ) ,'')                    AS                           ZIP_CODE_NMBR 
		, NULLIF(upper( TRIM( ZIP_CODE_PLS4_NMBR ) ) ,'')               AS                      ZIP_CODE_PLS4_NMBR 
		, concat(ZIP_CODE_NMBR|| ZIP_CODE_PLS4_NMBR )        AS                      PRACTICE_FULL_ZIP_CODE_NUMBER 
		, upper( TRIM( CNTY_NAME ) )                         AS                                          CNTY_NAME 
        , CASE WHEN STATE_CODE = 'OH' THEN 'IN STATE'
                WHEN STATE_CODE IN ('PA','WV','KY','IN','MI') THEN 'BORDERING STATE'
                    ELSE 'OUT OF STATE' END                              AS               OUT_OF_STATE_STATUS_DESC
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, upper( TRIM( ADRS_TYPE_CODE ) )                    AS                                     ADRS_TYPE_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, ADRS_DCTVT_DTTM                                    AS                                    ADRS_DCTVT_DTTM 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, TRIM( PRMRY_LCTN_IND )                             AS                                     PRMRY_LCTN_IND 
		, CNEA_CRT_DTTM                                      AS                                      CNEA_CRT_DTTM 
		from SRC_PA
            ),
LOGIC_MA as ( SELECT 
		  upper( TRIM( LINE_1_ADRS ) )                       AS                                        LINE_1_ADRS 
		, upper( TRIM( LINE_2_ADRS ) )                       AS                                        LINE_2_ADRS 
		, nullif(upper( TRIM( CITY_NAME ) ), '')             AS                                          CITY_NAME 
		, nullif(upper( TRIM( STATE_CODE ) ), '')            AS                                         STATE_CODE 
		, nullif(upper( TRIM( ZIP_CODE_NMBR ) ), '')         AS                                      ZIP_CODE_NMBR 
		, nullif(upper( TRIM( ZIP_CODE_PLS4_NMBR ) ), '')    AS                                 ZIP_CODE_PLS4_NMBR 
		, concat(ZIP_CODE_NMBR,ZIP_CODE_PLS4_NMBR)           AS                CORRESPONDENCE_FULL_ZIP_CODE_NUMBER
		, nullif(upper( TRIM( CNTY_NAME ) ), '')             AS                                          CNTY_NAME  
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, upper( TRIM( ADRS_TYPE_CODE ) )                    AS                                     ADRS_TYPE_CODE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, ADRS_DCTVT_DTTM                                    AS                                    ADRS_DCTVT_DTTM 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE  
		, CNEA_CRT_DTTM                                      AS                                      CNEA_CRT_DTTM 
		from SRC_MA
            ),
LOGIC_NPI as ( SELECT 
		  upper( TRIM( NPI_NMBR ) )                          AS                                           NPI_NMBR 
		, upper( TRIM( NPI_CNFRM_IND ) )                     AS                                      NPI_CNFRM_IND 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		from SRC_NPI
            ),
LOGIC_MDCR as ( SELECT 
		  upper( TRIM( CRDN_ID_NMBR ) )                      AS                                       CRDN_ID_NMBR 
		, upper( TRIM( STATE_CODE ) )                        AS                                         STATE_CODE 
		, EFCTV_DATE                                         AS                                         EFCTV_DATE 
		, TRIM( PRVDR_BASE_NMBR )                            AS                                    PRVDR_BASE_NMBR 
		, TRIM( PRVDR_SFX_NMBR )                             AS                                     PRVDR_SFX_NMBR 
		, PCRD_DCTVT_DTTM                                    AS                                    PCRD_DCTVT_DTTM 
		, CRDD_DCTVT_DTTM                                    AS                                    CRDD_DCTVT_DTTM 
		, EXPRT_DATE                                         AS                                         EXPRT_DATE 
		, upper( TRIM( CRDN_TYPE_CODE ) )                    AS                                     CRDN_TYPE_CODE 
		from SRC_MDCR
            ),

LOGIC_OPSI as ( SELECT 
		  upper( TRIM( MEDICARE_PROVIDER_TYPE_CODE ) )       AS                        MEDICARE_PROVIDER_TYPE_CODE 
		, upper( TRIM( MEDICARE_PROVIDER_TYPE_DESC ) )       AS                        MEDICARE_PROVIDER_TYPE_DESC 
		, upper( TRIM( CRITICAL_ACCESS_IND ) )               AS                                CRITICAL_ACCESS_IND 
		, upper( TRIM( OPPS_QUALITY_IND ) )                  AS                                   OPPS_QUALITY_IND 
		, upper( TRIM( PROVIDER_CBSA_CODE ) )                AS                                 PROVIDER_CBSA_CODE 
		, TRIM( PRO_NUM )                                    AS                                            PRO_NUM 
		, EXPIRATION_DATE                                    AS                                    EXPIRATION_DATE 
		from SRC_OPSI
            ),
LOGIC_ES as ( SELECT 
		  upper( TRIM( ENRL_STS_NAME ) )                     AS                                      ENRL_STS_NAME 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, upper( TRIM( ENRL_STS_TYPE_CODE ) )                AS                                 ENRL_STS_TYPE_CODE 
		from SRC_ES
            ),
LOGIC_CS as ( SELECT 
		  upper( TRIM( CRTF_STS_TYPE_NAME ) )                AS                                 CRTF_STS_TYPE_NAME 
		, upper( TRIM( PRVDR_BASE_NMBR ) )                   AS                                    PRVDR_BASE_NMBR 
		, upper( TRIM( PRVDR_SFX_NMBR ) )                    AS                                     PRVDR_SFX_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		, upper( TRIM( CRTF_STS_TYPE_CODE ) )                AS                                 CRTF_STS_TYPE_CODE 
		from SRC_CS
            ),
LOGIC_PRG as ( SELECT 
		  TRIM( PROVIDER_PROGRAM_CODE )                      AS                              PROVIDER_PROGRAM_CODE 
		, TRIM( PROVIDER_PROGRAM_DESC )                      AS                              PROVIDER_PROGRAM_DESC 
		, TRIM( PROVIDER_PROGRAM_FOCUS_TEXT )                AS                        PROVIDER_PROGRAM_FOCUS_TEXT 
		, TRIM( PROGRAM_PARTICIPATION_PERIOD )               AS                       PROGRAM_PARTICIPATION_PERIOD 
		, TRIM( PRVDR_BASE_NMBR )                            AS                                    PRVDR_BASE_NMBR 
		, TRIM( PRVDR_SFX_NMBR )                             AS                                     PRVDR_SFX_NMBR 
		, DCTVT_DTTM                                         AS                                         DCTVT_DTTM 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE 
		from SRC_PRG
            ),
LOGIC_PDS as ( SELECT 
		  upper( TRIM( DEP_SRVC_CODE ) )                     AS                                      DEP_SRVC_CODE 
		, upper( TRIM( DEP_SRVC_NAME ) )                     AS                                      DEP_SRVC_NAME 
		, TRIM( PRVDR_BASE_NMBR )                            AS                                    PRVDR_BASE_NMBR 
		, TRIM( PRVDR_SFX_NMBR )                             AS                                     PRVDR_SFX_NMBR 
		, ENDNG_DATE                                         AS                                         ENDNG_DATE
		, REPLACE(DCTVT_DTTM, '10000-01-01 00:00:00', '2999-12-31 00:00:00')::TIMESTAMP  AS             DCTVT_DTTM 
		from SRC_PDS
            ),
LOGIC_MEM as ( SELECT 
		  CASE INACTIVE WHEN '0' THEN 'Y'
              WHEN '1' THEN 'N' ELSE NULL END                AS                                           INACTIVE  
		, upper( TRIM( ADMINISTRATIVE_AGENT ) )              AS                               ADMINISTRATIVE_AGENT 
		, TRIM( PEACH_BASE_NUMBER )                          AS                                  PEACH_BASE_NUMBER 
		, TRIM( PEACH_SUFFIX_NUMBER )                        AS                                PEACH_SUFFIX_NUMBER 
		from SRC_MEM
            ),
LOGIC_SOS as ( SELECT 
		  CASE TRIM( _90_DAY_EXAMS ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 				AS 		_90_DAY_EXAMS 
		, CASE TRIM( DISPUTE_IMES ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 				AS 		DISPUTE_IMES 
		, CASE TRIM( DISPUTE_FILE_REVIEW ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 		AS 		DISPUTE_FILE_REVIEW 
		, CASE TRIM( B_READER ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 					AS 		B_READER 
		, CASE TRIM( C_92_EXAMS ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 				AS 		C_92_EXAMS 
		, CASE TRIM( C_92_FILE_REVIEW ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 			AS 		C_92_FILE_REVIEW 
		, CASE TRIM( DUR ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 						AS 		DUR 
		, CASE TRIM( IMES ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 						AS 		IMES 
		, CASE TRIM( MEDICAL_FILE_REVIEW ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 		AS 		MEDICAL_FILE_REVIEW 
		, CASE TRIM( PRIORAUTH ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 					AS 		PRIORAUTH 
		, CASE TRIM( DMIME ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 						AS 		DMIME 
		, CASE TRIM( ADDITIONAL_LANGUAGE ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 		AS 		ADDITIONAL_LANGUAGE 
		, CASE TRIM( ON_LINE_FILE_REVIEWS ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 		AS 		ON_LINE_FILE_REVIEWS 
		, CASE TRIM( PULMONARY_EXAM ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 			AS 		PULMONARY_EXAM 
		, CASE TRIM( PULMONARY_REVIEW ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 			AS 		PULMONARY_REVIEW 
		, CASE TRIM( ASBESTOSIS_EXAM ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 			AS 		ASBESTOSIS_EXAM 
		, CASE TRIM( MCO_MEDICAL_DIRECTOR ) WHEN '1' THEN 'Y' WHEN '0' THEN 'N' ELSE NULL END 		AS 		MCO_MEDICAL_DIRECTOR 
		, TRIM( PEACH_BASE_NUMBER )                          AS                                  PEACH_BASE_NUMBER 
		, TRIM( PEACH_SUFFIX_NUMBER )                        AS                                PEACH_SUFFIX_NUMBER 
		from SRC_SOS
            )

---- RENAME LAYER ----
,

RENAME_PREA as ( SELECT 
		  PEACH_NUMBER                                    	 as                                       PEACH_NUMBER
		, PEACH_FORMATTED_NUMBER                             as                             PEACH_FORMATTED_NUMBER
		, PRVDR_BASE_NMBR                                    as                                  PEACH_BASE_NUMBER
		, PRVDR_SFX_NMBR                                     as                                PEACH_SUFFIX_NUMBER
		, PRVDR_ID                                           as                                           PRVDR_ID
		, DCTVT_DTTM                                         as                                    PREA_DCTVT_DTTM 
				FROM     LOGIC_PREA   ), 
RENAME_PRDT as ( SELECT 
		  PRVDR_LEGAL_NAME                                   as                                   PRVDR_LEGAL_NAME
		, PRSN_IND                                           as                                           PRSN_IND
		, GNDR_CODE                                          as                                          GNDR_CODE
		, BIRTH_DATE                                         as                                         BIRTH_DATE
		, DEATH_DATE                                         as                                         DEATH_DATE
		, BSNS_TYPE_CODE                                     as                                     BSNS_TYPE_CODE
		, PRDT_CRT_DTTM                                      as                                      PRDT_CRT_DTTM
		, CRT_USER_CODE                                      as                                 PRDT_CRT_USER_CODE
		, DCTVT_DTTM                                         as                                    PRDT_DCTVT_DTTM
		, DCTVT_USER_CODE                                    as                               PRDT_DCTVT_USER_CODE
		, PRCT_TYPE_CODE                                     as                                     PRCT_TYPE_CODE
		, PRCT_TYPE_NAME                                     as                                     PRCT_TYPE_NAME
		, PRVDR_ID                                           as                                      PRDT_PRVDR_ID
		, PRVDR_BASE_NMBR                                    as                               PRDT_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                PRDT_PRVDR_SFX_NMBR 
				FROM     LOGIC_PRDT   ), 
RENAME_PRED as ( SELECT 
		  DBA_NAME                                           as                                           DBA_NAME
		, NEW_PATIENT_CODE                                   as                                   NEW_PATIENT_CODE
		, NEW_PATIENT_DESC                                   as                                   NEW_PATIENT_DESC
		, PRVDR_BASE_NMBR                                    as                               PRED_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                PRED_PRVDR_SFX_NMBR
		, PRED_CRT_DTTM                                      as                                      PRED_CRT_DTTM
		, DCTVT_DTTM                                         as                                    PRED_DCTVT_DTTM 
				FROM     LOGIC_PRED   ), 
RENAME_PT as ( SELECT 
		  PRVDR_TYPE_CODE                                    as                                    PRVDR_TYPE_CODE
		, PRVDR_TYPE_NAME                                    as                                    PRVDR_TYPE_DESC
		, PRVDR_BASE_NMBR                                    as                                 PT_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                  PT_PRVDR_SFX_NMBR
		, DCTVT_DTTM                                         as                                      PT_DCTVT_DTTM 
				FROM     LOGIC_PT   ), 
RENAME_SPEC as ( SELECT 
		  MDCL_SPC_TYPE_NAME                                 as                              MDCL_SPC_TYPE_NAME
		, PRVDR_BASE_NMBR                                    as                               SPEC_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                SPEC_PRVDR_SFX_NMBR
		, PCRD_DCTVT_DTTM                                    as                               SPEC_PCRD_DCTVT_DTTM
		, CRDD_DCTVT_DTTM                                    as                               SPEC_CRDD_DCTVT_DTTM
		, EXPRT_DATE                                         as                                    SPEC_EXPRT_DATE
		, MDCL_SPC_LVL_CODE                                  as                             SPEC_MDCL_SPC_LVL_CODE 
				FROM     LOGIC_SPEC   ), 
RENAME_PC as ( SELECT 
		  PHONE_NUMBER                                       as                              PROVIDER_PHONE_NUMBER
		, FAX_NUMBER                                         as                                PROVIDER_FAX_NUMBER
		, EMAIL_ADRS                                         as                             PROVIDER_EMAIL_ADDRESS
		, PRVDR_BASE_NMBR                                    as                                 PC_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                  PC_PRVDR_SFX_NMBR
		, ENDNG_DATE                                         as                                      PC_ENDNG_DATE
		, DCTVT_DTTM                                         as                                      PC_DCTVT_DTTM
		, CNTCT_PRP_TYPE_CODE                                as                                CNTCT_PRP_TYPE_CODE
		, TMPCNDT_DCTVT_DTTM                                 as                                 TMPCNDT_DCTVT_DTTM
		, ADRS_TYPE_LCT_NMBR                                 as                                 ADRS_TYPE_LCT_NMBR
		, PECN_CRT_DTTM                                      as                                      PECN_CRT_DTTM 
				FROM     LOGIC_PC   ), 
RENAME_PA as ( SELECT 
		  LINE_1_ADRS                                        as                          PRACTICE_STREET_ADDRESS_1
		, LINE_2_ADRS                                        as                          PRACTICE_STREET_ADDRESS_2
		, CITY_NAME                                          as                                 PRACTICE_CITY_NAME
		, STATE_CODE                                         as                                PRACTICE_STATE_CODE
		, ZIP_CODE_NMBR                                      as                             PRACTICE_ZIP_CODE_NMBR
		, ZIP_CODE_PLS4_NMBR                                 as                        PRACTICE_ZIP_CODE_PLS4_NMBR
		, PRACTICE_FULL_ZIP_CODE_NUMBER                      as                      PRACTICE_FULL_ZIP_CODE_NUMBER
		, CNTY_NAME                                          as                               PRACTICE_COUNTY_NAME
		, OUT_OF_STATE_STATUS_DESC                           as                           OUT_OF_STATE_STATUS_DESC
		, PRVDR_BASE_NMBR                                    as                                 PA_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                  PA_PRVDR_SFX_NMBR
		, ADRS_TYPE_CODE                                     as                                  PA_ADRS_TYPE_CODE
		, DCTVT_DTTM                                         as                                      PA_DCTVT_DTTM
		, ADRS_DCTVT_DTTM                                    as                                 PA_ADRS_DCTVT_DTTM
		, ENDNG_DATE                                         as                                      PA_ENDNG_DATE
		, PRMRY_LCTN_IND                                     as                                  PA_PRMRY_LCTN_IND  
		, CNEA_CRT_DTTM                                    	 AS                                        PA_CRT_DTTM
				FROM     LOGIC_PA   ), 
RENAME_MA as ( SELECT 
		  LINE_1_ADRS                                        as                    CORRESPONDENCE_STREET_ADDRESS_1
		, LINE_2_ADRS                                        as                    CORRESPONDENCE_STREET_ADDRESS_2
		, CITY_NAME                                          as                           CORRESPONDENCE_CITY_NAME
		, STATE_CODE                                         as                          CORRESPONDENCE_STATE_CODE
		, ZIP_CODE_NMBR                                      as                       CORRESPONDENCE_ZIP_CODE_NMBR
		, ZIP_CODE_PLS4_NMBR                                 as                  CORRESPONDENCE_ZIP_CODE_PLS4_NMBR
		, CORRESPONDENCE_FULL_ZIP_CODE_NUMBER                as                CORRESPONDENCE_FULL_ZIP_CODE_NUMBER
		, CNTY_NAME                                          as                         CORRESPONDENCE_COUNTY_NAME
		, PRVDR_BASE_NMBR                                    as                                 MA_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                  MA_PRVDR_SFX_NMBR
		, ADRS_TYPE_CODE                                     as                                  MA_ADRS_TYPE_CODE
		, DCTVT_DTTM                                         as                                      MA_DCTVT_DTTM
		, ADRS_DCTVT_DTTM                                    as                                 MA_ADRS_DCTVT_DTTM
		, ENDNG_DATE                                         as                                      MA_ENDNG_DATE  
		, CNEA_CRT_DTTM                                    	 AS                                        MA_CRT_DTTM 
				FROM     LOGIC_MA   ), 
RENAME_NPI as ( SELECT 
		  NPI_NMBR                                           as                                           NPI_NMBR
		, NPI_CNFRM_IND                                      as                                      NPI_CNFRM_IND
		, EFCTV_DATE                                         as                                     NPI_EFCTV_DATE
		, PRVDR_BASE_NMBR                                    as                                NPI_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                 NPI_PRVDR_SFX_NMBR
		, ENDNG_DATE                                         as                                     NPI_ENDNG_DATE
		, DCTVT_DTTM                                         as                                     NPI_DCTVT_DTTM 
				FROM     LOGIC_NPI   ), 
RENAME_MDCR as ( SELECT 
		  CRDN_ID_NMBR                                       as                                    MEDICARE_NUMBER
		, STATE_CODE                                         as                         MEDICARE_NUMBER_STATE_CODE
		, EFCTV_DATE                                         as                     MEDICARE_NUMBER_EFFECTIVE_DATE
		, PRVDR_BASE_NMBR                                    as                               MDCR_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                MDCR_PRVDR_SFX_NMBR
		, PCRD_DCTVT_DTTM                                    as                               MDCR_PCRD_DCTVT_DTTM
		, CRDD_DCTVT_DTTM                                    as                               MDCR_CRDD_DCTVT_DTTM
		, EXPRT_DATE                                         as                                    MDCR_EXPRT_DATE
		, CRDN_TYPE_CODE                                     as                                MDCR_CRDN_TYPE_CODE 
				FROM     LOGIC_MDCR   ), 
RENAME_OPSI as ( SELECT 
		  MEDICARE_PROVIDER_TYPE_CODE                        as                        MEDICARE_PROVIDER_TYPE_CODE
		, MEDICARE_PROVIDER_TYPE_DESC                        as                        MEDICARE_PROVIDER_TYPE_DESC
		, CRITICAL_ACCESS_IND                                as                                CRITICAL_ACCESS_IND
		, OPPS_QUALITY_IND                                   as                                   OPPS_QUALITY_IND
		, PROVIDER_CBSA_CODE                                 as                                 PROVIDER_CBSA_CODE
		, PRO_NUM                                            as                                       OPSI_PRO_NUM
		, EXPIRATION_DATE                                    as                               OPSI_EXPIRATION_DATE 
				FROM     LOGIC_OPSI   ), 
RENAME_ES as ( SELECT 
		  ENRL_STS_NAME                                      as                     CURRENT_ENROLLMENT_STATUS_DESC
		, PRVDR_BASE_NMBR                                    as                                 ES_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                  ES_PRVDR_SFX_NMBR
		, DCTVT_DTTM                                         as                                      ES_DCTVT_DTTM
		, ENDNG_DATE                                         as                                      ES_ENDNG_DATE
		, ENRL_STS_TYPE_CODE                                 as                                 ENRL_STS_TYPE_CODE 
				FROM     LOGIC_ES   ), 
RENAME_CS as ( SELECT 
		  CRTF_STS_TYPE_NAME                                 as                  CURRENT_CERTIFICATION_STATUS_DESC
		, PRVDR_BASE_NMBR                                    as                                 CS_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                  CS_PRVDR_SFX_NMBR
		, DCTVT_DTTM                                         as                                      CS_DCTVT_DTTM
		, ENDNG_DATE                                         as                                      CS_ENDNG_DATE
		, CRTF_STS_TYPE_CODE                                 as                                 CRTF_STS_TYPE_CODE 
				FROM     LOGIC_CS   ), 
RENAME_PRG as ( SELECT 
		  PROVIDER_PROGRAM_CODE                              as                              PROVIDER_PROGRAM_CODE
		, PROVIDER_PROGRAM_DESC                              as                              PROVIDER_PROGRAM_DESC
		, PROVIDER_PROGRAM_FOCUS_TEXT                        as                        PROVIDER_PROGRAM_FOCUS_TEXT
		, PROGRAM_PARTICIPATION_PERIOD                       as                       PROGRAM_PARTICIPATION_PERIOD
		, PRVDR_BASE_NMBR                                    as                                PRG_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                 PRG_PRVDR_SFX_NMBR
		, DCTVT_DTTM                                         as                                     PRG_DCTVT_DTTM
		, ENDNG_DATE                                         as                                     PRG_ENDNG_DATE 
				FROM     LOGIC_PRG   ), 
RENAME_PDS as ( SELECT 
		  DEP_SRVC_CODE                                      as                                   DEP_SERVICE_CODE
		, DEP_SRVC_NAME                                      as                                   DEP_SERVICE_DESC
		, PRVDR_BASE_NMBR                                    as                                PDS_PRVDR_BASE_NMBR
		, PRVDR_SFX_NMBR                                     as                                 PDS_PRVDR_SFX_NMBR
		, ENDNG_DATE                                         as                                     PDS_ENDNG_DATE
		, DCTVT_DTTM                                         as                                     PDS_DCTVT_DTTM 
				FROM     LOGIC_PDS   ), 
RENAME_MEM as ( SELECT 
		  INACTIVE                                           as                                     DEP_ACTIVE_IND
		, ADMINISTRATIVE_AGENT                               as                      DEP_ADMINISTRATIVE_AGENT_NAME
		, PEACH_BASE_NUMBER                                  as                                MEM_PRVDR_BASE_NMBR
		, PEACH_SUFFIX_NUMBER                                as                                 MEM_PRVDR_SFX_NMBR 
				FROM     LOGIC_MEM   ), 
RENAME_SOS as ( SELECT 
		  _90_DAY_EXAMS                                      as                                DEP_90_DAY_EXAM_IND
		, DISPUTE_IMES                                       as                                    DEP_ADR_IME_IND
		, DISPUTE_FILE_REVIEW                                as                            DEP_ADR_FILE_REVIEW_IND
		, B_READER                                           as                                   DEP_B_READER_IND
		, C_92_EXAMS                                         as                                   DEP_C92_EXAM_IND
		, C_92_FILE_REVIEW                                   as                           DEP_C92A_FILE_REVIEW_IND
		, DUR                                                as                                 DEP_DUR_REVIEW_IND
		, IMES                                               as                                        DEP_IME_IND
		, MEDICAL_FILE_REVIEW                                as                        DEP_MEDICAL_FILE_REVIEW_IND
		, PRIORAUTH                                          as                 DEP_PRIOR_AUTHORIZATION_REVIEW_IND
		, DMIME                                              as                                     DEP_DM_IME_IND
		, ADDITIONAL_LANGUAGE                                as                        DEP_ADDITIONAL_LANGUAGE_IND
		, ON_LINE_FILE_REVIEWS                               as                         DEP_ONLINE_FILE_REVIEW_IND
		, PULMONARY_EXAM                                     as                             DEP_PULMONARY_EXAM_IND
		, PULMONARY_REVIEW                                   as                           DEP_PULMONARY_REVIEW_IND
		, ASBESTOSIS_EXAM                                    as                            DEP_ASBESTOSIS_EXAM_IND
		, MCO_MEDICAL_DIRECTOR                               as                           MCO_MEDICAL_DIRECTOR_IND
		, PEACH_BASE_NUMBER                                  as                                SOS_PRVDR_BASE_NMBR
		, PEACH_SUFFIX_NUMBER                                as                                 SOS_PRVDR_SFX_NMBR 
				FROM     LOGIC_SOS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PREA                           as ( SELECT * from    RENAME_PREA ),

FILTER_PRDT                           as ( SELECT * from    RENAME_PRDT 
				WHERE PRDT_DCTVT_DTTM > current_date  ),
FILTER_PRED                           as ( SELECT * from    RENAME_PRED 
				WHERE PRED_DCTVT_DTTM > current_date  ),
FILTER_PA                             as ( SELECT * from    RENAME_PA 
				WHERE PA_ADRS_TYPE_CODE = 'PRCBU' and PA_DCTVT_DTTM > current_date and PA_ADRS_DCTVT_DTTM > current_date and PA_ENDNG_DATE > current_date and PA_PRMRY_LCTN_IND = 'Y' 
				QUALIFY (ROW_NUMBER()OVER(PARTITION BY PA_PRVDR_BASE_NMBR||PA_PRVDR_SFX_NMBR ORDER BY PA_CRT_DTTM DESC))=1 ),
FILTER_MA                             as ( SELECT * from    RENAME_MA 
				WHERE MA_ADRS_TYPE_CODE = 'CORRE' and MA_DCTVT_DTTM > current_date and MA_ADRS_DCTVT_DTTM > current_date and MA_ENDNG_DATE > current_date  
				QUALIFY (ROW_NUMBER()OVER(PARTITION BY MA_PRVDR_BASE_NMBR||MA_PRVDR_SFX_NMBR ORDER BY MA_CRT_DTTM DESC))=1  ),
FILTER_PT                             as ( SELECT * from    RENAME_PT 
				WHERE PT_DCTVT_DTTM > current_date  ),
FILTER_NPI                            as ( SELECT * from    RENAME_NPI 
				WHERE NPI_DCTVT_DTTM > current_date and NPI_ENDNG_DATE > current_date  ),
FILTER_CS                             as ( SELECT * from    RENAME_CS 
				WHERE CS_DCTVT_DTTM > current_date and CS_ENDNG_DATE > current_date  ),
FILTER_ES                             as ( SELECT * from    RENAME_ES 
				WHERE ES_DCTVT_DTTM > current_date and ES_ENDNG_DATE > current_date  ),
FILTER_PC                             as ( SELECT * from    RENAME_PC 
				WHERE PC_DCTVT_DTTM > current_date and PC_ENDNG_DATE > current_date and CNTCT_PRP_TYPE_CODE = 'OFFC' and TMPCNDT_DCTVT_DTTM > current_date and ADRS_TYPE_LCT_NMBR = 1  
				QUALIFY (ROW_NUMBER()OVER(PARTITION BY PC_PRVDR_BASE_NMBR||PC_PRVDR_SFX_NMBR ORDER BY PECN_CRT_DTTM DESC))=1  ),
FILTER_MEM                            as ( SELECT * from    RENAME_MEM   ),
FILTER_SOS                            as ( SELECT * from    RENAME_SOS   ),
FILTER_PDS                            as ( SELECT * from    RENAME_PDS 
				WHERE PDS_ENDNG_DATE > current_date and PDS_DCTVT_DTTM > current_date  ),
FILTER_OPSI                           as ( SELECT * from    RENAME_OPSI 
				WHERE OPSI_EXPIRATION_DATE > current_date  ),
FILTER_PRG                            as ( SELECT * from    RENAME_PRG 
				WHERE PRG_DCTVT_DTTM > current_date and PRG_ENDNG_DATE > current_date 
                QUALIFY (ROW_NUMBER()OVER(PARTITION BY PRG_PRVDR_BASE_NMBR||PRG_PRVDR_SFX_NMBR ORDER BY PROGRAM_PARTICIPATION_PERIOD DESC)) = 1  ),
FILTER_MDCR                           as ( SELECT * from    RENAME_MDCR 
				WHERE MDCR_CRDD_DCTVT_DTTM > current_date and MDCR_PCRD_DCTVT_DTTM > current_date and MDCR_EXPRT_DATE > current_date and MDCR_CRDN_TYPE_CODE = 'MDCR'  ),

FILTER_SPEC                           as ( SELECT  UPPER( LISTAGG( DISTINCT MDCL_SPC_TYPE_NAME , ', ') WITHIN GROUP (ORDER BY MDCL_SPC_TYPE_NAME) ) as PROVIDER_SPECIALTY_LIST_TEXT, 
															 SPEC_PRVDR_BASE_NMBR, 
															 SPEC_PRVDR_SFX_NMBR
                                          from    RENAME_SPEC 
                                                WHERE SPEC_CRDD_DCTVT_DTTM > current_date
                                                     and SPEC_PCRD_DCTVT_DTTM > current_date 
                                                     and SPEC_EXPRT_DATE > current_date and SPEC_MDCL_SPC_LVL_CODE = 'SPCT' 
                                                group by SPEC_PRVDR_BASE_NMBR,
												         SPEC_PRVDR_SFX_NMBR),
---- JOIN LAYER ----

PREA as ( SELECT * 
				FROM  FILTER_PREA
				        LEFT JOIN FILTER_PRDT ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PRDT.PRDT_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PRDT.PRDT_PRVDR_SFX_NMBR 
						LEFT JOIN FILTER_PRED ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PRED.PRED_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PRED.PRED_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_PA ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PA.PA_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PA.PA_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_MA ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_MA.MA_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_MA.MA_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_PT ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PT.PT_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PT.PT_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_NPI ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_NPI.NPI_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_NPI.NPI_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_CS ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_CS.CS_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_CS.CS_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_ES ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_ES.ES_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_ES.ES_PRVDR_SFX_NMBR 
						LEFT JOIN FILTER_PC ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PC.PC_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PC.PC_PRVDR_SFX_NMBR 
						LEFT JOIN FILTER_MEM ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_MEM.MEM_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_MEM.MEM_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_SOS ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_SOS.SOS_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_SOS.SOS_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_PDS ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PDS.PDS_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PDS.PDS_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_OPSI ON  FILTER_PREA.PEACH_NUMBER =  FILTER_OPSI.OPSI_PRO_NUM 
						LEFT JOIN FILTER_PRG ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_PRG.PRG_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_PRG.PRG_PRVDR_SFX_NMBR 
						LEFT JOIN FILTER_MDCR ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_MDCR.MDCR_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_MDCR.MDCR_PRVDR_SFX_NMBR
						LEFT JOIN FILTER_SPEC ON  FILTER_PREA.PEACH_BASE_NUMBER =  FILTER_SPEC.SPEC_PRVDR_BASE_NMBR AND FILTER_PREA.PEACH_SUFFIX_NUMBER = FILTER_SPEC.SPEC_PRVDR_SFX_NMBR
						)




SELECT 
  md5(cast(
    
    coalesce(cast(PEACH_BASE_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(PEACH_SUFFIX_NUMBER as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY 
, PEACH_NUMBER
, PEACH_FORMATTED_NUMBER
, PEACH_BASE_NUMBER
, PEACH_SUFFIX_NUMBER
, PRVDR_ID
, PRVDR_LEGAL_NAME
, PRSN_IND
, GNDR_CODE
, BIRTH_DATE
, DEATH_DATE
, BSNS_TYPE_CODE
, PRDT_CRT_DTTM
, PRDT_CRT_USER_CODE
, PRDT_DCTVT_DTTM
, PRDT_DCTVT_USER_CODE
, DBA_NAME
, PRCT_TYPE_CODE
, PRCT_TYPE_NAME
, PRVDR_TYPE_CODE
, PRVDR_TYPE_DESC
, PROVIDER_SPECIALTY_LIST_TEXT
, PROVIDER_PHONE_NUMBER
, PROVIDER_FAX_NUMBER
, PROVIDER_EMAIL_ADDRESS
, PRACTICE_STREET_ADDRESS_1
, PRACTICE_STREET_ADDRESS_2
, PRACTICE_CITY_NAME
, PRACTICE_STATE_CODE
, PRACTICE_ZIP_CODE_NMBR
, PRACTICE_ZIP_CODE_PLS4_NMBR
, PRACTICE_FULL_ZIP_CODE_NUMBER
, PRACTICE_COUNTY_NAME
, CORRESPONDENCE_STREET_ADDRESS_1
, CORRESPONDENCE_STREET_ADDRESS_2
, CORRESPONDENCE_CITY_NAME
, CORRESPONDENCE_STATE_CODE
, CORRESPONDENCE_ZIP_CODE_NMBR
, CORRESPONDENCE_ZIP_CODE_PLS4_NMBR
, CORRESPONDENCE_FULL_ZIP_CODE_NUMBER
, CORRESPONDENCE_COUNTY_NAME
, NPI_NMBR
, NPI_CNFRM_IND
, NPI_EFCTV_DATE
, MEDICARE_NUMBER
, MEDICARE_NUMBER_STATE_CODE
, MEDICARE_NUMBER_EFFECTIVE_DATE
, MEDICARE_PROVIDER_TYPE_CODE
, MEDICARE_PROVIDER_TYPE_DESC
, CRITICAL_ACCESS_IND
, OPPS_QUALITY_IND
, PROVIDER_CBSA_CODE
, CURRENT_ENROLLMENT_STATUS_DESC
, CURRENT_CERTIFICATION_STATUS_DESC
, NEW_PATIENT_CODE
, NEW_PATIENT_DESC
, PROVIDER_PROGRAM_CODE
, PROVIDER_PROGRAM_DESC
, PROVIDER_PROGRAM_FOCUS_TEXT
, PROGRAM_PARTICIPATION_PERIOD
, OUT_OF_STATE_STATUS_DESC
, DEP_SERVICE_CODE
, DEP_SERVICE_DESC
, NULL AS DEP_STATUS_DESC 
, DEP_ACTIVE_IND
, DEP_90_DAY_EXAM_IND
, DEP_ADR_IME_IND
, DEP_ADR_FILE_REVIEW_IND
, DEP_B_READER_IND
, DEP_C92_EXAM_IND
, DEP_C92A_FILE_REVIEW_IND
, DEP_DUR_REVIEW_IND
, DEP_IME_IND
, DEP_MEDICAL_FILE_REVIEW_IND
, DEP_PRIOR_AUTHORIZATION_REVIEW_IND
, DEP_DM_IME_IND
, DEP_ADDITIONAL_LANGUAGE_IND
, DEP_ONLINE_FILE_REVIEW_IND
, DEP_PULMONARY_EXAM_IND
, DEP_PULMONARY_REVIEW_IND
, DEP_ASBESTOSIS_EXAM_IND
, MCO_MEDICAL_DIRECTOR_IND
, DEP_ADMINISTRATIVE_AGENT_NAME
from PREA