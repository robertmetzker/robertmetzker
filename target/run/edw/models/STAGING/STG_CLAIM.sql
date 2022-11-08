

      create or replace  table DEV_EDW.STAGING.STG_CLAIM  as
      (---- SRC LAYER ----
WITH
SRC_C as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM ),
SRC_ST as ( SELECT *     from      DEV_VIEWS.PCMP.STATE ),
SRC_CO as ( SELECT *     from      DEV_VIEWS.PCMP.COUNTRY  ),
SRC_CCT as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_CLAIM_TYPE ),
SRC_CT as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_TYPE ),
SRC_JT as ( SELECT *     from      DEV_VIEWS.PCMP.JURISDICTION_TYPE ),
SRC_OMT as ( SELECT *     from      DEV_VIEWS.PCMP.OCCURRENCE_MEDIA_TYPE  ),
SRC_OPT as ( SELECT *     from      DEV_VIEWS.PCMP.OCCURRENCE_PREMISES_TYPE  ),
SRC_OST as ( SELECT *     from      DEV_VIEWS.PCMP.OCCURRENCE_SOURCE_TYPE ),
SRC_UCT as ( SELECT *     from      DEV_VIEWS.PCMP.UNUSUAL_CLAIM_TYPE ),
SRC_CAD as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_ADDITIONAL_DETAIL  ),
SRC_ST2 as ( SELECT *     from      DEV_VIEWS.PCMP.STATE ),
SRC_ELT as ( SELECT *     from      DEV_VIEWS.PCMP.EDUCATION_LEVEL_TYPE  ),
SRC_CEST as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_EMPLOYMENT_STATUS_TYPE ),
SRC_CIT as ( SELECT *     from      DEV_VIEWS.PCMP.CATASTROPHIC_INJURY_TYPE ),
SRC_NOIT as ( SELECT *     from      DEV_VIEWS.PCMP.NATURE_OF_INJURY_TYPE ),
SRC_NOICT as ( SELECT *     from      DEV_VIEWS.PCMP.NATURE_OF_INJURY_CATEGORY_TYPE ),
SRC_COLCT as ( SELECT *     from      DEV_VIEWS.PCMP.CAUSE_OF_LOSS_CATEGORY_TYPE ),
SRC_COLT as ( SELECT *     from      DEV_VIEWS.PCMP.CAUSE_OF_LOSS_TYPE ),
//SRC_C as ( SELECT *     from     CLAIM) ,
//SRC_ST as ( SELECT *     from     STATE) ,
//SRC_CO as ( SELECT *     from     COUNTRY) ,
//SRC_CCT as ( SELECT *     from     CLAIM_CLAIM_TYPE) ,
//SRC_CT as ( SELECT *     from     CLAIM_TYPE) ,
//SRC_JT as ( SELECT *     from     JURISDICTION_TYPE) ,
//SRC_OMT as ( SELECT *     from     OCCURRENCE_MEDIA_TYPE) ,
//SRC_OPT as ( SELECT *     from     OCCURRENCE_PREMISES_TYPE) ,
//SRC_OST as ( SELECT *     from     OCCURRENCE_SOURCE_TYPE) ,
//SRC_UCT as ( SELECT *     from     UNUSUAL_CLAIM_TYPE) ,
//SRC_CAD as ( SELECT *     from     CLAIM_ADDITIONAL_DETAIL) ,
//SRC_ST2 as ( SELECT *     from     STATE) ,
//SRC_ELT as ( SELECT *     from     EDUCATION_LEVEL_TYPE) ,
//SRC_CEST as ( SELECT *     from     CLAIM_EMPLOYMENT_STATUS_TYPE) ,
//SRC_CIT            as ( SELECT *     FROM     CATASTROPHIC_INJURY_TYPE) ,
//SRC_NOIT as ( SELECT *     from     NATURE_OF_INJURY_TYPE) ,
//SRC_NOICT as ( SELECT *     from     NATURE_OF_INJURY_CATEGORY_TYPE) ,
//SRC_COLCT as ( SELECT *     from     CAUSE_OF_LOSS_CATEGORY_TYPE) ,
//SRC_COLT as ( SELECT *     from     CAUSE_OF_LOSS_TYPE) ,
----LOGIC LAYER----

LOGIC_C as ( SELECT 
		AGRE_ID::NUMERIC(31) AS AGRE_ID,
		TRIM(CLM_NO) AS CLM_NO,
		cast(CLM_OCCR_DTM as DATE) AS CLM_OCCR_DTM,
		cast(CLM_OCCR_RPT_DT as DATE) AS CLM_OCCR_RPT_DT,
		iff(regexp_like(CLM_LOSS_DESC, '.*\\w.*'),UPPER(TRIM(CLM_LOSS_DESC)), null ) CLM_LOSS_DESC,
		UPPER(TRIM(CLM_OCCR_LOC_NM)) AS CLM_OCCR_LOC_NM,
	case when UPPER(TRIM(CLM_OCCR_LOC_STR_1))='' then NULL
	else UPPER(TRIM(CLM_OCCR_LOC_STR_1)) 
		END AS CLM_OCCR_LOC_STR_1,
	case when UPPER(TRIM(CLM_OCCR_LOC_STR_2))='' then NULL
	else UPPER(TRIM(CLM_OCCR_LOC_STR_2))
		END AS CLM_OCCR_LOC_STR_2,
		UPPER(TRIM(CLM_OCCR_LOC_CITY_NM)) AS CLM_OCCR_LOC_CITY_NM,
		UPPER(TRIM(CLM_OCCR_LOC_POST_CD)) AS CLM_OCCR_LOC_POST_CD,
		UPPER(TRIM(CLM_OCCR_LOC_CNTY_NM)) AS CLM_OCCR_LOC_CNTY_NM,
	case when UPPER(TRIM(CLM_OCCR_LOC_COMT))='' then NULL
	else UPPER(TRIM(CLM_OCCR_LOC_COMT))
		END AS CLM_OCCR_LOC_COMT,
		UPPER(TRIM(JUR_TYP_CD)) AS JUR_TYP_CD,
		UPPER(TRIM(OCCR_MEDA_TYP_CD)) AS OCCR_MEDA_TYP_CD,
		UPPER(TRIM(OCCR_PRMS_TYP_CD)) AS OCCR_PRMS_TYP_CD,
		UPPER(TRIM(OCCR_SRC_TYP_CD)) AS OCCR_SRC_TYP_CD,
		UPPER(TRIM(UNSL_CLM_TYP_CD)) AS UNSL_CLM_TYP_CD,
		cast(CLM_FST_DCSN_DT as DATE) AS CLM_FST_DCSN_DT,
		cast(CLM_DISAB_BGN_DT as DATE) AS CLM_DISAB_BGN_DT,
		CLM_CLMT_SHFT_BGN_TIME AS CLM_CLMT_SHFT_BGN_TIME,
		UPPER(CLM_CLMT_FATL_IND) AS CLM_CLMT_FATL_IND,
		iff(regexp_like(CLM_CLMT_JOB_TTL, '.*\\w.*'),UPPER(TRIM(CLM_CLMT_JOB_TTL)), null ) CLM_CLMT_JOB_TTL,
		cast(CLM_CLMT_HIRE_DT as DATE) AS CLM_CLMT_HIRE_DT,
		cast(CLM_CLMT_LST_WK_DT as DATE) AS CLM_CLMT_LST_WK_DT,
		CLM_CLMT_HIRE_STT_ID::NUMERIC(31) AS CLM_CLMT_HIRE_STT_ID,
		UPPER(TRIM(EDUC_LVL_TYP_CD)) AS EDUC_LVL_TYP_CD,
		UPPER(TRIM(CLM_EMPL_STS_TYP_CD)) AS CLM_EMPL_STS_TYP_CD,
		cast(CLM_EMPLR_NTF_DT as DATE) AS CLM_EMPLR_NTF_DT,
		cast(CLM_ADMN_NTF_DT as DATE) AS CLM_ADMN_NTF_DT,
		UPPER(CLM_ALCH_INVD_IND) AS CLM_ALCH_INVD_IND,
		UPPER(CLM_DRUG_INVD_IND) AS CLM_DRUG_INVD_IND,
        UPPER(CLM_CTRPH_INJR_IND) AS CLM_CTRPH_INJR_IND,
		upper( TRIM( CTRPH_INJR_TYP_CD ) ) as CTRPH_INJR_TYP_CD, 
		UPPER(TRIM(NOI_TYP_CD)) AS NOI_TYP_CD,
		UPPER(TRIM(NOI_CTG_TYP_CD)) AS NOI_CTG_TYP_CD,
		UPPER(TRIM(CAUS_OF_LOSS_CTG_TYP_CD)) AS CAUS_OF_LOSS_CTG_TYP_CD,
		UPPER(TRIM(CAUS_OF_LOSS_TYP_CD)) AS CAUS_OF_LOSS_TYP_CD,
		UPPER(CLM_NOTE_IND) AS CLM_NOTE_IND,
		UPPER(CLM_REL_SNPSHT_IND) AS CLM_REL_SNPSHT_IND,
		AUDIT_USER_ID_CREA::NUMERIC(31) AS AUDIT_USER_ID_CREA,
		AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,
		AUDIT_USER_ID_UPDT::NUMERIC(31) AS AUDIT_USER_ID_UPDT,
		AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,
		STT_ID::NUMERIC(31) AS STT_ID,
		CNTRY_ID::NUMERIC(31) AS CNTRY_ID 
				from SRC_C
            ),
LOGIC_ST as ( SELECT 
		UPPER(TRIM(STT_ABRV)) AS STT_ABRV,
		UPPER(TRIM(STT_NM)) AS STT_NM,
		STT_ID::NUMERIC(31) AS STT_ID,
		UPPER(TRIM(STT_VOID_IND)) AS STT_VOID_IND 
				from SRC_ST
            ),
LOGIC_CO as ( SELECT 
		UPPER(TRIM(CNTRY_NM)) AS CNTRY_NM,
		CNTRY_ID::NUMERIC(31) AS CNTRY_ID,
		UPPER(TRIM(CNTRY_VOID_IND)) AS CNTRY_VOID_IND 
				from SRC_CO
            ),
LOGIC_CCT as ( SELECT 
		UPPER(TRIM(CLM_TYP_CD)) AS CLM_TYP_CD,
		AGRE_ID::NUMERIC(31) AS AGRE_ID 
				from SRC_CCT
            ),
LOGIC_CT as ( SELECT 
		UPPER(TRIM(CLM_TYP_NM)) AS CLM_TYP_NM,
		UPPER(TRIM(CLM_TYP_CD)) AS CLM_TYP_CD 
				from SRC_CT
            ),
LOGIC_JT as ( SELECT 
		UPPER(TRIM(JUR_TYP_NM)) AS JUR_TYP_NM,
		UPPER(TRIM(JUR_TYP_CD)) AS JUR_TYP_CD 
				from SRC_JT
            ),
LOGIC_OMT as ( SELECT 
		UPPER(TRIM(OCCR_MEDA_TYP_NM)) AS OCCR_MEDA_TYP_NM,
		UPPER(TRIM(OCCR_MEDA_TYP_CD)) AS OCCR_MEDA_TYP_CD 
				from SRC_OMT
            ),
LOGIC_OPT as ( SELECT 
		UPPER(TRIM(OCCR_PRMS_TYP_NM)) AS OCCR_PRMS_TYP_NM,
		UPPER(TRIM(OCCR_PRMS_TYP_CD)) AS OCCR_PRMS_TYP_CD 
				from SRC_OPT
            ),
LOGIC_OST as ( SELECT 
		UPPER(TRIM(OCCR_SRC_TYP_NM)) AS OCCR_SRC_TYP_NM,
		UPPER(TRIM(OCCR_SRC_TYP_CD)) AS OCCR_SRC_TYP_CD 
				from SRC_OST
            ),
LOGIC_UCT as ( SELECT 
		UPPER(TRIM(UNSL_CLM_TYP_NM)) AS UNSL_CLM_TYP_NM,
		UPPER(TRIM(UNSL_CLM_TYP_CD)) AS UNSL_CLM_TYP_CD,
		UPPER(TRIM(VOID_IND)) AS VOID_IND 
				from SRC_UCT
            ),
LOGIC_CAD as ( SELECT 
		cast(CLM_STATU_LMT_DT as DATE) AS CLM_STATU_LMT_DT,
		AGRE_ID::NUMERIC(31) AS AGRE_ID,
		UPPER(TRIM(VOID_IND)) AS VOID_IND 
				from SRC_CAD
            ),
LOGIC_ST2 as ( SELECT 
		UPPER(TRIM(STT_ABRV)) AS STT_ABRV,
		UPPER(TRIM(STT_NM)) AS STT_NM,
		STT_ID::NUMERIC(31) AS STT_ID,
		UPPER(TRIM(STT_VOID_IND)) AS STT_VOID_IND 
				from SRC_ST2
            ),
LOGIC_ELT as ( SELECT 
		UPPER(TRIM(EDUC_LVL_TYP_NM)) AS EDUC_LVL_TYP_NM,
		UPPER(TRIM(EDUC_LVL_TYP_CD)) AS EDUC_LVL_TYP_CD,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_ELT
            ),
LOGIC_CEST as ( SELECT 
		UPPER(TRIM(CLM_EMPL_STS_TYP_NM)) AS CLM_EMPL_STS_TYP_NM,
		UPPER(TRIM(CLM_EMPL_STS_TYP_CD)) AS CLM_EMPL_STS_TYP_CD,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_CEST
            ),
LOGIC_CIT as ( SELECT 
		  upper( TRIM( CTRPH_INJR_TYP_NM ) )                 as                                  CTRPH_INJR_TYP_NM 
		, upper( TRIM( VOID_IND ) )                          as                                           VOID_IND 
		, upper( TRIM( CTRPH_INJR_TYP_CD ) )                 as                                  CTRPH_INJR_TYP_CD 
		FROM SRC_CIT
            ),			
LOGIC_NOIT as ( SELECT 
		UPPER(TRIM(NOI_TYP_NM)) AS NOI_TYP_NM,
		UPPER(TRIM(NOI_TYP_CD)) AS NOI_TYP_CD,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_NOIT
            ),
LOGIC_NOICT as ( SELECT 
		UPPER(TRIM(NOI_CTG_TYP_NM)) AS NOI_CTG_TYP_NM,
		UPPER(TRIM(NOI_CTG_TYP_CD)) AS NOI_CTG_TYP_CD,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_NOICT
            ),
LOGIC_COLCT as ( SELECT 
		UPPER(TRIM(CAUS_OF_LOSS_CTG_TYP_NM)) AS CAUS_OF_LOSS_CTG_TYP_NM,
		UPPER(TRIM(CAUS_OF_LOSS_CTG_TYP_CD)) AS CAUS_OF_LOSS_CTG_TYP_CD,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_COLCT
            ),
LOGIC_COLT as ( SELECT 
		UPPER(TRIM(CAUS_OF_LOSS_TYP_NM)) AS CAUS_OF_LOSS_TYP_NM,
		UPPER(TRIM(CAUS_OF_LOSS_TYP_CD)) AS CAUS_OF_LOSS_TYP_CD,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_COLT
            )
----RENAME LAYER ----
,
RENAME_C as ( SELECT AGRE_ID AS AGRE_ID,CLM_NO AS CLM_NO,
			
			CLM_OCCR_DTM AS CLM_OCCR_DATE,
			
			CLM_OCCR_RPT_DT AS CLM_OCCR_RPT_DATE,
			CLM_LOSS_DESC AS CLM_LOSS_DESC,
			CLM_OCCR_LOC_NM AS CLM_OCCR_LOC_NM,
			CLM_OCCR_LOC_STR_1 AS CLM_OCCR_LOC_STR_1,
			CLM_OCCR_LOC_STR_2 AS CLM_OCCR_LOC_STR_2,
			CLM_OCCR_LOC_CITY_NM AS CLM_OCCR_LOC_CITY_NM,
			CLM_OCCR_LOC_POST_CD AS CLM_OCCR_LOC_POST_CD,
			CLM_OCCR_LOC_CNTY_NM AS CLM_OCCR_LOC_CNTY_NM,
			CLM_OCCR_LOC_COMT AS CLM_OCCR_LOC_COMT,
			JUR_TYP_CD AS JUR_TYP_CD,
			OCCR_MEDA_TYP_CD AS OCCR_MEDA_TYP_CD,
			OCCR_PRMS_TYP_CD AS OCCR_PRMS_TYP_CD,
			OCCR_SRC_TYP_CD AS OCCR_SRC_TYP_CD,
			UNSL_CLM_TYP_CD AS UNSL_CLM_TYP_CD,
			CLM_FST_DCSN_DT AS CLM_FST_DCSN_DATE,
			CLM_DISAB_BGN_DT AS CLM_DISAB_BGN_DATE,
			CLM_CLMT_SHFT_BGN_TIME AS CLM_CLMT_SHFT_BGN_DTM,
			CLM_CLMT_FATL_IND AS CLM_CLMT_FATL_IND,
			CLM_CLMT_JOB_TTL AS CLM_CLMT_JOB_TTL,
			CLM_CLMT_HIRE_DT AS CLM_CLMT_HIRE_DATE,
			CLM_CLMT_LST_WK_DT AS CLM_CLMT_LST_WK_DATE,
			CLM_CLMT_HIRE_STT_ID AS CLM_CLMT_HIRE_STT_ID,
			EDUC_LVL_TYP_CD AS EDUC_LVL_TYP_CD,
			CLM_EMPL_STS_TYP_CD AS CLM_EMPL_STS_TYP_CD,
			CLM_EMPLR_NTF_DT AS CLM_EMPLR_NTF_DATE,
			CLM_ADMN_NTF_DT AS CLM_ADMN_NTF_DATE,
			CLM_ALCH_INVD_IND AS CLM_ALCH_INVD_IND,
			CLM_DRUG_INVD_IND AS CLM_DRUG_INVD_IND,
			CLM_CTRPH_INJR_IND AS CLM_CTRPH_INJR_IND,
			CTRPH_INJR_TYP_CD  as CTRPH_INJR_TYP_CD,
			NOI_TYP_CD AS NOI_TYP_CD,
			NOI_CTG_TYP_CD AS NOI_CTG_TYP_CD,
			CAUS_OF_LOSS_CTG_TYP_CD AS CAUS_OF_LOSS_CTG_TYP_CD,
			CAUS_OF_LOSS_TYP_CD AS CAUS_OF_LOSS_TYP_CD,
			CLM_NOTE_IND AS CLM_NOTE_IND,
			CLM_REL_SNPSHT_IND AS CLM_REL_SNPSHT_IND,
			AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,
			AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,
			AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,
			AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,
			STT_ID AS STT_ID,
			CNTRY_ID AS CNTRY_ID 
			from      LOGIC_C
        ),
RENAME_ST as ( SELECT 
			
			STT_ABRV AS CLM_OCCR_LOC_STT_CD,
			
			STT_NM AS CLM_OCCR_LOC_STT_NM,
			
			STT_ID AS ST_STT_ID,
			
			STT_VOID_IND AS ST_STT_VOID_IND 
			from      LOGIC_ST
        ),
RENAME_CO as ( SELECT 
			
			CNTRY_NM AS CLM_OCCR_LOC_CNTRY_NM,
			
			CNTRY_ID AS CO_CNTRY_ID,
			
			CNTRY_VOID_IND AS CO_CNTRY_VOID_IND 
			from      LOGIC_CO
        ),
RENAME_CCT as ( SELECT CLM_TYP_CD AS CLM_TYP_CD,
			
			AGRE_ID AS CCT_AGRE_ID 
			from      LOGIC_CCT
        ),
RENAME_CT as ( SELECT CLM_TYP_NM AS CLM_TYP_NM,
			
			CLM_TYP_CD AS CT_CLM_TYP_CD 
			from      LOGIC_CT
        ),
RENAME_JT as ( SELECT JUR_TYP_NM AS JUR_TYP_NM,
			
			JUR_TYP_CD AS JT_JUR_TYP_CD 
			from      LOGIC_JT
        ),
RENAME_OMT as ( SELECT OCCR_MEDA_TYP_NM AS OCCR_MEDA_TYP_NM,
			
			OCCR_MEDA_TYP_CD AS OMT_OCCR_MEDA_TYP_CD 
			from      LOGIC_OMT
        ),
RENAME_OPT as ( SELECT OCCR_PRMS_TYP_NM AS OCCR_PRMS_TYP_NM,
			
			OCCR_PRMS_TYP_CD AS OPT_OCCR_PRMS_TYP_CD 
			from      LOGIC_OPT
        ),
RENAME_OST as ( SELECT OCCR_SRC_TYP_NM AS OCCR_SRC_TYP_NM,
			
			OCCR_SRC_TYP_CD AS OST_OCCR_SRC_TYP_CD 
			from      LOGIC_OST
        ),
RENAME_UCT as ( SELECT UNSL_CLM_TYP_NM AS UNSL_CLM_TYP_NM,
			
			UNSL_CLM_TYP_CD AS UCT_UNSL_CLM_TYP_CD,
			
			VOID_IND AS UCT_VOID_IND 
			from      LOGIC_UCT
        ),
RENAME_CAD as ( SELECT 
			
			CLM_STATU_LMT_DT AS CLM_STATU_LMT_DATE,
			
			AGRE_ID AS CAD_AGRE_ID,
			
			VOID_IND AS CAD_VOID_IND 
			from      LOGIC_CAD
        ),
RENAME_ST2 as ( SELECT 
			
			STT_ABRV AS CLM_CLMT_HIRE_STT_CD,
			
			STT_NM AS CLM_CLMT_HIRE_STT_NM,
			
			STT_ID AS ST2_STT_ID,
			
			STT_VOID_IND AS ST2_STT_VOID_IND 
			from      LOGIC_ST2
        ),
RENAME_ELT as ( SELECT EDUC_LVL_TYP_NM AS EDUC_LVL_TYP_NM,
			
			EDUC_LVL_TYP_CD AS ELT_EDUC_LVL_TYP_CD,
			
			VOID_IND AS ELT_VOID_IND 
			from      LOGIC_ELT
        ),
RENAME_CEST as ( SELECT CLM_EMPL_STS_TYP_NM AS CLM_EMPL_STS_TYP_NM,
			
			CLM_EMPL_STS_TYP_CD AS CEST_CLM_EMPL_STS_TYP_CD,
			
			VOID_IND AS CEST_VOID_IND 
			from      LOGIC_CEST
        ),
RENAME_CIT        as ( SELECT 
		  CTRPH_INJR_TYP_NM                                  as                                  CTRPH_INJR_TYP_NM
		, VOID_IND                                           as                                       CIT_VOID_IND
		, CTRPH_INJR_TYP_CD                                  as                              CIT_CTRPH_INJR_TYP_CD 
				FROM     LOGIC_CIT   ), 		
RENAME_NOIT as ( SELECT NOI_TYP_NM AS NOI_TYP_NM,
			
			NOI_TYP_CD AS NOIT_NOI_TYP_CD,
			
			VOID_IND AS NOIT_VOID_IND 
			from      LOGIC_NOIT
        ),
RENAME_NOICT as ( SELECT NOI_CTG_TYP_NM AS NOI_CTG_TYP_NM,
			
			NOI_CTG_TYP_CD AS NOICT_NOI_CTG_TYP_CD,
			
			VOID_IND AS NOICT_VOID_IND 
			from      LOGIC_NOICT
        ),
RENAME_COLCT as ( SELECT CAUS_OF_LOSS_CTG_TYP_NM AS CAUS_OF_LOSS_CTG_TYP_NM,
			
			CAUS_OF_LOSS_CTG_TYP_CD AS COLCT_CAUS_OF_LOSS_CTG_TYP_CD,
			
			VOID_IND AS COLCT_VOID_IND 
			from      LOGIC_COLCT
        ),
RENAME_COLT as ( SELECT CAUS_OF_LOSS_TYP_NM AS CAUS_OF_LOSS_TYP_NM,
			
			CAUS_OF_LOSS_TYP_CD AS COLT_CAUS_OF_LOSS_TYP_CD,
			
			VOID_IND AS COLT_VOID_IND 
			from      LOGIC_COLT
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_C as ( SELECT  * 
			from     RENAME_C 
            
        ),

        FILTER_ST as ( SELECT  * 
			from     RENAME_ST 
            WHERE ST_STT_VOID_IND = 'N'
        ),

        FILTER_CO as ( SELECT  * 
			from     RENAME_CO 
            WHERE CO_CNTRY_VOID_IND = 'N'
        ),

        FILTER_JT as ( SELECT  * 
			from     RENAME_JT 
            
        ),

        FILTER_CAD as ( SELECT  * 
			from     RENAME_CAD 
            WHERE CAD_VOID_IND = 'N'
        ),

        FILTER_UCT as ( SELECT  * 
			from     RENAME_UCT 
            WHERE UCT_VOID_IND = 'N'
        ),

        FILTER_OMT as ( SELECT  * 
			from     RENAME_OMT 
            
        ),

        FILTER_OPT as ( SELECT  * 
			from     RENAME_OPT 
            
        ),

        FILTER_OST as ( SELECT  * 
			from     RENAME_OST 
            
        ),

        FILTER_CCT as ( SELECT  * 
			from     RENAME_CCT 
            
        ),

        FILTER_CT as ( SELECT  * 
			from     RENAME_CT 
            
        ),

        FILTER_ELT as ( SELECT  * 
			from     RENAME_ELT 
            WHERE ELT_VOID_IND = 'N'
        ),

        FILTER_CEST as ( SELECT  * 
			from     RENAME_CEST 
            WHERE CEST_VOID_IND = 'N'
        ),

        FILTER_NOIT as ( SELECT  * 
			from     RENAME_NOIT 
            WHERE NOIT_VOID_IND = 'N'
        ),

        FILTER_NOICT as ( SELECT  * 
			from     RENAME_NOICT 
            WHERE NOICT_VOID_IND = 'N'
        ),

        FILTER_COLT as ( SELECT  * 
			from     RENAME_COLT 
            WHERE COLT_VOID_IND = 'N'
        ),

        FILTER_COLCT as ( SELECT  * 
			from     RENAME_COLCT 
            WHERE COLCT_VOID_IND = 'N'
        ),

        FILTER_ST2 as ( SELECT  * 
			from     RENAME_ST2 
            WHERE ST2_STT_VOID_IND = 'N'
        ),
		FILTER_CIT   as ( SELECT * FROM    RENAME_CIT 
                                            WHERE CIT_VOID_IND = 'N'  )

----JOIN LAYER----
,
CCT as ( SELECT * 
			from  FILTER_CCT
				LEFT JOIN FILTER_CT ON FILTER_CCT.CLM_TYP_CD = FILTER_CT.CT_CLM_TYP_CD  ),
C as ( SELECT * 
			from  FILTER_C
				LEFT JOIN FILTER_ST ON FILTER_C.STT_ID = FILTER_ST.ST_STT_ID 
				LEFT JOIN FILTER_CO ON FILTER_C.CNTRY_ID = FILTER_CO.CO_CNTRY_ID 
				LEFT JOIN FILTER_JT ON FILTER_C.JUR_TYP_CD = FILTER_JT.JT_JUR_TYP_CD 
				LEFT JOIN FILTER_CAD ON FILTER_C.AGRE_ID = FILTER_CAD.CAD_AGRE_ID 
				LEFT JOIN FILTER_UCT ON FILTER_C.UNSL_CLM_TYP_CD = FILTER_UCT.UCT_UNSL_CLM_TYP_CD 
				LEFT JOIN FILTER_OMT ON FILTER_C.OCCR_MEDA_TYP_CD = FILTER_OMT.OMT_OCCR_MEDA_TYP_CD 
				LEFT JOIN FILTER_OPT ON FILTER_C.OCCR_PRMS_TYP_CD = FILTER_OPT.OPT_OCCR_PRMS_TYP_CD 
				LEFT JOIN FILTER_OST ON FILTER_C.OCCR_SRC_TYP_CD = FILTER_OST.OST_OCCR_SRC_TYP_CD 
				LEFT JOIN CCT ON FILTER_C.AGRE_ID = CCT.CCT_AGRE_ID 
				LEFT JOIN FILTER_ELT ON FILTER_C.EDUC_LVL_TYP_CD = FILTER_ELT.ELT_EDUC_LVL_TYP_CD 
				LEFT JOIN FILTER_CEST ON FILTER_C.CLM_EMPL_STS_TYP_CD = FILTER_CEST.CEST_CLM_EMPL_STS_TYP_CD 
				LEFT JOIN FILTER_NOIT ON FILTER_C.NOI_TYP_CD = FILTER_NOIT.NOIT_NOI_TYP_CD 
				LEFT JOIN FILTER_NOICT ON FILTER_C.NOI_CTG_TYP_CD = FILTER_NOICT.NOICT_NOI_CTG_TYP_CD 
				LEFT JOIN FILTER_COLT ON FILTER_C.CAUS_OF_LOSS_TYP_CD = FILTER_COLT.COLT_CAUS_OF_LOSS_TYP_CD 
				LEFT JOIN FILTER_COLCT ON FILTER_C.CAUS_OF_LOSS_CTG_TYP_CD = FILTER_COLCT.COLCT_CAUS_OF_LOSS_CTG_TYP_CD 
				LEFT JOIN FILTER_ST2 ON FILTER_C.CLM_CLMT_HIRE_STT_ID = FILTER_ST2.ST2_STT_ID
				LEFT JOIN FILTER_CIT ON  FILTER_C.CTRPH_INJR_TYP_CD =  FILTER_CIT.CIT_CTRPH_INJR_TYP_CD  )
select * from C
      );
    