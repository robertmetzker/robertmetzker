----SRC LAYER----
WITH
SRC_PRSN as ( SELECT *     from      DEV_VIEWS.PCMP.PERSON_HISTORY ),
SRC_GT as ( SELECT *     from      DEV_VIEWS.PCMP.GENDER_TYPE ),
SRC_MST as ( SELECT *     from      DEV_VIEWS.PCMP.MARITAL_STATUS_TYPE ),
SRC_TFST as ( SELECT *     from      DEV_VIEWS.PCMP.TAX_FILING_STATUS_TYPE ),
SRC_CH as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER_HISTORY )
//SRC_PRSN as ( SELECT *     from      PERSON_HISTORY),
//SRC_GT as ( SELECT *     from      GENDER_TYPE),
//SRC_MST as ( SELECT *     from      MARITAL_STATUS_TYPE),
//SRC_TFST as ( SELECT *     from      TAX_FILING_STATUS_TYPE),
//SRC_CH as ( SELECT *     from      CUSTOMER_HISTORY)
----LOGIC LAYER----
,
LOGIC_PRSN as ( SELECT 
		HIST_ID AS HIST_ID,
		CUST_ID AS CUST_ID,
		cast(PRSN_BRTH_DTM as DATE) AS PRSN_BRTH_DTM,
		cast(PRSN_DTH_DTM as DATE) AS PRSN_DTH_DTM,
		UPPER(TRIM(GNDR_TYP_CD)) AS GNDR_TYP_CD,
		UPPER(TRIM(MAR_STS_TYP_CD)) AS MAR_STS_TYP_CD,
		UPPER(TRIM(TAX_FILE_STS_TYP_CD)) AS TAX_FILE_STS_TYP_CD,
		PRSN_TAX_EXMT_NO AS PRSN_TAX_EXMT_NO,
		UPPER(TRIM(PRSN_TAX_INCMPT_IND)) AS PRSN_TAX_INCMPT_IND 
				from SRC_PRSN
            ),
LOGIC_GT as ( SELECT 
		UPPER(TRIM(GNDR_TYP_NM)) AS GNDR_TYP_NM,
		UPPER(TRIM(GNDR_TYP_CD)) AS GNDR_TYP_CD 
				from SRC_GT
            ),
LOGIC_MST as ( SELECT 
		UPPER(TRIM(MAR_STS_TYP_NM)) AS MAR_STS_TYP_NM,
		UPPER(TRIM(MAR_STS_TYP_CD)) AS MAR_STS_TYP_CD 
				from SRC_MST
            ),
LOGIC_TFST as ( SELECT 
		UPPER(TRIM(TAX_FILE_STS_TYP_NM)) AS TAX_FILE_STS_TYP_NM,
		UPPER(TRIM(TAX_FILE_STS_TYP_CD)) AS TAX_FILE_STS_TYP_CD 
				from SRC_TFST
            ),
LOGIC_CH as ( SELECT 
		HIST_EFF_DTM AS HIST_EFF_DTM,
		HIST_END_DTM AS HIST_END_DTM,
		HIST_ID AS HIST_ID 
				from SRC_CH
            )
----RENAME LAYER ----
,
RENAME_PRSN as ( SELECT HIST_ID AS HIST_ID,CUST_ID AS CUST_ID,
			
			PRSN_BRTH_DTM AS PRSN_BIRTH_DATE,
			
			PRSN_DTH_DTM AS PRSN_DEATH_DATE,GNDR_TYP_CD AS GNDR_TYP_CD,MAR_STS_TYP_CD AS MAR_STS_TYP_CD,TAX_FILE_STS_TYP_CD AS TAX_FILE_STS_TYP_CD,PRSN_TAX_EXMT_NO AS PRSN_TAX_EXMT_NO,PRSN_TAX_INCMPT_IND AS PRSN_TAX_INCMPT_IND 
			from      LOGIC_PRSN
        ),
RENAME_GT as ( SELECT GNDR_TYP_NM AS GNDR_TYP_NM,
			
			GNDR_TYP_CD AS GT_GNDR_TYP_CD 
			from      LOGIC_GT
        ),
RENAME_MST as ( SELECT MAR_STS_TYP_NM AS MAR_STS_TYP_NM,
			
			MAR_STS_TYP_CD AS MST_MAR_STS_TYP_CD 
			from      LOGIC_MST
        ),
RENAME_TFST as ( SELECT TAX_FILE_STS_TYP_NM AS TAX_FILE_STS_TYP_NM,
			
			TAX_FILE_STS_TYP_CD AS TFST_TAX_FILE_STS_TYP_CD 
			from      LOGIC_TFST
        ),
RENAME_CH as ( SELECT HIST_EFF_DTM AS HIST_EFF_DTM,HIST_END_DTM AS HIST_END_DTM,
			
			HIST_ID AS CH_HIST_ID 
			from      LOGIC_CH
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_PRSN as ( SELECT  * 
			from     RENAME_PRSN 
            
        ),

        FILTER_MST as ( SELECT  * 
			from     RENAME_MST 
            
        ),

        FILTER_GT as ( SELECT  * 
			from     RENAME_GT 
            
        ),

        FILTER_TFST as ( SELECT  * 
			from     RENAME_TFST 
            
        ),

        FILTER_CH as ( SELECT  * 
			from     RENAME_CH 
            
        )
----JOIN LAYER----
,
PRSN as ( SELECT * 
			from  FILTER_PRSN
				LEFT JOIN FILTER_MST ON FILTER_PRSN.MAR_STS_TYP_CD = FILTER_MST.MST_MAR_STS_TYP_CD 
				LEFT JOIN FILTER_GT ON FILTER_PRSN.GNDR_TYP_CD = FILTER_GT.GT_GNDR_TYP_CD
				LEFT JOIN FILTER_TFST ON FILTER_PRSN.TAX_FILE_STS_TYP_CD = FILTER_TFST.TFST_TAX_FILE_STS_TYP_CD 
				INNER JOIN FILTER_CH ON FILTER_PRSN.HIST_ID = FILTER_CH.CH_HIST_ID )
select * from PRSN