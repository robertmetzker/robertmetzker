----SRC LAYER----
WITH
SRC_P as ( SELECT *     from      DEV_VIEWS.PCMP.PARTICIPATION ),
SRC_PT as ( SELECT *     from      DEV_VIEWS.PCMP.PARTICIPATION_TYPE  ),
SRC_C as ( SELECT *     from      DEV_VIEWS.PCMP.CUSTOMER  ),
SRC_CP as ( SELECT *     from      DEV_VIEWS.PCMP.CLAIM_PARTICIPATION  )
----LOGIC LAYER----
,
LOGIC_P as ( SELECT 
		PTCP_ID AS PTCP_ID,
		AGRE_ID AS AGRE_ID,
		UPPER(TRIM(PTCP_TYP_CD)) AS PTCP_TYP_CD,
		CUST_ID AS CUST_ID,
		UPPER(PTCP_NOTE_IND) AS PTCP_NOTE_IND 
				from SRC_P
            ),
LOGIC_PT as ( SELECT 
		UPPER(TRIM(PTCP_TYP_CD)) AS PTCP_TYP_CD,
		UPPER(TRIM(PTCP_TYP_NM)) AS PTCP_TYP_NM,
		UPPER(PTCP_TYP_VOID_IND) AS PTCP_TYP_VOID_IND 
				from SRC_PT
            ),
LOGIC_C as ( SELECT 
		CUST_ID AS CUST_ID,
		UPPER(TRIM(CUST_TYP_CD)) AS CUST_TYP_CD,
		UPPER(TRIM(CUST_NO)) AS CUST_NO,
		UPPER(CUST_FRGN_CITZ_IND) AS CUST_FRGN_CITZ_IND 
				from SRC_C
            ),
LOGIC_CP as ( SELECT 
		CLM_PTCP_ID AS CLM_PTCP_ID,
		PTCP_ID AS PTCP_ID,
		cast(CLM_PTCP_EFF_DT as DATE) AS CLM_PTCP_EFF_DT,
		cast(CLM_PTCP_END_DT as DATE) AS CLM_PTCP_END_DT,
		UPPER(TRIM(CLM_PTCP_DTL_COMT)) AS CLM_PTCP_DTL_COMT,
		RLT_ID AS RLT_ID,
		UPPER(CLM_PTCP_PRI_IND) AS CLM_PTCP_PRI_IND,
		AUDIT_USER_ID_CREA AS AUDIT_USER_ID_CREA,
		AUDIT_USER_CREA_DTM AS AUDIT_USER_CREA_DTM,
		AUDIT_USER_ID_UPDT AS AUDIT_USER_ID_UPDT,
		AUDIT_USER_UPDT_DTM AS AUDIT_USER_UPDT_DTM,
		UPPER(VOID_IND) AS VOID_IND 
				from SRC_CP
            )
----RENAME LAYER ----
,
RENAME_P as ( SELECT PTCP_ID AS PTCP_ID,AGRE_ID AS AGRE_ID,PTCP_TYP_CD AS PTCP_TYP_CD,CUST_ID AS CUST_ID,PTCP_NOTE_IND AS PTCP_NOTE_IND 
			from      LOGIC_P
        ),
RENAME_PT as ( SELECT 
			
			PTCP_TYP_CD AS PT_PTCP_TYP_CD,PTCP_TYP_NM AS PTCP_TYP_NM,PTCP_TYP_VOID_IND AS PTCP_TYP_VOID_IND 
			from      LOGIC_PT
        ),
RENAME_C as ( SELECT 
			
			CUST_ID AS C_CUST_ID,CUST_TYP_CD AS CUST_TYP_CD,CUST_NO AS CUST_NO,CUST_FRGN_CITZ_IND AS CUST_FRGN_CITZ_IND 
			from      LOGIC_C
        ),
RENAME_CP as ( SELECT CLM_PTCP_ID AS CLM_PTCP_ID,
			
			PTCP_ID AS CP_PTCP_ID,CLM_PTCP_EFF_DT AS CLM_PTCP_EFF_DT,CLM_PTCP_END_DT AS CLM_PTCP_END_DT,CLM_PTCP_DTL_COMT AS CLM_PTCP_DTL_COMT,RLT_ID AS RLT_ID,CLM_PTCP_PRI_IND AS CLM_PTCP_PRI_IND,
			
			AUDIT_USER_ID_CREA AS CP_AUDIT_USER_ID_CREA,
			
			AUDIT_USER_CREA_DTM AS CP_AUDIT_USER_CREA_DTM,
			
			AUDIT_USER_ID_UPDT AS CP_AUDIT_USER_ID_UPDT,
			
			AUDIT_USER_UPDT_DTM AS CP_AUDIT_USER_UPDT_DTM,
			
			VOID_IND AS CP_VOID_IND 
			from      LOGIC_CP
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_P as ( SELECT  * 
			from     RENAME_P 
            
        ),

        FILTER_PT as ( SELECT  * 
			from     RENAME_PT 
            WHERE RENAME_PT.PTCP_TYP_VOID_IND = 'N'
        ),

        FILTER_C as ( SELECT  * 
			from     RENAME_C 
            
        ),

        FILTER_CP as ( SELECT  * 
			from     RENAME_CP 
            
        )
----JOIN LAYER----
,
P as ( SELECT * 
			from  FILTER_P
				INNER JOIN FILTER_PT ON FILTER_P.PTCP_TYP_CD = FILTER_PT.PT_PTCP_TYP_CD 
				INNER JOIN FILTER_C ON FILTER_P.CUST_ID = FILTER_C.C_CUST_ID 
				LEFT JOIN FILTER_CP ON FILTER_P.PTCP_ID = FILTER_CP.CP_PTCP_ID  ),
ETL AS ( SELECT CAST(PTCP_ID AS NUMERIC(31,0)) AS PTCP_ID
,CAST(AGRE_ID AS NUMERIC(31,0)) AS AGRE_ID
,PTCP_TYP_CD
,CAST(CUST_ID AS NUMERIC(31,0)) AS CUST_ID
,PTCP_NOTE_IND
,PT_PTCP_TYP_CD
,PTCP_TYP_NM
,PTCP_TYP_VOID_IND
,CAST(C_CUST_ID AS NUMERIC(31,0)) AS C_CUST_ID
,CUST_TYP_CD
,CUST_NO
,CUST_FRGN_CITZ_IND
,CAST(CLM_PTCP_ID AS NUMERIC(31,0)) AS CLM_PTCP_ID
,CAST(CP_PTCP_ID AS NUMERIC(31,0)) AS CP_PTCP_ID
,CLM_PTCP_EFF_DT
,CLM_PTCP_END_DT
,CLM_PTCP_DTL_COMT
,CAST(RLT_ID AS NUMERIC(31,0)) AS RLT_ID
,CLM_PTCP_PRI_IND
,CAST(CP_AUDIT_USER_ID_CREA AS NUMERIC(31,0)) AS CP_AUDIT_USER_ID_CREA
,CP_AUDIT_USER_CREA_DTM
,CAST(CP_AUDIT_USER_ID_UPDT AS NUMERIC(31,0)) AS CP_AUDIT_USER_ID_UPDT
,CP_AUDIT_USER_UPDT_DTM::TIMESTAMP AS CP_AUDIT_USER_UPDT_DTM
,CP_VOID_IND
 FROM P )
select * from ETL