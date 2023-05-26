// Generic SQL for Mod Industry Stuff

//Step 1:  Create temp
create or replace table RUB1.DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP like RUB1.DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY

//Step 2: Create history temps
//SQL1:
create or replace TABLE RUB1.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP AS
        (
        select 
            CLM_AGRE_ID,
            CLM_NO,
            date_trunc( 'day', CLM_PLCY_RLTNS_EFF_DT ) as CLM_PLCY_RLTNS_EFF_DT, 
            case 
                when date_trunc('day',CLM_PLCY_RLTNS_END_DT) = lead(CLM_PLCY_RLTNS_EFF_DT) over (partition by CLM_AGRE_ID order by CLM_PLCY_RLTNS_EFF_DT, CLM_PLCY_RLTNS_END_DT)
                then DATEADD('DAY',-1,date_trunc('day',CLM_PLCY_RLTNS_END_DT))
                else CLM_PLCY_RLTNS_END_DT 
            end CLM_PLCY_RLTNS_END_DT, 
            CTL_ELEM_SUB_TYP_CD, 
            CRNT_PLCY_IND
        FROM 
            RUB1.DW_REPORT.DW_CLAIM_POLICY_HISTORY)

//SQL2:
SELECT CPRH.AGRE_ID as CLM_AGRE_ID ,CPRH.CLM_PRFL_ANSW_TEXT,'P' AS SOC_MNL_CLS_TYPE,
    date_trunc('day',CPRH.HIST_EFF_DTM) as HIST_EFF_DTM,
    coalesce(date_trunc('day',CPRH.HIST_END_DTM),TO_DATE('2999-12-31','YYYY-MM-DD')) as HIST_END_DTM,
    coalesce(MINO.MOD_INDST_CODE,'11') as MOD_INDST_CODE ,
    coalesce(date_trunc('day',MINO_BGN_DATE),TO_DATE('1900-01-01','YYYY-MM-DD')) as MINO_BGN_DATE,
    coalesce(date_trunc('day',MINO_END_DATE),TO_DATE('2999-12-31','YYYY-MM-DD'))as MINO_END_DATE,
    coalesce(MINC.MOD_INDST_DESC,'UNKNOWN') as MOD_INDST_DESC ,A.CLM_NO
    FROM 
     RUB1.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A 
     INNER JOIN RUB1.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    left outer join RUB1.PCMP.CLAIM_PROFILE_HISTORY CPRH ON CPRH.AGRE_ID = A.CLM_AGRE_ID
      LEFT OUTER JOIN RUB1.BWCODS.TDDMINO MINO
    ON CPRH.CLM_PRFL_ANSW_TEXT = MINO.SOC_MNL_CLS_NO AND MINO.SOC_MNL_CLS_TYPE = 'P' 
    left outer join RUB1.BWCODS.TDDMINC MINC
    ON MINO.MOD_INDST_CODE = MINC.MOD_INDST_CODE
    WHERE CPRH.PRFL_STMT_ID = '6000108' AND CPRH.VOID_IND = 'n'
    AND A.CTL_ELEM_SUB_TYP_CD IN  ('pec','pes')
    and B.CLM_REL_SNPSHT_IND = 'n' 
    AND ((CPRH.HIST_EFF_DTM BETWEEN date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')))
                 OR (date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT) BETWEEN date_trunc('day',CPRH.HIST_EFF_DTM) AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')))) 
    
    AND CPRH.AGRE_ID>=10000000 AND CPRH.AGRE_ID<=20000000000
    ORDER BY CPRH.AGRE_ID,CPRH.HIST_EFF_DTM,MINO_BGN_DATE,MINO_END_DATE

//SQL3:
SELECT CCCH.CLM_AGRE_ID ,CCCH.ADJST_CLASS_CODE,
    CASE WHEN date_trunc('day',B.CLM_OCCR_DTM) < '1996-07-01' THEN 'S' ELSE 'N' END AS SOC_MNL_CLS_TYPE,
    date_trunc('day',CCCH.CLASS_CD_EFF_DT) as HIST_EFF_DTM,
    coalesce(date_trunc('day',CCCH.CLASS_CD_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')) as HIST_END_DTM,
    coalesce(MINO.MOD_INDST_CODE,'11') as MOD_INDST_CODE ,
    coalesce(date_trunc('day',MINO_BGN_DATE),TO_DATE('1900-01-01','YYYY-MM-DD')) as MINO_BGN_DATE,
    coalesce(date_trunc('day',MINO_END_DATE),TO_DATE('2999-12-31','YYYY-MM-DD'))as MINO_END_DATE,
    coalesce(MINC.MOD_INDST_DESC,'UNKNOWN') as MOD_INDST_DESC ,
    A.CLM_NO
    FROM 
     RUB1.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A 
     INNER JOIN RUB1.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    left outer join RUB1.DW_REPORT.DW_CLAIM_CLASS_CODE_HISTORY CCCH ON CCCH.CLM_AGRE_ID = A.CLM_AGRE_ID
    LEFT OUTER JOIN RUB1.BWCODS.TDDMINO MINO
    ON CCCH.ADJST_CLASS_CODE = MINO.SOC_MNL_CLS_NO AND MINO.SOC_MNL_CLS_TYPE =(CASE WHEN date_trunc('day',B.CLM_OCCR_DTM) < '1996-07-01' THEN 'S' ELSE 'N' END )
    left outer join RUB1.BWCODS.TDDMINC MINC
    ON MINO.MOD_INDST_CODE = MINC.MOD_INDST_CODE
    WHERE B.CLM_REL_SNPSHT_IND = 'n'
    AND A.CTL_ELEM_SUB_TYP_CD IN  ('pa')
        AND CCCH.CLM_AGRE_ID>=10000000 AND CCCH.CLM_AGRE_ID<=20000000000
    AND ((CCCH.CLASS_CD_EFF_DT BETWEEN date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')))
          OR (A.CLM_PLCY_RLTNS_EFF_DT BETWEEN date_trunc('day',CCCH.CLASS_CD_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')))) 
    ORDER BY CCCH.CLM_AGRE_ID,CCCH.CLASS_CD_EFF_DT,MINO_BGN_DATE,MINO_END_DATE

//SQL4:
SELECT CCCH.CLM_AGRE_ID,CCCH.ADJST_CLASS_CODE,'S' AS SOC_MNL_CLS_TYPE ,
    date_trunc('day',CCCH.CLASS_CD_EFF_DT) as HIST_EFF_DTM,
    coalesce(date_trunc('day',CCCH.CLASS_CD_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')) as HIST_END_DTM, 
    coalesce(MINO.MOD_INDST_CODE,'11') as MOD_INDST_CODE,
    coalesce(date_trunc('day',MINO_BGN_DATE),TO_DATE('1900-01-01','YYYY-MM-DD')) as MINO_BGN_DATE,
    coalesce(date_trunc('day',MINO_END_DATE),TO_DATE('2999-12-31','YYYY-MM-DD'))as MINO_END_DATE,
    coalesce(MINC.MOD_INDST_DESC,'UNKNOWN') as MOD_INDST_DESC,
    A.CLM_NO
    FROM 
     RUB1.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A 
     INNER JOIN RUB1.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    left outer join RUB1.DW_REPORT.DW_CLAIM_CLASS_CODE_HISTORY CCCH ON CCCH.CLM_AGRE_ID = A.CLM_AGRE_ID
    LEFT OUTER JOIN RUB1.BWCODS.TDDMINO MINO
    ON CCCH.ADJST_CLASS_CODE = MINO.SOC_MNL_CLS_NO AND MINO.SOC_MNL_CLS_TYPE = 'S'
    left outer join RUB1.BWCODS.TDDMINC MINC
    ON MINO.MOD_INDST_CODE = MINC.MOD_INDST_CODE
    WHERE B.CLM_REL_SNPSHT_IND = 'n'
    AND (A.CTL_ELEM_SUB_TYP_CD not IN  ('pec','pes','pa') or A.CTL_ELEM_SUB_TYP_CD is null)
    AND ((CCCH.CLASS_CD_EFF_DT BETWEEN date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')))
          OR (A.CLM_PLCY_RLTNS_EFF_DT BETWEEN date_trunc('day',CCCH.CLASS_CD_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('2999-12-31','YYYY-MM-DD')))) 
    AND CCCH.CLM_AGRE_ID>=10000000 AND CCCH.CLM_AGRE_ID<=20000000000

    ORDER BY CLM_AGRE_ID,CCCH.CLASS_CD_EFF_DT,MINO_BGN_DATE,MINO_END_DATE






// FLAT FILE PROCESSING VERSION
//List tables and remove any
LIST  @~/RUB1/DW_REPORT/DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP;

//RM individual stages
rm @~/RUB1/DW_REPORT/DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP/CLAIM_MOD_INDUSTRY_2000000_2999999.csv.gz;

//Step xx-2:  Put the files into SF
put file://E:\EXTRACTS\svc_automic\EL\prd\snow_etl\RPD1\DW_REPORT\data\2023_03_02AM\CLAIM_MOD_INDUSTRY_0_999999.csv @~/RUB1/DW_REPORT/DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP/ auto_compress=true;

//Step xx-1:  Copy the information from loaded files into TEMP
copy into RUB1.DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP from @~/RUB1/DW_REPORT/DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')   on_error='continue';

//Step xx: When complete, swap TEMP for original table
alter table RUB1.DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY swap with RUB1.DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP
