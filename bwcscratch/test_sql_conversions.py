import re
from datetime import datetime

# qsql =  "select count(distinct cb.CUST_ID) from PCMP.CUSTOMER_BLOCK_ROLE_BLOCK cbinner join PCMP.BLOCK blk on cb.BLK_ID = blk.BLK_IDwhere BLK_EFF_DT <= sysdate and (BLK_END_DT > (select max(DW_LOAD_DATE) from DW_REPORT.DW_L1_DATA_LOAD_AUDIT where DW_TABLE_NAME = 'DW_CUSTOMER_DEMOGRAPHICS') or BLK_END_DT is null) and BLK_TYP_CD in ('all_docm_blk', 'plcy_docm_blk', 'clm_docm_blk') and cb.CUST_BLK_ROLE_BLK_VOID_IND = 'n' and blk.VOID_IND = 'n' "
# qsql =  "select count(distinct p.CUST_ID) from PCMP.CLAIM cinner join PCMP.PARTICIPATION p on c.AGRE_ID = p.AGRE_ID and p.PTCP_TYP_CD = 'clmt' and c.CLM_REL_SNPSHT_IND = 'n'inner join PCMP.CLAIM_PARTICIPATION cp on p.PTCP_ID = cp.PTCP_ID and cp.VOID_IND = 'n' and cp.CLM_PTCP_PRI_IND = 'y' "
# qsql =  "select count(distinct h.plcy_prd_id) from dw_report.DW_POLICY_STATUS_REASON_HISTORY h, pcmp.POLICY_PERIOD ppwhere h.PLCY_PRD_ID=pp.PLCY_PRD_ID and pp.VOID_IND='n' "
# qsql =  "select distinct CLM_NO, AGRE_ID, ICDC_MPD_CODE from DW_REPORT.DW_CLAIM_INJURY_HISTORYwhere ICD_STS_TYP_NM in ( 'Accept / Appeal', 'Accepted','Pending','Hearing','SubAgg - Accept/Appeal', 'SubAgg - Payable', 'SubAgg - Not Payable', 'SubAgg - Pending Abatement')AND trunc(HIST_EFF_DTM) <= trunc(coalesce(HIST_END_DTM, '9999-12-31'))and VOID_IND = 'n'MINUSSELECT DISTINCT CLM_NO, AGRE_ID, ICDC_MPD_CODE FROM DW_REPORT.DW_CLAIM_BILAT_HISTORY"
qsql = " SELECT  COUNT(*)  FROM  ( SELECT  DISTINCT A.CASE_ID, B.CASE_PRFL_SEQ_NO  FROM  PCMP.CASES A JOIN  PCMP.CASE_PROFILE B ON A.CASE_ID = B.CASE_ID WHERE  A.VOID_IND = 'n'  AND  A.APP_CNTX_TYP_CD = 'claim'  AND  A.CASE_CTG_TYP_CD = 'lgl'  AND  A.CASE_TYP_CD = 'othr_lgl' andB.VOID_IND = 'n'  AND  B.CASE_PRFL_CTG_TYP_CD like 'req%' ) ABC "

# qsql = '''select count(distinct cb.CUST_ID) from PCMP.CUSTOMER_BLOCK_ROLE_BLOCK cb
# inner join PCMP.BLOCK blk on cb.BLK_ID = blk.BLK_ID
# where BLK_EFF_DT <= SYSDATE 
# and (BLK_END_DT > (select max(DW_LOAD_DATE) from DW_REPORT.DW_L1_DATA_LOAD_AUDITwhere DW_TABLE_NAME = 'DW_CUSTOMER_DEMOGRAPHICS') 
# or BLK_END_DT is null) and BLK_TYP_CD in ('all_docm_blk', 'plcy_docm_blk', 'clm_docm_blk') 
# and cb.CUST_BLK_ROLE_BLK_VOID_IND = 'n' and BTRIM(blk.VOID_IND) = 'n' '''

print(f'---RAW---\n{qsql}\n----')
#TRUNC should beecome TRUNC('DAY', xxxx) 
# position 44 unexpected ''+%*%-%`%,%.%%[[:space:]]+ %''

problem_statements = [ 'FROM ','AND','UNION','MINUS','JOIN ','INNER ','OUTER ','SELECT ','WHERE ' ]

for problem in problem_statements:
    if problem.lower() in qsql.lower():
        problem_LC = problem.lower()
        problem_UC = problem.upper()
        qsql = re.sub(rf'{problem_LC}|{problem_UC}',rf' {problem_UC} ', qsql )

if 'sysdate' in qsql.lower():
    sf_sql = re.sub('sysdate|SYSDATE','current_date', qsql)
else: 
    sf_sql = qsql

sf_sql = re.sub('BTRIM|btrim','TRIM', sf_sql )
sf_sql = re.sub('trunc\(|TRUNC\(',"TRUNC('DAY',", sf_sql )


now = datetime.now()
datestring = now.strftime("%Y%m%d")

# print( datestring  )
print( qsql )
print ( '^^^^^^^^^^^^^^^^^^')
print( sf_sql )

