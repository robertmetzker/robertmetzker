

      create or replace  table DEV_EDW.STAGING.STG_CUSTOMER_BLOCK  as
      (----SRC LAYER----
WITH
SRC_CBRB as ( SELECT *     from      STAGING.STG_CUSTOMER_BLOCK_SOURCE ),

--SRC_CBRB as ( SELECT *     from     STG_CUSTOMER_BLOCK_SOURCE), 
-----------LOGIC LAYER-----------
SRC_ACP as (
select *, 
   case when NVL(blk_end_dt,CURRENT_DATE) >= current_date then 'Y' else NULL end as future_end,
   CASE WHEN BLK_TYP_CD='ALERT' THEN 'A' ELSE 'D' END AS TYP_CD_GRP,
  min (case when future_end = 'Y' THEN blk_eff_dt ELSE NULL END) OVER(PARTITION BY CUST_ID,TYP_CD_GRP ) as first_eff_dt,
ROW_NUMBER() OVER(PARTITION BY CUST_ID, TYP_CD_GRP, blk_end_dt ORDER BY BLK_EFF_DT, blk_end_dt NULLS FIRST) as seq
from SRC_CBRB
where void_ind = 'N'
and BLK_TYP_CD in ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK','ALERT')
),
-----To Delete Ovelap Dates based on mapping rule------
DEL_SCB AS (select *,
case when blk_end_dt is not null and blk_eff_dt < lag(blk_end_dt) over (partition by cust_id,TYP_CD_GRP order by blk_eff_dt,blk_end_dt) then 'D'
            when blk_end_dt < lag(blk_end_dt) over (partition by cust_id,TYP_CD_GRP order by blk_eff_dt,blk_end_dt) then 'D' else 'DD' end as indicator            
from SRC_ACP 
 ),
DEL1_SCB AS (SELECT * FROM DEL_SCB WHERE INDICATOR != 'D') ,

----------LOGIC LAYER-----------
FIX_SCB as (
select *,
lead(blk_eff_dt,1) over (partition by cust_id,TYP_CD_GRP order by blk_eff_dt, blk_end_dt NULLS FIRST) as next_eff,
lag(blk_end_dt,1) over (partition by cust_id,TYP_CD_GRP order by blk_eff_dt, blk_end_dt NULLS FIRST) as prev_end,
(case when SEQ=1 AND dateadd(day,1, nvl(blk_end_dt,'2099-12-31')) < next_eff then 'Y' else null end ) as FIX_ME,
CASE WHEN BLK_EFF_DT <= NVL(FIRST_EFF_DT,CURRENT_DATE) THEN 'N' ELSE 'Y' END AS IGNORE_ME
from DEL1_SCB
where seq = 1
),
UNION_SCB AS (select 
     NULL as    CUST_BLK_ROLE_BLK_ID    
,                                  CUST_ID     
,             NULL as              CBRB_BLK_ID    
,             NULL as              CUST_ROLE_TYP_CD      
,             NULL as              CBRB_AUDIT_USER_ID_CREA     
,             NULL as              CBRB_AUDIT_USER_CREA_DTM 
,             NULL as              CBRB_AUDIT_USER_ID_UPDT    
,             NULL as              CBRB_AUDIT_USER_UPDT_DTM 
,             NULL as              CUST_BLK_ROLE_BLK_VOID_IND             
,             NULL as              BLK_TYP_CD    
,TYP_CD_GRP as DRVD_BLK_TYP_CD
,dateadd(day, 1, blk_end_dt) as blk_eff_dt
,dateadd(day,-1, next_eff) as blk_end_dt             
,             NULL as              B_AUDIT_USER_ID_CREA            
,             NULL as              B_AUDIT_USER_CREA_DTM       
,             NULL as              B_AUDIT_USER_ID_UPDT           
,             NULL as              B_AUDIT_USER_UPDT_DTM       
,             'N'  as    VOID_IND         
,             NULL as              B_BLK_ID            
,             NULL as              BLK_TYP_NM     
,             NULL as              APP_TYP_CD      
,             NULL as              PLCY_BLK_HDR_DSPLY_IND        
,             NULL as              CLM_BLK_HDR_DSPLY_IND         
,             NULL as              BLK_TYP_VOID_IND       
,             NULL as              BT_BLK_TYP_CD
,  'N'   AS    DOCUMENT_BLK_IND
,  'N'   AS    THREATENING_BEHAVIOR_BLOCK_IND           
from FIX_SCB
where fix_me = 'Y' AND IGNORE_ME = 'N' 
UNION
select 
  DEL1_SCB.CUST_BLK_ROLE_BLK_ID      
,DEL1_SCB.CUST_ID      
,DEL1_SCB.CBRB_BLK_ID            
,DEL1_SCB.CUST_ROLE_TYP_CD             
,DEL1_SCB.CBRB_AUDIT_USER_ID_CREA             
,DEL1_SCB.CBRB_AUDIT_USER_CREA_DTM        
,DEL1_SCB.CBRB_AUDIT_USER_ID_UPDT            
,DEL1_SCB.CBRB_AUDIT_USER_UPDT_DTM       
,DEL1_SCB.CUST_BLK_ROLE_BLK_VOID_IND      
,DEL1_SCB.BLK_TYP_CD  
,DEL1_SCB.TYP_CD_GRP
,DEL1_SCB.BLK_EFF_DT 
,DEL1_SCB.BLK_END_DT             
,DEL1_SCB.B_AUDIT_USER_ID_CREA     
,DEL1_SCB.B_AUDIT_USER_CREA_DTM 
,DEL1_SCB.B_AUDIT_USER_ID_UPDT     
,DEL1_SCB.B_AUDIT_USER_UPDT_DTM 
,DEL1_SCB.VOID_IND   
,DEL1_SCB.B_BLK_ID     
,DEL1_SCB.BLK_TYP_NM            
,DEL1_SCB.APP_TYP_CD             
,DEL1_SCB.PLCY_BLK_HDR_DSPLY_IND 
,DEL1_SCB.CLM_BLK_HDR_DSPLY_IND  
,DEL1_SCB.BLK_TYP_VOID_IND 
,DEL1_SCB.BT_BLK_TYP_CD
,iff(DEL1_SCB.BLK_TYP_CD IN ('ALL_DOCM_BLK', 'CLM_DOCM_BLK', 'PLCY_DOCM_BLK'), 'Y', 'N' ) DOCUMENT_BLK_IND
,iff(DEL1_SCB.BLK_TYP_CD IN ('ALERT'), 'Y', 'N' ) THREATENING_BEHAVIOR_BLOCK_IND            
 from DEL1_SCB
INNER JOIN FIX_SCB ON (DEL1_SCB.CUST_ID=FIX_SCB.CUST_ID AND DEL1_SCB.BLK_EFF_DT=FIX_SCB.BLK_EFF_DT AND FIX_SCB.IGNORE_ME='N' ))
SELECT * FROM UNION_SCB
      );
    