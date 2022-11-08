



with all_values as(
  select distinct PAYMENT_PLAN_TYPE_CODE as value_field
  from EDW_STAGING_DIM.DIM_YEAR_CONTROL_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        '1_PYMT','1_PYMT_PEC','12_PYMT','12_PYMT_PEC','2_PYMT','2_PYMT_PEC','4_PYMT','4_PYMT_PEC','6_PYMT','6_PYMT_PEC','DFR_12_PYMT_PEC','NB_AUG_12_PYMT','NB_AUG_2_PYMT','NB_AUG_4_PYMT','NB_AUG_6_PYMT','NB_DEC_12_PYMT','NB_DEC_4_PYMT','NB_DEC_6_PYMT','NB_FEB_12_PYMT','NB_FEB_6_PYMT','NB_JAN_12_PYMT','NB_JAN_4_PYMT','NB_JAN_6_PYMT','NB_JUL_12_PYMT','NB_JUL_6_PYMT','NB_MAR_12_PYMT','NB_NOV_12_PYMT','NB_NOV_4_PYMT','NB_NOV_6_PYMT','NB_OCT_12_PYMT','NB_OCT_2_PYMT','NB_OCT_4_PYMT','NB_OCT_6_PYMT','NB_SEP_12_PYMT','NB_SEP_2_PYMT','NB_SEP_4_PYMT','NB_SEP_6_PYMT'
        )
)

select count(*)
from validation_errors

