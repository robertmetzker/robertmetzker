



with all_values as(
  select distinct CTL_ELEM_TYP_CD as value_field
  from STAGING.STG_POLICY_CONTROL_ELEMENT 
  
),

validation_errors as (
  select distinct value_field
  from all_values 
    where value_field not in (
        'ADJ_METH','AUDT_TYP','BILL_PLN','BILL_TYP','PYMT_PLN','RPT_FREQ','EMP_LS_PLCY_TYP','PLCY_TYP'
        )
)

select count(*)
from validation_errors

