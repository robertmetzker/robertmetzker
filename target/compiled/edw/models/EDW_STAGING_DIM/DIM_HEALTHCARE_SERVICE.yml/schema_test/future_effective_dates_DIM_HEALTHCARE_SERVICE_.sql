

with future_dates as (
    SELECT * 
    FROM  EDW_STAGING_DIM.DIM_HEALTHCARE_SERVICE
    where 
    record_effective_date> current_date + 1
    or record_end_date> current_date
    or record_effective_date > record_end_date
)
    select count(*) from future_dates
    
