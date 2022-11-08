
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_NETWORK
where NETWORK_HKEY is null


