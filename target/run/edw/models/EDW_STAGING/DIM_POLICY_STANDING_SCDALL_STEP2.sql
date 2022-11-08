

      create or replace  table DEV_EDW.EDW_STAGING.DIM_POLICY_STANDING_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT *
	from      STAGING.DSV_POLICY_STANDING )
select * from SCD1
      );
    