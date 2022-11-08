

      create or replace  table DEV_EDW.EDW_STAGING.DIM_ADMISSION_SCDALL_STEP2  as
      (----SRC LAYER----
WITH
SCD1 as ( SELECT *    from      STAGING.DSV_ADMISSION )
select * from SCD1
      );
    