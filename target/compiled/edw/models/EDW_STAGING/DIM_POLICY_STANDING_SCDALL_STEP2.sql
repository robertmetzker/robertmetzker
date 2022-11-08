----SRC LAYER----
WITH
SCD1 as ( SELECT *
	from      STAGING.DSV_POLICY_STANDING )
select * from SCD1