use schema PLAYGROUND.E21TRUBIS;


insert into PLAYGROUND.E21TRUBIS.ARSALESMAN select * from PLAYGROUND.E21_RAW.E21TRUBIS_ARSALESMAN;          --232
insert into PLAYGROUND.E21TRUBIS.BRANCH select * from PLAYGROUND.E21_RAW.E21TRUBIS_BRANCH;                  -- 20
insert into PLAYGROUND.E21TRUBIS.BUSAREA_REGION select * from PLAYGROUND.E21_RAW.E21TRUBIS_BUSAREA_REGION;  -- 11
insert into PLAYGROUND.E21TRUBIS.BUSAREA select * from PLAYGROUND.E21_RAW.E21TRUBIS_BUSAREA;                -- 13
insert into PLAYGROUND.E21TRUBIS.REGION select * from PLAYGROUND.E21_RAW.E21TRUBIS_REGION;                  -- 11
insert into PLAYGROUND.E21TRUBIS.GLMOACCTS select * from PLAYGROUND.E21_RAW.E21TRUBIS_GLMOACCTS;            --  7
insert into PLAYGROUND.E21TRUBIS.GLPERMSTR select * from PLAYGROUND.E21_RAW.E21TRUBIS_GLPERMSTR;        --   1800
insert into PLAYGROUND.E21TRUBIS.GLCHART select * from PLAYGROUND.E21_RAW.E21TRUBIS_GLCHART;            --1178782
insert into PLAYGROUND.E21TRUBIS.INVHEAD select * from PLAYGROUND.E21_RAW.E21TRUBIS_INVHEAD;            --3164938

insert into PLAYGROUND.E21TRUBIS.ORDHEAD select * from PLAYGROUND.E21_RAW.E21TRUBIS_ORDHEAD;            --5071025

select count(*), count(hash(*)) from E21TRUBIS.ARSALESMAN;      --232
select count(*), count(hash(*)) from E21TRUBIS.BRANCH;          -- 20
select count(*), count(hash(*)) from E21TRUBIS.BUSAREA_REGION;  -- 11
select count(*), count(hash(*)) from E21TRUBIS.BUSAREA;         -- 13
select count(*), count(hash(*)) from E21TRUBIS.REGION;          -- 11
select count(*), count(hash(*)) from E21TRUBIS.GLMOACCTS;       --  7
select count(*), count(hash(*)) from E21TRUBIS.GLPERMSTR;   --    1800
select count(*), count(hash(*)) from E21TRUBIS.GLCHART;     -- 1178782
select count(*), count(hash(*)) from E21TRUBIS.INVHEAD;     -- 3164938
select count(*), count(hash(*)) from E21TRUBIS.ORDHEAD;     -- 5071025