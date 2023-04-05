SELECT PLCY_NMBR, BUS_SEQ_NMBR, PRD_BGNG_DATE, PRD_ENDNG_DATE, ANSWR_CRT_DTTM, QSTN_CODE, ANSWR_TEXT, SRVC_OFRNG_CODE, CRT_PRGRM_NAME, CRT_USER_CODE, DCTVT_PRGRM_NAME, DCTVT_USER_CODE, DCTVT_DTTM 
				FROM BWCCMN.EMPLR_ANSWER;
            python /usr/local/etl/run/run_sql2csv.py  --fname /cifs/mswg9/groups/IT/ETL/snowflake/extracts/EMPLR_ANSWER.csv --operation vsql --t_schema BASE  --t_environment uat2 --tgtdb vertica --sql "SELECT PLCY_NMBR, BUS_SEQ_NMBR, PRD_BGNG_DATE, PRD_ENDNG_DATE, ANSWR_CRT_DTTM, QSTN_CODE, ANSWR_TEXT, SRVC_OFRNG_CODE, CRT_PRGRM_NAME, CRT_USER_CODE, DCTVT_PRGRM_NAME, DCTVT_USER_CODE, DCTVT_DTTM from  BWCCMN.EMPLR_ANSWER"

        
SELECT QSTN_TEXT, QSTN_EXPLN_TEXT 
				FROM BWCCMN.EMPLR_QUESTION;
            python /usr/local/etl/run/run_sql2csv.py  --fname /cifs/mswg9/groups/IT/ETL/snowflake/extracts/EMPLR_QUESTION.csv --operation vsql --t_schema BASE  --t_environment uat2 --tgtdb vertica --sql "SELECT QSTN_TEXT, QSTN_EXPLN_TEXT from  BWCCMN.EMPLR_QUESTION"

        
SELECT CODE_DESC 
				FROM BWCCMN.TEGSVOC;
            python /usr/local/etl/run/run_sql2csv.py  --fname /cifs/mswg9/groups/IT/ETL/snowflake/extracts/TEGSVOC.csv --operation vsql --t_schema BASE  --t_environment uat2 --tgtdb vertica --sql "SELECT CODE_DESC from  BWCCMN.TEGSVOC"

        