staged_file_prefix='BWCSTAGE'
staged_target_prefix='BWCTABLE'
staged_files={
    f'{staged_file_prefix}_DW_APC_INFO': {
        'target':'TCDAPCI',
        'strategy':'bulk',
        'columns':{
            'APCI_CRNT_FLAG': {'operation':'current_ind','based_on':'APC_END_DATE',},
            'APC_AMT': {'operation':'amount_field',},
        },               
    },
    f'{staged_file_prefix}_DW_API_SUM': {
        'target':'TSPMAPI',
        'strategy':'bulk',
        ##MCO_NAME field is not trimmed in BWCODS, but is being trimmed by new process
                
    },
    f'{staged_file_prefix}_DW_NETWORK': {
        'target':'TDFNTWD',
        'strategy':'bulk',
         'columns': {
             'FDRL_TAX_ID': {'operation':'format_fin'},
             'SRVC_PHONE_NO': {'operation':'format_phone_num'},
             'MCO_CRTFC_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDMCCS','based_on_column':'MCO_CRTFC_STS','source_column':'MCO_CRTFC_STS', 'join_column':'MCO_CRTFC_STS_CODE','filter':''},
         },
    },
    f'{staged_file_prefix}_DW_NETWORK_PAYMENTS': {
        'target':'TDFMCPY',
        'strategy':'bulk',
        'columns':{
            'MCO_PAID_AMT': {'operation':'amount_field',},
            'BATCH_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDBTST','based_on_column':'BATCH_STS','source_column':'BATCH_STS','join_column':'BATCH_STS_CODE','filter':''},
            ##Count mismatch due to new/updated records in BWOCDS table vs CAM extract. Could be incremental or just bulk with new data?
        },
    },
    f'{staged_file_prefix}_DW_HDR_COND': {
        'target':'TDFCCND',
        'strategy':'to_do_bulk',
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 1,118 vs BWCODS - 1,423,028
    },
    f'{staged_file_prefix}_DW_HDR_HSPTL_PROC': {
        'target':'TDFPRCC',
        'strategy':'to_do_bulk',
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 125 vs BWCODS - 1,347,198
    },
    f'{staged_file_prefix}_DW_HDR_OCCSPAN': {
        'target':'TDFCOCR',
        'strategy':'to_do_bulk',
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 5,468 vs BWCODS - 6,939,913
    },
    f'{staged_file_prefix}_DW_HDR_VALUE': {
        'target':'TDFCVAL',
        'strategy':'to_do_bulk',
        'columns':{
            'HSPTL_VALUE_AMT': {'operation':'amount_field',},
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 1,070 vs BWCODS - 3,117,032
        },
    },
    f'{staged_file_prefix}_DW_ICD_PROC_CODE': {
        'target':'TCDHPRC',
        'strategy':'bulk',
    },
    f'{staged_file_prefix}_DW_INVOICE_LINE_APC': {
        'target':'TDFCHAP',
        'strategy':'to_do_bulk',  
        'columns':{
            'HSPTL_CTGRY_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDOHCC','based_on_column':'HSPTL_CTGRY_CODE','source_column':'HSPTL_CTGRY_CODE','join_column':'HSPTL_CTGRY_CODE','filter':''},
            'OPPS_BASE_AMT': {'operation':'amount_field',},
            'OTLR_AMT': {'operation':'amount_field',},
            'RURAL_ADD_ON_AMT': {'operation':'amount_field',},
            'HH_ADD_ON_AMT': {'operation':'amount_field',},
            'MDCR_RMBRS_AMT': {'operation':'amount_field',},
            'BWC_MARK_UP_AMT': {'operation':'amount_field',},
            'CALC_TTL_AMT': {'operation':'amount_field',},
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 11,674 vs BWCODS - 8,592,108
        }
    },
    f'{staged_file_prefix}_DW_DRG_CODE': {
        'target':'TCDDRGR',
        'strategy':'bulk',
        'columns':{ 
            'TRIM_POINT_AMT': {'operation':'amount_field',},
            'CRNT_DRG_CODE_FLAG': {'operation':'current_ind','based_on':'DRG_CODE_END_DATE',},
            'UPDT_EMPLY_ID_NO': {'operation':'format_emp_id',},
            'CRT_EMPLY_ID_NO': {'operation':'format_emp_id',},
        },
    },
    f'{staged_file_prefix}_DW_NETWORK_COUNTY': {
        'target':'THFMCTY',
        'strategy':'bulk',
        'columns':{
            'RLTNS_END_DATE' : {'operation' :'high_date'},
            'CRNT_RLTNS_FLAG': {'operation':'current_ind','based_on':'RLTNS_END_DATE',},
            'UPDT_EMPLY_ID_NO': {'operation':'format_emp_id',},
            ##only difference is CNTY_NAME:'BWC COUNTY' and CNTY_CODE:'98' are populated for NTWRK_ID_NO in (-00000000001 thru -00000000009) in cam extract 
        },
    },
    f'{staged_file_prefix}_DW_POLICY_NETWORK': {
        'target':'THFMEMP',
        'strategy':'to_do_bulk', 
        'columns':{
            'RLTNS_END_DATE' : {'operation' :'high_date'},
            'RLTNS_BGN_TMSTM': {'operation':'derive_timestamp','based_on':'RLTNS_BGN_DATE',},
            'RLTNS_END_TMSTM': {'operation':'derive_timestamp','based_on':'RLTNS_END_DATE',},
            'CRNT_RLTNS_FLAG': {'operation':'current_ind','based_on':'RLTNS_END_DATE',},
            'SLCTN_PHONE_NO': {'operation':'format_phone_num'},
            'UPDT_EMPLY_ID_NO': {'operation':'format_emp_id',},
            'RLTNS_SRC_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDRLSR','based_on_column':'RLTNS_SRC_TEXT','source_column':'RLTNS_SRC_TEXT','join_column':'RLTNS_SRC_CODE','filter':''},
            'DATA_SRC_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDDTSR','based_on_column':'DATA_SRC_TEXT','source_column':'DATA_SRC_TEXT','join_column':'DATA_SRC_IND','filter':''},
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 763 vs BWCODS - 3,592,211
        },
    },
    f'{staged_file_prefix}_DW_INVOICE_LINE_DIST': { 
        'target':'TDFCLIP',
        'strategy':'to_do_bulk',
        'columns':{
            'PYMNT_BILL_NO': {'operation':'rename_column','based_on':'CARE_EOB_ID_NO',},
            'PYMNT_CODE_AMT': {'operation':'amount_field',},
            'ACNTB_CODE' : {'operation':'substring_column', 'based_on':'CARE_EOB_ID_NO','start':1, 'length':2, 'dtype':''},
            'FUND_TYPE' : {'operation':'substring_column', 'based_on':'CARE_EOB_ID_NO','start':3, 'length':1, 'dtype':''},
            'CVRG_TYPE' : {'operation':'substring_column', 'based_on':'CARE_EOB_ID_NO','start':4, 'length':1, 'dtype':''},
            'ACDNT_TYPE' : {'operation':'substring_column', 'based_on':'CARE_EOB_ID_NO','start':5, 'length':1, 'dtype':''},
            'BILL_TYPE_F2' : {'operation':'substring_column', 'based_on':'CARE_EOB_ID_NO','start':6, 'length':2, 'dtype':''},
            'BILL_TYPE_L3' : {'operation':'substring_column', 'based_on':'CARE_EOB_ID_NO','start':8, 'length':3, 'dtype':''},
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 104,210 vs BWCODS - 220,714,624
        },
    },
    f'{staged_file_prefix}_DW_INVOICE_LINE_MOD': {
        'target':'TDFCLPM',
        'strategy':'to_do_bulk',
        'columns':{
            #not sure where the below columns are sourced, so returning them as null for now
            'PRCD_MDFR_DSG_CODE': {'operation':'null_column'},
            'PRCD_MDFR_DSG_DESC': {'operation':'null_column'},
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 48,140 vs BWCODS - 26,954,524
        },
    },
    f'{staged_file_prefix}_DW_INV_DRG_COMP': {
        'target':'TDFCDRG',
        'strategy':'to_do_bulk',  
        'columns':{
            'DRG_TTL_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '01'},
            'LOS_AVG_DAYS_CNT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '02'},
            'LOS_GMTRC_DAYS_CNT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '03'},
            'DRG_WGHT_NMBR': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '04'},
            'OTLR_CTF_DAYS_CNT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '05'},
            'LOS_OTLR_DAYS_CNT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '06'},
            'LOS_PPS_DAYS_CNT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '07'},
            'OPRTG_OTLR_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '08'},
            'OPRTG_IME_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '09'},
            'OPRTG_DSH_ADJ_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '10'},
            'PASS_THRU_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '11'},
            'CPTL_TTL_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '12'},
            'CPTL_HSPTL_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '13'},
            'CPTL_FDRL_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '14'},
            'CPTL_OTLR_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '15'},
            'CPTL_OLD_HOLD_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '16'},
            'CPTL_DSH_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '17'},
            'CPTL_IME_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '18'},
            'CPTL_EXCPT_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '19'},
            'OPRTG_FDRL_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '20'},
            'NEW_TECH_PYMNT_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '21'},
            'READM_DSCNT_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '22'},
            'VALUE_BASE_PRC_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '23'},
            'HAC_RDCTN_AMT': {'operation':'pivot', 'based_on':'COMP_VALUE', 'decode_column': 'SEQUENCE_NUMBER', 'decode_value': '24'},
        },
        'group_by': ['INVC_NO','DW_CNTRL_DATE','CARE_HDR_ID_NO'],
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 30 vs BWCODS - 53,565
    },
    f'{staged_file_prefix}_DW_DIAG': {
        'target':'TDFCICD',
        'strategy':'to_do_bulk',
        'columns':{
            #not sure where to source ICDV_CODE info, so returning them as null for now
            'ICDV_CODE': {'operation':'null_column'},
            ###still many null PRSNT_ADMSN_TEXT values due to join--missing codes 'W','D','1' in TDFCICD table--missing spaces in TCDDPAM table
            'PRSNT_ADMSN_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDDPAM','based_on_column':'PRSNT_ADMSN_TEXT','source_column':'PRSNT_ADMSN_TEXT','join_column':'PRSNT_ADMSN_CODE','filter':''},
        ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 79,092 vs BWCODS - 98,873,217
        },
        
    },
    f'{staged_file_prefix}_DW_POLICY': {
        'target':'TDFPAPV',
        'strategy':'to_do_bulk',
        'columns':{
            #need to find out where to source PRM_AMT values
            'PRM_AMT': {'operation':'null_column',},
            'APV_AMT': {'operation':'amount_field',},
         ##INCREMENTAL?
        ##data comparison looks good, but large count difference: cam extract - 6,388 vs BWCODS - 1,266,084
        },        
    },
    f'{staged_file_prefix}_DW_EDT': {
        'target':'TCDEDTC',
        'strategy':'bulk',
        'columns':{
            'EDIT_EOB_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDEDSC','based_on_column':'CARE_EDIT_EOB_STS','source_column':'CARE_EDIT_EOB_STS','join_column':'CARE_EDIT_EOB_STS','filter':''},
            'UPDT_EMPLY_ID_NO': {'operation':'format_emp_id',},
        },
    },
    f'{staged_file_prefix}_DW_EOB': {
        'target':'TCDEOBC',
        'strategy':'bulk',
        'columns':{
            'CARE_EDIT_EOB_STS': {'operation':'rename_column','based_on':'EDIT_EOB_STS',},
            'EDIT_EOB_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDEDSC','based_on_column':'EDIT_EOB_STS','source_column':'EDIT_EOB_STS','join_column':'CARE_EDIT_EOB_STS','filter':''},
            'UPDT_EMPLY_ID_NO': {'operation':'format_emp_id',},
            ##added nullif({value},'') to join conditions--BWCODS shows blanks and BWCODS_NEW shows NULL for EDIT_EOB_STS
        },        
    },
    f'{staged_file_prefix}_DW_INVOICE_HEADER': {
        'target':'TDFCDBC',
        'strategy':'to_do_bulk',
        'columns':{
            ##Working through columns--may need to reach out for mapping rules
            'CARE_BATCH_NO': {'operation':'substring_column', 'based_on':'INVC_NO','start':12, 'length':4, 'dtype':''},
            'CARE_BATCH_SQNC_NO': {'operation':'substring_column', 'based_on':'INVC_NO','start':17, 'length':2, 'dtype':''},
            'CARE_EXTNS_NO': {'operation':'substring_column', 'based_on':'INVC_NO','start':20, 'length':2, 'dtype':''},
            'CLAIM_KEY': {'operation':'null_column',},#setting to null for now--need logic
            'PAYTO_PRVDR_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPRTH','based_on_column':'PAYTO_PRVDR_CODE','source_column':'PRVDR_TYPE','join_column':'PRVDR_TYPE_CODE','filter':'''CRNT_CODE_FLAG='Y' '''},
            'SRVCN_PRVDR_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPRTH','based_on_column':'SRVCN_PRVDR_CODE','source_column':'PRVDR_TYPE','join_column':'PRVDR_TYPE_CODE','filter':'''CRNT_CODE_FLAG='Y' '''},
            'RFRNG_PRVDR_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPRTH','based_on_column':'RFRNG_PRVDR_CODE','source_column':'PRVDR_TYPE','join_column':'PRVDR_TYPE_CODE','filter':'''CRNT_CODE_FLAG='Y' '''},
            'BILL_CTGRY_TEXT': {'operation':'null_column'},#setting to null for now--need logic
            'BILL_PRCSN_IND': {'operation':'null_column'},###mapping says "NO DATA"
            'BILL_SRC_TYPE': {'operation':'hardcode_column','''value''':''' 'CARE' '''},
            'RISK_TYPE': {'operation':'null_column'},#setting to null for now--need logic
            'BSNS_SQNC_NO': {'operation':'substring_column', 'based_on':'PLCY_NO','start':9, 'length':3, 'dtype':'NUMBER'},
            'BWC_RCPT_DATE': {'operation':'add_date', 'starting_date':'MCO_RCPT_DATE','days_added':'MCO_BWC_CDAYS_LAG'},
            'BWC_RCPT_MONTH': {'operation':'null_column'},
            'BWC_RCPT_YEAR': {'operation':'null_column'},
            #'BWC_RCPT_MONTH': {'operation':'substring_column', 'based_on':'BWC_RCPT_DATE','start':6, 'length':2},
            #'BWC_RCPT_YEAR': {'operation':'substring_column', 'based_on':'BWC_RCPT_DATE','start':1, 'length':4},
            'ADMTN_ICD_CODE': {'operation':'null_column'},#mapping says Hospital Bill Admit Diag 88 or 15
            'PRNCP_ICD_CODE': {'operation':'null_column'},#mapping says Hospital Bill Principal Diag 89 or 16
            'PTNT_STS_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPTSC','based_on_column':'CARE_PTNT_STS_CODE','source_column':'PTNT_STS_TEXT','join_column':'CARE_PTNT_STS_CODE','filter':''},
            'DRG_MTCH_FLAG': {'operation':'null_column'},#mapping says if DRG Submitted by Provider = DRG Calculated by BWC, then 'Y' both spaces then (blank) else 'N'
            'LNGTH_SRVC_LAG': {'operation':'srvc_lag', 'fdos':'FIRST_SRVC_DATE', 'ldos': 'LAST_SRVC_DATE'},
            'MDC_INVC_TYPE_CODE': {'operation':'rename_column','based_on':'INVC_TYPE_CODE',},
            'MDC_INVC_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDMINV','based_on_column':'INVC_TYPE_CODE','source_column':'INVC_TYPE','join_column':'INVC_TYPE_CODE','filter':''},
            'PYMNT_RMBRS_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPRMI','based_on_column':'PYMNT_RMBRS_IND','source_column':'PYMNT_RMBRS_TEXT','join_column':'PYMNT_RMBRS_IND','filter':''},
            'PBM_ORGN_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPBOC','based_on_column':'PBM_ORGN_CODE','source_column':'PBM_ORGN_TEXT','join_column':'PBM_ORGN_CODE','filter':''},
            'ADMTN_ICDV_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'PRNCP_ICDV_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'TOTAL_PRVDR_AMT': {'operation':'amount_field',},
            'TOTAL_NTWK_AMT': {'operation':'amount_field',},
            'TOTAL_BWC_FEE_AMT': {'operation':'amount_field',},
            'TOTAL_PAID_AMT': {'operation':'amount_field',},
            'TOTAL_BWC_ALWD_AMT': {'operation':'amount_field',},
            'TOTAL_INTRS_PD_AMT': {'operation':'amount_field',},
            'TOTAL_RMBRS_AMT': {'operation':'amount_field',},
            'TOTAL_RCVRY_AMT': {'operation':'amount_field',},
            'MDCL_EDCTN_AMT': {'operation':'amount_field',},
            'TOTAL_PHRMC_PD_AMT': {'operation':'amount_field',},
            'TOTAL_PBM_ADMN_AMT': {'operation':'amount_field',},
     
        },
    },
     f'{staged_file_prefix}_DW_INVOICE_LINE': {
         'target':'TDFCLIC',
         'strategy':'to_do_bulk',
         'columns':{
            'CLAIM_KEY': {'operation':'null_column',},##need to identify logic
            'LINE_ITEM_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDBLST','based_on_column':'LINE_ITEM_STS','source_column':'BILL_STS','join_column':'CARE_BILL_STS_CODE','filter':''},
            'PAYTO_PRVDR_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPRTH','based_on_column':'PAYTO_PRVDR_CODE','source_column':'PRVDR_TYPE','join_column':'PRVDR_TYPE_CODE','filter':'''CRNT_CODE_FLAG='Y' '''},
            'SRVCN_PRVDR_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPRTH','based_on_column':'SRVCN_PRVDR_CODE','source_column':'PRVDR_TYPE','join_column':'PRVDR_TYPE_CODE','filter':'''CRNT_CODE_FLAG='Y' '''},
            'BILL_CTGRY_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDSPBC','based_on_column':'SRVCN_PRVDR_CODE','source_column':'BILL_CTGRY_TEXT','join_column':'SRVCN_PRVDR_CODE','filter':''},
            'BILL_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDBLST','based_on_column':'BILL_STS','source_column':'BILL_STS','join_column':'BILL_STS_CODE','filter':''},
            'BILL_SRC_TYPE': {'operation':'hardcode_column','''value''':''' 'CARE' '''},
            'ICD_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'RVN_CNTR_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'PLACE_SRVC_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPLCS','based_on_column':'PLC_SRVC_EDI_CODE','source_column':'PLACE_SRVC_TEXT','join_column':'PLC_SRVC_EDI_CODE','filter':'''CRNT_PLC_SRVC_FLAG='Y' '''},
            'PYMNT_MONTH': {'operation':'substring_column', 'based_on':'PYMNT_DATE','start':5, 'length':2,'dtype':''},
            'PYMNT_YEAR': {'operation':'substring_column', 'based_on':'PYMNT_DATE','start':1, 'length':4,'dtype':''},
            'FDB_GNRC_CODE_NO': {'operation':'null_column'},#mapping says no longer used
            'THRPT_CLASS_CODE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TDDDRGC','based_on_column':'NTNL_DRUG_CODE','source_column':'THRPT_CLASS_CODE','join_column':'NTNL_DRUG_CODE','filter':''},##BWCODS.TDDDRGC table is empty
            'RCVRY_FUND_TYPE': {'operation':'null_column'},#setting to null for now--need logic
            'RCVRY_CVRG_TYPE': {'operation':'null_column'},#setting to null for now--need logic
            'RCVRY_ACDNT_TYPE': {'operation':'null_column'},#setting to null for now--need logic
            'RCVRY_BILL_TYPE_F2': {'operation':'null_column'},#setting to null for now--need logic
            'RCVRY_BILL_TYPE_L3': {'operation':'null_column'},#setting to null for now--need logic
            'DRUG_CMPND_TEXT': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDDRCC','based_on_column':'DRUG_CMPND_CODE','source_column':'DRUG_CMPND_TEXT','join_column':'DRUG_CMPND_CODE','filter':''},
            'DRUG_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDDRTY','based_on_column':'DRUG_TYPE_CODE','source_column':'DRUG_TYPE','join_column':'DRUG_TYPE_CODE','filter':''},
            'DSRT_CODE': {'operation':'rename_column','based_on':'GNRC_PRDCT_IND',},
            'DSRT_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDDSRT','based_on_column':'GNRC_PRDCT_IND','source_column':'DSRT_TYPE','join_column':'DSRT_CODE','filter':''},
            'DRUG_CVRG_STS': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDDRCS','based_on_column':'DRUG_CVRG_STS_CODE','source_column':'DRUG_CVRG_STS','join_column':'DRUG_CVRG_STS_CODE','filter':''},
            'PBM_PRCNG_SRC_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPBMP','based_on_column':'PBM_PRCNG_SRC_CODE','source_column':'PBM_PRCNG_SRC_TYPE','join_column':'PBM_PRCNG_SRC_CODE','filter':''},
            'PBM_PRCNG_TYPE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TCDPBMT','based_on_column':'PBM_PRCNG_CODE','source_column':'PBM_PRCNG_TYPE','join_column':'PBM_PRCNG_CODE','filter':''},
            'THRPT_CLS_PRN_CODE': {'operation':'lookup','based_on_table':'BWCODS_OLD.TDDDRGC','based_on_column':'NTNL_DRUG_CODE','source_column':'THRPT_CLS_PRN_CODE','join_column':'NTNL_DRUG_CODE','filter':''},##BWCODS.TDDDRGC table is empty
            'MDSPN_GPI_GRP_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'MDSPN_GPI_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICDV_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICD2_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICD2V_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICD3_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICD3V_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICD4_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'ICD4V_CODE': {'operation':'null_column'},#setting to null for now--need logic
            'BWC_ALWD_AMT': {'operation':'amount_field',},
            'BWC_FEE_AMT': {'operation':'amount_field',},
            'INTRS_PAID_AMT': {'operation':'amount_field',},
            'IW_COPAY_AMT': {'operation':'amount_field',},
            'NTWK_AMT': {'operation':'amount_field',},
            'PAID_AMT': {'operation':'amount_field',},
            'PBM_ADMN_AMT': {'operation':'amount_field',},
            'PBM_APR_DS_AMT': {'operation':'amount_field',},
            'PBM_APR_IN_AMT': {'operation':'amount_field',},
            'PBM_APR_SL_AMT': {'operation':'amount_field',},
            'PBM_SBM_DSPN_AMT': {'operation':'amount_field',},
            'PBM_SBM_TAX_AMT': {'operation':'amount_field',},
            'PBM_SBM_UC_AMT': {'operation':'amount_field',},
            'PHRMC_PD_AMT': {'operation':'amount_field',},
            'PRVDR_AMT': {'operation':'amount_field',},
            'RCVRY_AMT': {'operation':'amount_field',},
            'RMBRS_AMT': {'operation':'amount_field',},
        }, 
     },
    # f'{staged_file_prefix}_DW_INVOICE_EDTEOB': {
    #     'target':'DW_INVOICE_EDTEOB',
    #     'strategy':'bulk',
    # },
    # f'{staged_file_prefix}_DW_REF': {
    #     'target':'DW_REF',
    #     'strategy':'bulk',
    ######TCDCOND AND TCDMODS ARE TWO TARGETS
    # },
    # f'{staged_file_prefix}_DW_REV_CPT_CODE': {
    #     'target':'REV_CPT_CODE',
    #     'strategy':'bulk',
    # },
}