#std
import sys,argparse,os,gzip,datetime
from pathlib import Path

#other libs

#local libs
def set_libpath():
    r'''
    Set path import to be relative to the location of the dir the prog is run from
     C:\Users\nielsenjf\bwcroot\bwcenv\bwcrun\run_createviews.py
    becomes:  C:\Users\nielsenjf\bwcroot\
    '''
    import sys,os
    from pathlib import Path
    prog_path=Path(os.path.abspath(__file__))
    root=prog_path.parent.parent.parent
    pyversion=f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

def write_resultset(resultset,max_id):
    '''
    ['1','00-333224', '2', datetime.datetime(2014, 4, 6, 0, 0), datetime.datetime(2016, 11, 12, 0, 0), '40052.0000000000', 'Permanent Total', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'SSNxxxxxxxxx', '1', ' ', datetime.datetime(1970, 8, 18, 0, 0), 'SSN verificxxxxx', ' ', 'WATT', 'TERRY', 'D', '45937 ANNAPOLIS RD', '', 'HOPEDALE', 'OH', '439769724', 'phys', 'physical', datetime.datetime(2017, 7, 20, 0, 0), datetime.datetime(2016, 1, 1, 0, 0), datetime.datetime(2017, 6, 30, 0, 0), '9358909', 'W', 'Weekly', 'EP00000015', '4', 'BOTH, WAGE AND BENEFITS', '0', 'Social Security Number (SSN)']
    '''
    '''
EFF_DATE1 = INDM_PAY_EFF1_DATE
END_DATE1 = INDM_PAY_END1_DATE
TOT_AMT1 = INDM_PAY_DRV_TOT1_AMT
EFF_DATE2 = INDM_PAY_EFF2_DATE
END_DATE2 = INDM_PAY_END2_DATE
TOT_AMT2 = INDM_PAY_DRV_TOT2_AMT
EFF_DATE3 = INDM_PAY_EFF3_DATE
END_DATE3 = INDM_PAY_END3_DATE
TOT_AMT3 = INDM_PAY_DRV_TOT3_AMT

    '''

    
    selected_order_of_column=['CLM_NO','PRGRM_TYP1_CODE','INDM_PAY_EFF1_DATE','INDM_PAY_END1_DATE','INDM_PAY_DRV_TOT1_AMT','PRGRM_TYP1_NAME','INDM_PAY_EFF2_DATE','INDM_PAY_END2_DATE','INDM_PAY_DRV_TOT2_AMT','INDM_PAY_EFF3_DATE','INDM_PAY_END3_DATE','INDM_PAY_DRV_TOT3_AMT','INDM_PYMT_PRD_FRQN2_CODE','INDM_PYMT_PRD_FRQN2_NAME','INDM_PYMT_PRD_FRQN3_CODE','INDM_PYMT_PRD_FRQN3_NAME','PRGRM_TYP2_CODE','PRGRM_TYP2_NAME','PRGRM_TYP3_CODE','PRGRM_TYP3_NAME','DW_INDMN_PYMNT_SMRY_ODJFS_OTBND_ID','INDM_PAY_DRV_SCH_TOT1_AMT','INDM_PAY_DRV_SCH_TOT2_AMT','INDM_PAY_DRV_SCH_TOT3_AMT','RCRD_TYPE_CODE','ODJFS_CASE_NMBR','CLMT_BIRTH_DATE','CLMT_SSN_NMBR','SSN_VRFCT_CODE','CLMT_LAST_NAME' ,'CLMT_FIRST_NAME','CLMT_MID_NAME','CLMT_ADRS_LINE1','CLMT_ADRS_LINE2','CLMT_ADRS_CITY_NAME','CLMT_ADRS_STATE_CODE', 'CLMT_ADRS_POST_CODE', 'CLMT_ADRS_TYPE_CODE', 'CLMT_ADRS_TYPE_NAME', 'OTBND_FILE_CRT_DATE', 'MATCH_PRD_BGNG_DATE', 'MATCH_PRD_ENDNG_DATE','AGRE_ID','INDM_PYMT_PRD_FRQN1_CODE','INDM_PYMT_PRD_FRQN1_NAME', 'CNTRC_NMBR', 'MATCH_TYPE_CODE', 'MATCH_TYPE_NAME', 'MATCH_CNDTN_CODE','MATCH_CNDTN_NAME','DW_CREATE_DTM' , 'DW_UPDATE_DTM']

    ddl_order_column = ['DW_INDMN_PYMNT_SMRY_ODJFS_OTBND_ID' ,'RCRD_TYPE_CODE' , 'ODJFS_CASE_NMBR','CLMT_BIRTH_DATE' , 'CLMT_SSN_NMBR' , 'SSN_VRFCT_CODE' ,
    'CLMT_LAST_NAME' , 'CLMT_FIRST_NAME' , 'CLMT_MID_NAME' , 'CLMT_ADRS_LINE1' , 'CLMT_ADRS_LINE2' , 'CLMT_ADRS_CITY_NAME' , 'CLMT_ADRS_STATE_CODE' ,
    'CLMT_ADRS_POST_CODE' , 'CLMT_ADRS_TYPE_CODE' , 'CLMT_ADRS_TYPE_NAME' , 'OTBND_FILE_CRT_DATE' , 'MATCH_PRD_BGNG_DATE' , 'MATCH_PRD_ENDNG_DATE' ,
    'CLM_NO' , 'AGRE_ID' , 'PRGRM_TYP1_CODE' , 'PRGRM_TYP1_NAME' , 'INDM_PAY_DRV_TOT1_AMT' , 'INDM_PYMT_PRD_FRQN1_CODE' , 'INDM_PYMT_PRD_FRQN1_NAME' ,
    'INDM_PAY_EFF1_DATE' , 'INDM_PAY_END1_DATE' , 'INDM_PAY_DRV_SCH_TOT1_AMT' , 'PRGRM_TYP2_CODE' , 'PRGRM_TYP2_NAME' , 'INDM_PAY_DRV_TOT2_AMT' ,
    'INDM_PYMT_PRD_FRQN2_CODE' , 'INDM_PYMT_PRD_FRQN2_NAME' , 'INDM_PAY_EFF2_DATE' , 'INDM_PAY_END2_DATE' , 'INDM_PAY_DRV_SCH_TOT2_AMT' ,
    'PRGRM_TYP3_CODE' , 'PRGRM_TYP3_NAME' ,  'INDM_PAY_DRV_TOT3_AMT' , 'INDM_PYMT_PRD_FRQN3_CODE' , 'INDM_PYMT_PRD_FRQN3_NAME' , 'INDM_PAY_EFF3_DATE' ,
    'INDM_PAY_END3_DATE' , 'INDM_PAY_DRV_SCH_TOT3_AMT' ,  'CNTRC_NMBR' ,  'MATCH_TYPE_CODE' , 'MATCH_TYPE_NAME' , 'MATCH_CNDTN_CODE' , 'MATCH_CNDTN_NAME' ,
    'DW_CREATE_DTM' , 'DW_UPDATE_DTM' ]
    
    

    now=datetime.datetime.now()
    newrows=[]
    for row in resultset:
        newrow=remap_cols(ddl_order_column,selected_order_of_column,row)
        for position,col in  enumerate(newrow):
#            print row,'******row'
            if position==0:
#                last_id_inc+=1
                max_id = int(max_id)
                max_id+=1
#                col=last_id_inc
                col=max_id
            if isinstance(col,datetime.datetime):
                col=col.strftime('%Y%m%d')
            elif isinstance(col,str):
                if not col.strip(): col=''
            elif not col:
                col=''
            else:
                col=str(col)
            newrow[position]=col
        newrow_dict=dict(zip(ddl_order_column,newrow))
        newrows.append(newrow_dict)

    return newrows

            
def remap_cols(ddl_order_column,selected_order_of_column,selected_order_of_column_data):
    '''
    #create a dictionary that holds all of the values out of select row
    #then through the ddl to pull the values in ddl order

    #get the cols in the order defined by ddl
    #cols = 'RCRD_TYPE_CODE', 'ODJFS_CASE_NMBR', 'CLMT_BIRTH_DATE' ...
    #col = 'RCRD_TYPE_CODE'
    #in ddl location col is 1

    #got to col order defined by select statement
    #location=selected_order_of_column.index('RCRD_TYPE_CODE')=5
    #get value at that location
    #value = selected_order_of_column_data[5]
    #create_dict[col] = value
    #with the key as the columnm go through the ddl order and pull values out of new dict

    '''

    create_dict={}

#    selected_order_of_column_data[selected_order_of_column.index(col)]
    for col in ddl_order_column:
        try:
            create_dict[col] = selected_order_of_column_data[selected_order_of_column.index(col)]
        except Exception as error:
            #e1 51 52
#            print 'e1',len( selected_order_of_column_data),len( ddl_order_column)
#            print 'ddl_order_column*****',ddl_order_column
            #INDM_PAY_DRV_SCH_TOT3_AMT 23
            
            for  idx, col_name in enumerate( selected_order_of_column):
                print(idx,'name:\t',col_name,'val:\t',selected_order_of_column_data[idx])

            print('selected_order_of_column*****',selected_order_of_column)
            print('selected_order_of_column_data*****',selected_order_of_column_data)
            print('e1',error,col,selected_order_of_column.index(col))
#            print selected_order_of_column_data[selected_order_of_column.index(col)];raw_input('go')
    switch_data_to_ddl=[]
    for col in ddl_order_column:
        try:
            switch_data_to_ddl.append(create_dict[col])
        except:
            print(sorted(selected_order_of_column) == sorted(ddl_order_column))
#            print 'e2',col;raw_input('go')
    return switch_data_to_ddl

def row_compare(cur_EFF_DATE,cur_END_DATE,hist_EFF_DATE,hist_END_DATE):
    '''

    This function looks at cur row dates and the historical row dates to
    determine if a new row needs to be created or modification to the historical row

    if no gaps: smush (either move the dates or make no change)
    if new: can only happen when: cur_EFF_DATE>hist_EFF_DATE and cur_EFF_DATE > hist_END_DATE

    #not possible due to order by assumption
    if cur_EFF_DATE<hist_EFF_DATE 
         cur_EFF_DATE > hist_END_DATE:   
         cur_EFF_DATE < hist_END_DATE:   
   
    '''



    #to force continous times to merge add a day to end date
    #ex: Sept 1 and Sep 2 still counts as continous, Sep 1 and Sep 3 is not
    add_hist_END_DATE = hist_END_DATE +  datetime.timedelta(days=1) 


    #Section1: cur_EFF_DATE > than dates
    #this needs to be first: to capture the 1 day off smush

    #widen right side with cur_end, since eff=hist end   is first part necessary
    #cur_EFF_DATE>=hist_EFF_DATE may need to be looked at further
    if cur_EFF_DATE>=hist_EFF_DATE and cur_EFF_DATE == add_hist_END_DATE:
       return ('smush',hist_EFF_DATE,cur_END_DATE)

    #new row w\larger left side, since curr_eff > eff and end
    if cur_EFF_DATE>hist_EFF_DATE and cur_EFF_DATE > hist_END_DATE:
        return ('new/gap',cur_EFF_DATE,cur_END_DATE)

    # Section2: cur_END_DATE,hist_END_DATE


    #NOT needed since = is handled below, and curr_eff < hist_eff due to order by
    #if cur_EFF_DATE<=hist_EFF_DATE and cur_END_DATE<=hist_END_DATE :

    #widen left and right side to include new cur dates
    #ONLY should run for =, since order by is in SQL
    if cur_EFF_DATE<=hist_EFF_DATE and cur_END_DATE>=hist_END_DATE :
        hist_EFF_DATE=cur_EFF_DATE
        hist_END_DATE=cur_END_DATE
        #updated historical
        return ('smush',hist_EFF_DATE,hist_END_DATE)

    #do nothing, current is inbetween historical
    if cur_EFF_DATE>=hist_EFF_DATE and cur_END_DATE<=hist_END_DATE:
        #updated historical
       return ('smush',hist_EFF_DATE,hist_END_DATE)

    #widen the left side to include larger cur end date
    if cur_EFF_DATE>=hist_EFF_DATE and cur_END_DATE>=hist_END_DATE:
        #updated historical
       return ('smush',hist_EFF_DATE,cur_END_DATE)


   #(datetime.date(2016, 1, 1), datetime.date(2016, 7, 9), datetime.date(2004, 1, 25), datetime.date(2016, 11, 12))
#    print 'cur_EFF_DATE>=hist_EFF_DATE,cur_END_DATE>=hist_END_DATE'
#    print cur_EFF_DATE>=hist_EFF_DATE,cur_END_DATE>=hist_END_DATE
#    print 'cur_EFF_DATE,cur_END_DATE,hist_EFF_DATE,hist_END_DATE'
#    print (cur_EFF_DATE,cur_END_DATE,hist_EFF_DATE,hist_END_DATE)

def run_vsql(args,sql,fname_tmp):
    '''
    There is a bug w\python driver. So vsql is being used instead. Because data being processed is csv, the
    rows come in as strings.  Need to convert to datetime, for later date calculations.

    {'CLM_NO': '00-300012', 'PRGRM_TYP1_CODE': '2', 'INDM_PAY_EFF_DATE': datetime.date(2016, 11, 13),
     'INDM_PAY_END_DATE': datetime.date(2021, 4, 1), 'INDM_PAY_DRV_TOT_AMT': Decimal('99559.3300000000'), 
     'PRGRM_TYP1_NAME': 'Permanent Total', 'DW_INDMN_PYMNT_SMRY_ODJFS_OTBND_ID': 0, 
     'INDM_PAY_DRV_SCH_TOT1_AMT': '', 'INDM_PAY_DRV_SCH_TOT2_AMT': '', 'INDM_PAY_DRV_SCH_TOT3_AMT': '',
      'RCRD_TYPE_CODE': '1', 'ODJFS_CASE_NMBR': ' ', 'CLMT_BIRTH_DATE': datetime.date(1947, 8, 14), 
      'CLMT_SSN_NMBR': '293444726', 'SSN_VRFCT_CODE': ' ', 'CLMT_LAST_NAME': 'DIX', 
      'CLMT_FIRST_NAME': 'ROGER', 'CLMT_MID_NAME': 'E', 'CLMT_ADRS_LINE1': '114 GERMAN ST',
       'CLMT_ADRS_LINE2': None, 'CLMT_ADRS_CITY_NAME': 'PAULDING', 'CLMT_ADRS_STATE_CODE': 'OH', 
       'CLMT_ADRS_POST_CODE': '458791515', 'CLMT_ADRS_TYPE_CODE': 'mail', 
       'CLMT_ADRS_TYPE_NAME': 'mailing', 'OTBND_FILE_CRT_DATE': datetime.date(2022, 1, 11),
        'MATCH_PRD_BGNG_DATE': datetime.date(2020, 7, 1), 'MATCH_PRD_ENDNG_DATE': datetime.date(2021, 12, 31), 
        'AGRE_ID': 6750735, 'INDM_PYMT_PRD_FRQN1_CODE': 'W', 'INDM_PYMT_PRD_FRQN1_NAME': 'Weekly', 
        'CNTRC_NMBR': 'EP00000015', 'MATCH_TYPE_CODE': '4', 'MATCH_TYPE_NAME': 'BOTH, WAGE AND BENEFITS',
         'MATCH_CNDTN_CODE': '0', 
         'DW_CREATE_DTM': datetime.date(2022, 10, 4), 'DW_UPDATE_DTM': datetime.date(2022, 10, 4)}
    '''
    #dblib.vsql2file(db['login'],db['passwd'],fname_tmp,sql,noheader=True,gzip=True,delim="'\06'")
    results =  args.srccon.fetchall( sql )
    return list(results) 
    #print(results[0]);input('go')
        
    #inf.write_csv(fname_tmp, results, delim='\t' )

    # for row in gzip.open(fname_tmp):
    #     row=str(row).strip()

    #     cols=row.split('\t')
    #     newcols=cols[:]
    #     for idx,col in  enumerate(cols):
    #         if not col: continue
    #         if len(col)==10 and col.count('-')==2 and col[0:4].isdigit() and col[5:7].isdigit() and col[9:11].isdigit():
    #             yr,mth,dy=[int(f) for f in col.split('-')]
    #             newcols[idx]=datetime.datetime(yr,mth,dy)
    #     yield newcols


def run_sql(con,sql):
    for row in con.fetchrow(sql):
        yield row
    
def process_dbrows(rows,clm_no='',count=0):
    '''
    This function collapses rows together that have continous or overlapping dates.
    If there is a break, move the data to the columns to the right.
    After 3 groups are full to the right, move the data to a new row.
    Claim number and program type define rows for a group.
    A dictionary is used to keep track of the rows.
{'CLM_NO': '00-300012', 'PRGRM_TYP1_CODE': '2', 'INDM_PAY_EFF_DATE': datetime.date(2016, 11, 13), 
'INDM_PAY_END_DATE': datetime.date(2021, 4, 1), 'INDM_PAY_DRV_TOT_AMT': Decimal('99559.3300000000'), 
'PRGRM_TYP1_NAME': 'Permanent Total', 'DW_INDMN_PYMNT_SMRY_ODJFS_OTBND_ID': 0, 'INDM_PAY_DRV_SCH_TOT1_AMT': '', 
'INDM_PAY_DRV_SCH_TOT2_AMT': '', 'INDM_PAY_DRV_SCH_TOT3_AMT': '', 'RCRD_TYPE_CODE': '1', 'ODJFS_CASE_NMBR': ' ',
 'CLMT_BIRTH_DATE': datetime.date(1947, 8, 14), 'CLMT_SSN_NMBR': '293444726', 'SSN_VRFCT_CODE': ' ',
  'CLMT_LAST_NAME': 'DIX', 'CLMT_FIRST_NAME': 'ROGER', 'CLMT_MID_NAME': 'E', 'CLMT_ADRS_LINE1': '114 GERMAN ST', 
  'CLMT_ADRS_LINE2': None, 'CLMT_ADRS_CITY_NAME': 'PAULDING', 'CLMT_ADRS_STATE_CODE': 'OH', 
  'CLMT_ADRS_POST_CODE': '458791515', 'CLMT_ADRS_TYPE_CODE': 'mail', 'CLMT_ADRS_TYPE_NAME': 'mailing',
   'OTBND_FILE_CRT_DATE': datetime.date(2022, 1, 11), 'MATCH_PRD_BGNG_DATE': datetime.date(2020, 7, 1),
    'MATCH_PRD_ENDNG_DATE': datetime.date(2021, 12, 31), 'AGRE_ID': 6750735, 'INDM_PYMT_PRD_FRQN1_CODE': 'W', 
    'INDM_PYMT_PRD_FRQN1_NAME': 'Weekly', 'CNTRC_NMBR': 'EP00000015', 'MATCH_TYPE_CODE': '4', 
    'MATCH_TYPE_NAME': 'BOTH, WAGE AND BENEFITS', 'MATCH_CNDTN_CODE': '0',
 'MATCH_CNDTN_NAME': 'Social Security Number (SSN)', 'DW_CREATE_DTM': datetime.date(2022, 10, 4), 
 'DW_UPDATE_DTM': datetime.date(2022, 10, 4)}

    


    '''

    #this is the master dictionary that has: key:(cur_CLM_NO,cur_PRGRM_TYP1_CODE) val: [ list of rows]
    resultset={}
    rownum=0

    INDM_PYMT_PRD_FRQN2_CODE = ''
    INDM_PYMT_PRD_FRQN2_NAME = ''
    INDM_PYMT_PRD_FRQN3_CODE = ''
    INDM_PYMT_PRD_FRQN3_NAME = ''
    PRGRM_TYP2_CODE = ''
    PRGRM_TYP2_NAME = ''
    PRGRM_TYP3_CODE = ''
    PRGRM_TYP3_NAME = ''

    for rownum,cur_row_dict in enumerate(rows):
        cur_row=list(cur_row_dict.values())
                
#        if rownum>500: break

        #the current row coming from the 1st table, does not have 4 dates,2 amts  + 8 CODE,name columns
        #put in placeholder for those columns to make output table
        
        cur_CLM_NO,cur_PRGRM_TYP1_CODE,cur_EFF_DATE,cur_END_DATE,cur_TOT_AMT,PRGRM_TYP1_NAME=cur_row[:6]

        rest_cur=['','','','','',''] + ['','','','','','','',''] + cur_row[6:]

        cur_CLM_NO=cur_CLM_NO.strip()
        cur_PRGRM_TYP1_CODE=cur_PRGRM_TYP1_CODE.strip()
        
        #dictionary key 
        unique = (cur_CLM_NO,cur_PRGRM_TYP1_CODE)


        TOT_AMT2 =''
        TOT_AMT3 =''
        TOT_AMT1 =''

        #if the unique key is not in the dictionary, create a new row that matches output table
        #this row needs the additional empty placeholder columns added to it
        #the key points to a list with initialy, 1 row in it
        if unique not in resultset:
            new_row=cur_row[:6]+rest_cur
            TOT_AMT2 =''
            TOT_AMT3 =''
            TOT_AMT1 =''

            resultset[unique] = [new_row]
        else:
            #Since it is not a new key, either columns are going to be added to the right
            #or a new row will be added to the rowlist for that key.
            #Except for the most recent row, all other rows are not modified
            #Use [-1] to get the most recent row to modify

            histrow=resultset[unique][-1]
            #['00-300012', '2', datetime.date(2014, 6, 29), datetime.date(2016, 11, 12), 53977.2,'permanent total', '', '', '', '', '', '']

            CLM_NO,PRGRM_TYP1_CODE,EFF_DATE1,END_DATE1,TOT_AMT1,PRGRM_TYP1_NAME , EFF_DATE2,END_DATE2,TOT_AMT2,EFF_DATE3,END_DATE3,TOT_AMT3=histrow[:12]
            INDM_PYMT_PRD_FRQN2_CODE,INDM_PYMT_PRD_FRQN2_NAME,INDM_PYMT_PRD_FRQN3_CODE,INDM_PYMT_PRD_FRQN3_NAME,PRGRM_TYP2_CODE,PRGRM_TYP2_NAME,PRGRM_TYP3_CODE,PRGRM_TYP3_NAME=histrow[12:20]
                       

#            rest=histrow[20:]

            #if histrow[9] has data in it, that means a new row will need to be created if there is a gap
            #otherwise populate the "3" columns ex: EFF_DATE3
            if histrow[9]:
#                print 'clm_no9', CLM_NO;raw_input('go')
                next_level,ret_EFF_DATE,ret_END_DATE=row_compare(cur_EFF_DATE,cur_END_DATE,EFF_DATE3,END_DATE3)
                if next_level=='new/gap':
                    #APPEND:create a new row with row that came from database
                    new_row=cur_row[:6]+rest_cur
                    TOT_AMT2 =''
                    TOT_AMT3 =''
                    TOT_AMT1 =''
                    resultset[unique].append(new_row)
                    continue
                elif next_level=='smush':
                    #populate the "3" columns
                    EFF_DATE3=ret_EFF_DATE;END_DATE3=ret_END_DATE;TOT_AMT3=round(float(cur_TOT_AMT) + float(TOT_AMT3))
                    INDM_PYMT_PRD_FRQN3_CODE = 'W'
                    INDM_PYMT_PRD_FRQN3_NAME = 'Weekly'
                    PRGRM_TYP3_CODE = PRGRM_TYP1_CODE
                    PRGRM_TYP3_NAME = PRGRM_TYP1_NAME
                                 
            #if histrow[6] has data in it, populate the "3" columns if there is a gap
            #otherwise collpase the new data with with "2" columns  ex: EFF_DATE2
            elif histrow[6]:
                next_level,ret_EFF_DATE,ret_END_DATE=row_compare(cur_EFF_DATE,cur_END_DATE,EFF_DATE2,END_DATE2)
                if next_level=='new/gap':
                    #populate the "3" columns
                    EFF_DATE3=ret_EFF_DATE;END_DATE3=ret_END_DATE;TOT_AMT3=cur_TOT_AMT
                    INDM_PYMT_PRD_FRQN3_CODE = 'W'
                    INDM_PYMT_PRD_FRQN3_NAME = 'Weekly'
                    PRGRM_TYP3_CODE = PRGRM_TYP1_CODE
                    PRGRM_TYP3_NAME = PRGRM_TYP1_NAME

                elif next_level == 'smush':
                    #populate the "2" columns
                    EFF_DATE2=ret_EFF_DATE;END_DATE2=ret_END_DATE;TOT_AMT2=round(float(cur_TOT_AMT) + float(TOT_AMT2))
                    INDM_PYMT_PRD_FRQN2_CODE = 'W'
                    INDM_PYMT_PRD_FRQN2_NAME = 'Weekly'
                    PRGRM_TYP2_CODE = PRGRM_TYP1_CODE
                    PRGRM_TYP2_NAME = PRGRM_TYP1_NAME

            else:
                #There has not been a gap in dates yet, populate the "2" cols if there is a gap ex: EFF_DATE2
                next_level,ret_EFF_DATE,ret_END_DATE=row_compare(cur_EFF_DATE,cur_END_DATE,EFF_DATE1,END_DATE1)


                if next_level=='new/gap':
                    #populate the "2" columns
                    EFF_DATE2=ret_EFF_DATE;END_DATE2=ret_END_DATE;TOT_AMT2=cur_TOT_AMT
                    INDM_PYMT_PRD_FRQN2_CODE = 'W'
                    INDM_PYMT_PRD_FRQN2_NAME = 'Weekly'
                    PRGRM_TYP2_CODE = PRGRM_TYP1_CODE
                    PRGRM_TYP2_NAME = PRGRM_TYP1_NAME

                else:
                    #The "1" columns are populated because there is not gap:ex: EFF_DATE1
#                    print cur_TOT_AMT, TOT_AMT1
                    EFF_DATE1=ret_EFF_DATE;END_DATE1=ret_END_DATE;TOT_AMT1=round(float(cur_TOT_AMT) + float(TOT_AMT1))
                    TOT_AMT2 = ''
                    TOT_AMT3 = ''


            if EFF_DATE2 == '':
               TOT_AMT2 = ''
            if EFF_DATE3 == '':
               TOT_AMT3 = ''
#            print 'CLM_NO,PRGRM_TYP1_CODE,EFF_DATE1,END_DATE1,TOT_AMT1,PRGRM_TYP1_NAME,EFF_DATE2,END_DATE2,TOT_AMT2,EFF_DATE3,END_DATE3,TOT_AMT3',CLM_NO,PRGRM_TYP1_CODE,EFF_DATE1,END_DATE1,TOT_AMT1,PRGRM_TYP1_NAME,EFF_DATE2,END_DATE2,TOT_AMT2,EFF_DATE3,END_DATE3,TOT_AMT3
    

            histrow[:12]=CLM_NO,PRGRM_TYP1_CODE,EFF_DATE1,END_DATE1,TOT_AMT1,PRGRM_TYP1_NAME,EFF_DATE2,END_DATE2,TOT_AMT2,EFF_DATE3,END_DATE3,TOT_AMT3
            histrow[12:20]=INDM_PYMT_PRD_FRQN2_CODE,INDM_PYMT_PRD_FRQN2_NAME,INDM_PYMT_PRD_FRQN3_CODE,INDM_PYMT_PRD_FRQN3_NAME,PRGRM_TYP2_CODE,PRGRM_TYP2_NAME,PRGRM_TYP3_CODE,PRGRM_TYP3_NAME
#            print histrow[:12]
            final_hist_row=histrow
           
#            final_row = final_hist_row+list[rest]
            #OVERWRITE:replace row in result set with modified columns
            resultset[unique][-1]=final_hist_row
            

    final_rows=[]
    for rows in list(resultset.values()):
        for row in rows:
            TOT_AMT1=row[4];TOT_AMT2=row[8];TOT_AMT3=row[11]
            if not TOT_AMT1:
                TOT_AMT1=0
            else:
                TOT_AMT1=float(TOT_AMT1)
            if not TOT_AMT2:
                TOT_AMT2=0
            else:
                TOT_AMT2=float(TOT_AMT2)
            if not TOT_AMT3:
                TOT_AMT3=0
            else:
                TOT_AMT3=float(TOT_AMT3)
            if TOT_AMT1 > 99999 :
                TOT_AMT1 = 99999
            if TOT_AMT2 > 99999 :
                TOT_AMT2 = 99999
            if TOT_AMT3 > 99999 :
                TOT_AMT3 =99999
            row[4]=TOT_AMT1;row[8]=TOT_AMT2;row[11]=TOT_AMT3
            final_rows.append(row)
    return final_rows


def get_sql(args):
    '''

     and CLMT_SSN = '302681613'

     This code requires the following order by:
           CLM_NO, PRGRM_TYP1_CODE, B.INDM_PAY_EFF_DATE, B.INDM_PAY_END_DATE,
    '''

    sql1 = f'''
           select 
B.CLM_NO as CLM_NO,
B.PRGRM_TYP_CODE as PRGRM_TYP1_CODE, 
B.INDM_PAY_EFF_DATE,
B.INDM_PAY_END_DATE,
B.INDM_PAY_DRV_TOT_AMT,
B.PRGRM_TYP_NAME AS PRGRM_TYP1_NAME,
0 AS DW_INDMN_PYMNT_SMRY_ODJFS_OTBND_ID,
'' as INDM_PAY_DRV_SCH_TOT1_AMT,
'' as INDM_PAY_DRV_SCH_TOT2_AMT,
'' as INDM_PAY_DRV_SCH_TOT3_AMT,
'1' as RCRD_TYPE_CODE,
' ' as ODJFS_CASE_NMBR,
DATE(A.CLMT_BIRTH_DATE) AS CLMT_BIRTH_DATE,
substring(B.CLMT_SSN_NMBR,1,9) AS CLMT_SSN_NMBR,
' ' as SSN_VRFCT_CODE,
A.CLMT_LAST_NAME as CLMT_LAST_NAME ,
A.CLMT_FIRST_NAME as CLMT_FIRST_NAME,
A.CLMT_MID_NAME as CLMT_MID_NAME,
case when (A.CLMT_PHYS_ADRS_LINE1 > ' ') then A.CLMT_PHYS_ADRS_LINE1 else A.CLMT_MAIL_ADRS_LINE1 end CLMT_ADRS_LINE1,
case when (A.CLMT_PHYS_ADRS_LINE2 > ' ') then A.CLMT_PHYS_ADRS_LINE2 else A.CLMT_MAIL_ADRS_LINE2 end CLMT_ADRS_LINE2,
case when (A.CLMT_PHYS_ADRS_CITY_NAME > ' ') then A.CLMT_PHYS_ADRS_CITY_NAME else A.CLMT_MAIL_ADRS_CITY_NAME end CLMT_ADRS_CITY_NAME,
case when (A.CLMT_PHYS_ADRS_STATE_CODE > ' ') then A.CLMT_PHYS_ADRS_STATE_CODE else a.CLMT_MAIL_ADRS_STATE_CODE end CLMT_ADRS_STATE_CODE,
case when (A.CLMT_PHYS_ADRS_POST_CODE > ' ') then A.CLMT_PHYS_ADRS_POST_CODE else a.CLMT_MAIL_ADRS_POST_CODE end CLMT_ADRS_POST_CODE,
case when (A.CLMT_PHYS_ADRS_LINE1 > ' ') then 'phys' else 'mail' end CLMT_ADRS_TYPE_CODE,
case when (A.CLMT_PHYS_ADRS_LINE1 > ' ') then 'physical' else 'mailing' end CLMT_ADRS_TYPE_NAME,
B.OTBND_FILE_CRT_DATE as OTBND_FILE_CRT_DATE,
B.MATCH_PRD_BGNG_DATE as MATCH_PRD_BGNG_DATE,
B.MATCH_PRD_ENDNG_DATE as MATCH_PRD_ENDNG_DATE,
B.AGRE_ID as AGRE_ID,
'W' as INDM_PYMT_PRD_FRQN1_CODE,
'Weekly' as INDM_PYMT_PRD_FRQN1_NAME,
 'EP00000015' AS CNTRC_NMBR,
 '4' AS MATCH_TYPE_CODE,
 'BOTH, WAGE AND BENEFITS' AS MATCH_TYPE_NAME,
 '0' AS MATCH_CNDTN_CODE,
 'Social Security Number (SSN)' AS MATCH_CNDTN_NAME,
 current_date() AS DW_CREATE_DTM,
 current_date() AS DW_UPDATE_DTM
from {args.srcdb}.dw_report.DW_CLAIM_DEMOGRAPHICS A, {args.srcdb}.dw_report.DW_INDEMNITY_PAYMENT_ODJFS_OUTBOUND B
where A.clm_agre_id = B.AGRE_ID
and B.INDM_PAY_EFF_DATE <= B.INDM_PAY_END_DATE
AND B.OTBND_FILE_CRT_DATE = (SELECT MAX(C.OTBND_FILE_CRT_DATE) FROM {args.srcdb}.dw_report.DW_INDEMNITY_PAYMENT_ODJFS_OUTBOUND C) order by 1,2,3,4 '''
    return sql1

def get_sql_max(args):
        sql2 = f'''
           SELECT COALESCE(MAX(DW_INDMN_PYMNT_SMRY_ODJFS_OTBND_ID), 0) AS MAXID FROM {args.srcdb}.DW_REPORT.DW_INDEMNITY_PAYMENT_SUMMARY_ODJFS_OUTBOUND 
              '''
        return sql2

def process_args():
    '''
    '''

    #etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"


    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--srcdir', required=True, help='src dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument('--table',required=True, help='Target Table')

    #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    #optional
    parser.add_argument('--eldir', required=False, default=eldir,help='default directory for all logs, data files')
    parser.add_argument( '--dev', default=False, action='store_true', help='Changes ETL Dir from C to E if true' )
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    #
    args= parser.parse_args()

    args.root=Path(__file__).resolve().parent.parent
    #args.loaddir=args.root/'bwcpresent'
    if args.dev:
        args.eldir=f"C:/Temp/{os.environ['USERNAME'].replace('_x', '')}/EL"

     #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14AM
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key=ymd
    args.load_ts=ymd_hms
        #input("go")

    inf.build_args_paths(args,use_load_key=True)
    
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args





def main():


    try:
        args=None
        args=process_args()
        print(args)
        print(dbsetup.Envs[args.tgtenv])

        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
        srcdb_dict=dbsetup.Envs[args.srcenv][args.srckey]
        #tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))
        args.srccon = dblib.DB(srcdb_dict, log=args.log, port = srcdb_dict.get('port',''))
 
        clm_no=''
    #    clm_no='00-300115'
       
        # Establish the top of our directory for temp files, load files, etc.
        s_schema = args.srcschema

        t_schema = args.tgtschema
        
        #2 connections are used to merge 2 streams together
        con1=args.srccon
            
        data_dir=args.srcdata
        log_dir=args.logdir 
        
        sql1=get_sql(args)
        sql2=get_sql_max(args)
        fname_tmp=data_dir/'odjfs_vsql1.gz'
        ######
    #    db_rows=run_sql(con1,sql1) 
        db_rows=run_vsql(args,sql1,fname_tmp)
        
        #This returns 1 single row with 1 column
        #[{'MAXID': 831467}]
        max_rows=list(run_sql(con1,sql2))

        max_id=max_rows[0]['MAXID']

        args.log.info(f'{max_id=}')
    
        resultset=process_dbrows(db_rows)

        print('resultset rows:',len(resultset))
        
        #resultsetxxx = run_sql(con1,clm_no=clm_no)
        rows=write_resultset(resultset,max_id)
        fname_inproc=data_dir/'odjfs_summary.gz'
        inf.write_csv(fname_inproc, rows,sortit=False )
        print(rows[0])
        con1.load_file( args.tgtdb, args.tgtschema, args.table, fname_inproc, header = 1  )

    except:
        if args:
            args.log.info(inf.geterr())
            raise 
        else:
            print(inf.geterr())
            raise


if __name__=='__main__':
    main()

    '''
 python run_odjfs_summary_vsql_snow.py  --srcdir /prd/snow_etl/RPD1/DW_REPORT --tgtdir /prd/snow_etl/RPD1/DW_REPORT --table DW_INDEMNITY_PAYMENT_SUMMARY_ODJFS_OUTBOUND 
 python ~/bwclib/run/run_odjfs_summary_vsql.py    --operation odjfs_summary   --s_environment dev_cognos  --srcdb vertica --s_schema DW_REPORT   --t_environment dev_cognos  --tgtdb vertica --t_schema DW_REPORT --load_type PCMP --pool_size 2
 '''
