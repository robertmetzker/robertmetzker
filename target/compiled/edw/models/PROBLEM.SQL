SELECT "D1"."C0" AS "Note_ID", "D1"."C1" AS "Policy_Number", "D1"."C2" AS "Note_Category_Type_Name", "D1"."C3" AS "Note_Subject_Text", "D1"."C4" AS "Note_Type_Name", "D1"."C5" AS "Note_Call_Type_Name", "D1"."C6" AS "Note_Call_From_Type_Name", "D1"."C7" AS "Note_Caller_Person_Name", "D1"."C8" AS "Note_Audit_Create_User_ID", "D1"."C9" AS "Note_Audit_Create_Date", "D1"."C10" AS "Note_Text_INT", "D1"."C11" AS "Note_text_Formatted", "D1"."C12" AS "Employer_Contact_Name", "D1"."C13" AS "Employer_Contact_Email_Address" FROM ( SELECT "DW_POLICY_NOTE"."NOTE_ID" AS "C0", "DW_POLICY_NOTE"."PLCY_NO" AS "C1", "DW_POLICY_NOTE"."NOTE_CTG_TYP_NM" AS "C2", "DW_POLICY_NOTE"."NOTE_SBJ_TEXT" AS "C3", "DW_POLICY_NOTE"."NOTE_TYP_NM" AS "C4", "DW_POLICY_NOTE"."NOTE_CALL_TYP_NM" AS "C5", "DW_POLICY_NOTE"."NOTE_CALL_FR_TYP_NM" AS "C6", "DW_POLICY_NOTE"."NOTE_CLLR_PRSN_NM" AS "C7", "DW_POLICY_NOTE"."NOTE_AUDIT_USER_ID_CREA" AS "C8", CAST("DW_POLICY_NOTE"."NOTE_AUDIT_USER_CREA_DTM" AS DATE) AS "C9", LEFT("DW_POLICY_NOTE"."NOTE_TEXT_TEXT", 32750) AS "C10", REGEXP_REPLACE(REGEXP_REPLACE(replace(replace(LEFT("DW_POLICY_NOTE"."NOTE_TEXT_TEXT", 32750), '&nbsp;', ' '), '&#39;', ' '), '<.*?>|&.*;', ''), '[\r\n]+', '
') AS "C11", "DW_EMPLOYER_DEMOGRAPHICS"."EMPLR_CNTCT_NAME" AS "C12", "DW_EMPLOYER_DEMOGRAPHICS"."EMPLR_CNTCT_EMAIL_ADDRESS" AS "C13" FROM "DW_REPORT"."DW_PLCY_PRD" "DW_PLCY_PRD" INNER JOIN "DW_REPORT"."DW_EMPLOYER_DEMOGRAPHICS" "DW_EMPLOYER_DEMOGRAPHICS" ON "DW_PLCY_PRD"."AGRE_ID" = "DW_EMPLOYER_DEMOGRAPHICS"."AGRE_ID" LEFT OUTER JOIN "DW_REPORT"."DW_POLICY_NOTE" "DW_POLICY_NOTE" ON "DW_PLCY_PRD"."AGRE_ID" = "DW_POLICY_NOTE"."PLCY_AGRE_ID" AND ("DW_PLCY_PRD"."PLCY_PRD_ID" = "DW_POLICY_NOTE"."PLCY_PRD_ID" OR "DW_POLICY_NOTE"."PLCY_PRD_ID" IS NULL) WHERE "DW_POLICY_NOTE"."NOTE_AUDIT_USER_ID_CREA" IN ( '1000263', '6303112', '6303115', '6303117', '6303121', '6303122', '6303125', '6303132', '6303133', '6303134', '6303138', '6303140', '6303147', '6303150', '6303152', '6303153', '6303154', '6303155', '6303156', '6303161', '6303372', '6303443', '6303451', '6303482', '6303630', '6303634', '6303853', '6303919', '6303961', '6304535', '6304982', '6304986', '6304989', '6305011', '6305081', '1000582047' ) AND CAST("DW_POLICY_NOTE"."NOTE_AUDIT_USER_CREA_DTM" AS DATE) >= to_date ('2022-09-16', 'YYYY-MM-DD') AND CAST("DW_POLICY_NOTE"."NOTE_AUDIT_USER_CREA_DTM" AS DATE) <= to_date ('2022-09-26', 'YYYY-MM-DD') ) "D1" GROUP BY "D1"."C0", "D1"."C1", "D1"."C2", "D1"."C3", "D1"."C4", "D1"."C5", "D1"."C6", "D1"."C7", "D1"."C8", "D1"."C9", "D1"."C10", "D1"."C11", "D1"."C12", "D1"."C13"


regexp_replace( regexp_replace( LEFT("DW_POLICY_NOTE"."NOTE_TEXT_TEXT", 32750),'(&nbsp;)',''), '(&.*;)|(<\\w*>)|(</\\w*>)', ' ') AS "C11", 


LEFT("DW_POLICY_NOTE"."NOTE_TEXT_TEXT", 32750) AS "C10", 
REGEXP_REPLACE( 
    REGEXP_REPLACE( 
        replace( 
            replace( 
                LEFT("DW_POLICY_NOTE"."NOTE_TEXT_TEXT", 32750),
            '&nbsp;', ' '),
        '&#39;', ' '),
    '<.*?>|&.*;', ''),
 '[\r\n]+', '
') AS "C11",


<p>Good morning,</p>  <p>Dot Systems Inc- 729178 has a BWC balance in the amount of $12. This employer paid their BWC installment on 8/26/22, yet they are lapsed with a $12 late fee. Can you please review and advise?</p>  <p>Pam Bracken Sheakley Uniservice</p>  <p>&nbsp;</p>  <p>I sent an email to the BWC collections box....</p>  <p>Hello,</p>  <p>The employer&rsquo;s 8-22-2022 installment payment was posted timely but got applied to the PY21 true up payment.</p>  <p>Please reapply the payment posted on 8-26-2022 to the 8-22-2022 installment payment.</p>  <p>Please remove the lapse and late payment fee.</p>  <p>Thank you for your time and assistance.</p>  <p><strong>Debra Mitchell</strong></p>  <p>&nbsp;</p>