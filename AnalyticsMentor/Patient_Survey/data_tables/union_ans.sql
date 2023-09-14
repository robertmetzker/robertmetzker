SELECT
	 sr.state
	,st.region
	,sr.releaseperiod
	,q.questionnum
	,sr.measureid
	,m.measure
	,m.type
	,q.question
	,'B' 					as Box
	,q.bottomboxanswer 		as answer_txt
	,sr.bottomboxpercentage as answer_pct
	,nr.bottomboxpercentage as box_avg
from state_results   sr
inner join states st
	on (sr.state = st.state )
inner join questions q
	on (sr.measureid = q.measureid)
inner join measures M
	on (q.measureid = m.measureid)
inner join national_results nr
	on (sr.releaseperiod = nr.releaseperiod and sr.measureid = nr.measureid )
UNION all
SELECT
	 sr.state
	,st.region
	,sr.releaseperiod
	,q.questionnum
	,sr.measureid
	,m.measure
	,m.type
	,q.question
	,'M' 					as Box
	,q.middleboxanswer 		as answer_txt
	,sr.middleboxpercentage as answer_pct
	,nr.middleboxpercentage as box_avg
from state_results   sr
inner join states st
	on (sr.state = st.state )
inner join questions q
	on (sr.measureid = q.measureid)
inner join measures M
	on (q.measureid = M.measureid)
inner join national_results nr
	on (sr.releaseperiod = nr.releaseperiod and sr.measureid = nr.measureid )
UNION ALL
SELECT
	 sr.state
	,st.region
	,sr.releaseperiod
	,q.questionnum
	,sr.measureid
	,m.measure
	,m.type
	,q.question
	,'T' 				 as Box
	,q.topboxanswer 	 as answer_txt
	,sr.topboxpercentage as answer_pct
	,nr.topboxpercentage as box_avg
from state_results   sr
inner join states st
	on (sr.state = st.state )
inner join questions q
	on (sr.measureid = q.measureid)
inner join measures M
	on (q.measureid = m.measureid)
inner join national_results nr
	on (sr.releaseperiod = nr.releaseperiod and sr.measureid = nr.measureid )
;




SELECT
	 sr.state
	,st.region
	,sr.releaseperiod
	,q.questionnum
	,sr.measureid
	,m.measure
	,m.type
	,q.question
	,unnest( array['B','M','T'] )  															as Box
	,unnest( array[q.bottomboxanswer, q.middleboxanswer, q.topboxanswer ] )  				as answer_txt
	,unnest( array[sr.bottomboxpercentage, sr.middleboxpercentage, sr.topboxpercentage ] )  as answer_pct
	,unnest( array[nr.bottomboxpercentage, nr.middleboxpercentage, nr.topboxpercentage ] )  as box_avg
from state_results   sr
inner join states st
	on (sr.state = st.state )
inner join questions q
	on (sr.measureid = q.measureid)
inner join measures M
	on (q.measureid = m.measureid)
inner join national_results nr
	on (sr.releaseperiod = nr.releaseperiod and sr.measureid = nr.measureid );
