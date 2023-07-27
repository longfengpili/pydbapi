/*
* @Author: longfengpili
* @Date:   2023-06-02 15:27:41
* @Last Modified by:   longfengpili
* @Last Modified time: 2023-07-27 15:41:50
*/


#【arguments】#
date_min = '2020-02-12'
date_max = '2020-02-13'
fpid = ('12551515', '44546456')
#【arguments】#


###
--【1user_daily_info】
--筛选
create temp table temp_events_ev as
select *
from raw_data_aniland.sdk_data
where sts_data >= $date_min and sts_data <= $date_max
limit 10
;

select fpid, sts_data, msg_type, name, level from temp_events_ev;

###

###
--【2user show progress】
--筛选
create temp table temp_events_ev as
select *
from raw_data_aniland.sdk_data
where trunc(sts_data) >= ($date_min) and trunc(sts_data) <= $date_max::datetime
and ev in ('custom', 'kpi')
and fpid is not null and sts_data is not null
and fpid in $fpid;

--select
create temp table temp_x as
select * from temp_events_ev limit 10;
###


