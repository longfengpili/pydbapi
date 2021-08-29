/*
* @Author: chunyang.xu
* @Date:   2020-06-04 17:57:10
* @Email:  398745129@qq.com
* @Last Modified time: 2021-08-29 12:19:22
*/

#【arguments】# 
level = 10
fpid = ('301121904456', '30111300420')
date_max = now + timedelta(hours=-10)
date_min = date_max + timedelta(days=-10)
#【arguments】# 

### 
--【1user_daily_info verbose】
--筛选
create temp table temp_events_ev as
select *
from raw_data_aniland.sdk_data
where trunc(sts_data) >= $date_min and trunc(sts_data) <= $date_max::datetime
and level = $level
limit 1000
;

select fpid, sts_data, msg_type, name, level from temp_events_ev;

--再次筛选
create temp table temp_x as
select fpid, sts_data, msg_type, name, level from temp_events_ev;

--show
select * from temp_x;

### 

### 
--【2user verbose】
--筛选
create temp table temp_events_ev as
select *
from raw_data_aniland.sdk_data
where ev in ('custom', 'kpi')
and fpid in $fpid;

--select
select fpid, sts_data, msg_type, name, level from temp_events_ev limit 10;
###

###
select * from report_data_aniland.test_xu;
###
