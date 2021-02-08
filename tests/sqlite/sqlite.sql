/*
* @Author: chunyang.xu
* @Date:   2020-06-04 17:57:10
* @Email:  398745129@qq.com
* @Last Modified time: 2021-02-08 13:53:48
*/

###
--【1user_daily_info】
--筛选
create temporary table temp_events_ev as
select *
from test_xu
;

select * from temp_events_ev;

###

###
--【2user verbose1】
--筛选
create temporary table temp_events_ev as
select *
from test_xu
where name = $name
;

drop table if exists temp_xu;
--create测试我的测试
create table temp_xu as
select * from temp_events_ev;
###

###
--【1user_daily_info verbose2】
--筛选
create temporary table temp_events_ev as
select *
from test_xu
;

select * from temp_events_ev;

###