/*
* @Author: chunyang.xu
* @Date:   2020-06-04 17:57:10
* @Email:  398745129@qq.com
* @Last Modified time: 2020-06-15 16:53:43
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
--【2user verbose】
--筛选
create temporary table temp_events_ev as
select *
from test_xu
where name = $name
;

drop table if exists temp_xu;
--create
create table temp_xu as
select * from temp_events_ev;
###


