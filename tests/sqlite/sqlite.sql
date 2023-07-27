/*
* @Author: longfengpili
* @Date:   2023-06-02 15:27:41
* @Last Modified by:   longfengpili
* @Last Modified time: 2023-07-27 15:41:52
*/

#【arguments】#
end_date = today
start_date = end_date - timedelta(days=30)
report_multiple_index = 'date, source(128), country(128), os(128)'
#【arguments】#

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
--          【2user verbose1 epass】
-- 筛选
create temporary table temp_events_ev as
select *
from test_xu
where name = $name
;

--test
select * from table;

-- test
-- select * from temp_events_ev;

drop table if exists temp_xu;

--create测试我的测试
create table if not exists temp_xu as
select * from temp_events_ev;
###

###
--【1user_daily_info verbose2】
--筛选1
create temporary table temp_events_ev_1 as
select *
from test_xu
;

--筛选2
create temporary table temp_events_ev_2 as
select *
from test_xu
;

--筛选3
create temporary table temp_events_ev_3 as
select *
from test_xu
;
###


### 
--【bmsn jp index verbose1】
-- drop
drop index multiple_index on temp_xu;
-- create
create index multiple_index on temp_xu($report_multiple_index);
-- show
show index from temp_xu;
###
