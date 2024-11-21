/*
* @Author: longfengpili
* @Date:   2023-06-02 15:27:41
* @Last Modified by:   longfengpili
* @Last Modified time: 2024-11-21 14:09:59
*/

###
with user_info_front as
(
select time, uid
from logs.role_info
where dt >= '20220215' and dt <= '20220222'
and json_extract_scalar(detail, '$.server_time') >= '2022-02-16'
and json_extract_scalar(detail, '$.server_time') <= concat('2022-02-16', ' ', '23:59:59')
and detail != ''
-- and json_extract_scalar(detail, '$.uid') = '1001188321000019'
),

event_itemvaried as
(select cast(date_add('hour', -"#zone_offset", "#event_time") as varchar) as time,
date(date_add('hour', -"#zone_offset", "#event_time")) as date,
serveridofevent,
date_add('hour', -"#zone_offset", "#event_time") as datetime,
uid,
'unknown' as role_name
from logs.event_itemvaried
where dt >= '20220215' and dt <= '20220222'
and date_format(date_add('hour', -"#zone_offset", "#event_time"), '%Y-%m-%d') >= '2022-02-16'
and date_format(date_add('hour', -"#zone_offset", "#event_time"), '%Y-%m-%d') <= '2022-02-16'
),

event_gemvaried as
(select cast(date_add('hour', -"#zone_offset", "#event_time") as varchar) as time,
date(date_add('hour', -"#zone_offset", "#event_time")) as date,
serveridofevent,
date_add('hour', -"#zone_offset", "#event_time") as datetime,
uid
from logs.event_gemvaried
where dt >= '20220215' and dt <= '20220222'
and date_format(date_add('hour', -"#zone_offset", "#event_time"), '%Y-%m-%d') >= '2022-02-16'
and date_format(date_add('hour', -"#zone_offset", "#event_time"), '%Y-%m-%d') <= '2022-02-16'
),

user_info_back as(
select * from event_itemvaried
union all
select * from event_gemvaried
),

user_info as(
select * from user_info_front
union all
select * from user_info_back
)

select * from user_info;

###

###
select * from user_info;
###