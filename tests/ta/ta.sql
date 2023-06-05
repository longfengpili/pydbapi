/*
* @Author: longfengpili
* @Date:   2023-06-05 18:45:47
* @Last Modified by:   longfengpili
* @Last Modified time: 2023-06-05 18:57:57
*/

#【arguments】#
part_date = "'2023-06-04'"
event_name = "'Payment'"
#【arguments】#

###
--【1user_daily_info】
with test_payment as
(select "#user_id",
level,
openid,
date,
money
from v_event_19
where "$part_event" = $event_name and "$part_date" = $part_date
)

select * from test_payment limit 10

###