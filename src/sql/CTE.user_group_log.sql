with user_group_log as (
    select luga.hk_group_id, count(DISTINCT luga.hk_user_id) as cnt_added_users
    from KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.l_user_group_activity luga 
    left join KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    left join KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id 
    where sah.event = 'add'
    and luga.hk_group_id in (select hk_group_id from KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.h_groups order by registration_dt limit 10)
    group by luga.hk_group_id
)
select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;