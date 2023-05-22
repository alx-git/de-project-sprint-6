with user_group_log as (
    select luga.hk_group_id, count(DISTINCT luga.hk_user_id) as cnt_added_users
    from KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.l_user_group_activity luga 
    left join KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    left join KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.h_groups hg on luga.hk_group_id = hg.hk_group_id 
    where sah.event = 'add'
    and luga.hk_group_id in (select hk_group_id from KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.h_groups order by registration_dt limit 10)
    group by luga.hk_group_id
)
,user_group_messages as (
    select lgd.hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.l_groups_dialogs lgd
    left join KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
    group by lgd.hk_group_id
)
select 
ugl.hk_group_id,
ugl.cnt_added_users,
ugm.cnt_users_in_group_with_messages,
ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc