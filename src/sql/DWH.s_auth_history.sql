drop table if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.s_auth_history;
create table KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null constraint fk_s_auth_history_l_user_group_activity references KOVALCHUKALEXANDERGOOGLEMAILCOM__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event varchar(10) not null,
event_dt timestamp not null,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);