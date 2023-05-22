drop table if exists KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.group_log;
create table KOVALCHUKALEXANDERGOOGLEMAILCOM__STAGING.group_log
(
    group_id int not null primary key,
    user_id int not null,
    user_id_from int,
    event varchar(10) not null,
    datetime timestamp not null
)