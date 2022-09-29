with vars as
    (
        select parse_timestamp('%Y-%m-%d', '2022-05-01') as gainyprofiles_earliest_created_from
             , parse_timestamp('%Y-%m-%d', '2022-09-30') as gainyprofiles_latest_created_upto
             , True                                      as gainyprofiles_excludeistest
             , 0                                         as data_variant                 -- 0:screencontroller ; 1:event ; 2:event__screenconroller
             , True                                      as drop_consequtive_repetitions -- if we don't want to see consequtive swipes as separate steps for an example
             , ARRAY <STRING>[]                          as filter_events_include
             , ARRAY <STRING>[]                          as filter_events_exclude
    )
   , gainy_legal_uid_profileid as
    (
        with q_profiles as
                 (
                     select user_id                                                                                   as uid
                          , id                                                                                        as profile_id
                          , created_at
                          , row_number() over (partition by user_id order by created_at asc, _sdc_extracted_at asc)   as rn_earliest_created
                          , row_number() over (partition by user_id order by created_at desc, _sdc_extracted_at desc) as rn_latest_created
                          , row_number() over (partition by user_id order by _sdc_extracted_at desc, created_at desc) as rn_latest_active
                          , (sum(case when coalesce(is_test, false) then 1 else 0 end)
                                 over (partition by user_id)) >
                            0                                                                                         as is_test
                     from `gainyapp.gainyapp_integration_production.analytics_profiles`
                 )

        select q0.uid
             , created_at_earliest_created as created_at
             , q0.profile_id_latest_active as profile_id
             , is_test
        from (
                 select q0.uid
                      , q0.is_test
                      , q0.profile_id as profile_id_earliest_created
                      , q0.created_at as created_at_earliest_created
                      , q2.profile_id as profile_id_latest_created
                      , q2.created_at as created_at_latest_created
                      , q1.profile_id as profile_id_latest_active
                      , q1.created_at as created_at_latest_active
                      , q3.dt         as uid_at_earliest_event_occured
                 from (
                          select *
                          from q_profiles
                          where rn_earliest_created = 1
                      ) as q0
                          join (
                                   select *
                                   from q_profiles
                                   where rn_latest_active = 1
                               ) as q1 on q1.uid = q0.uid
                          join (
                                   select *
                                   from q_profiles
                                   where rn_latest_created = 1
                               ) as q2 on q2.uid = q0.uid
                          join ( -- some events of onboarding technically could appear earlier that created_at in profile (coz of onboarding's events-bucket with uid sent after profile registered) - and that says that user came earlier in fact
                                   select uid, min(dt) as dt
                                   from (
                                            SELECT (
                                                       select p.value.string_value
                                                       from unnest(a.event_params) as p
                                                       where lower(p.key) = 'uid'
                                                   )                                     AS uid
                                                 , (TIMESTAMP_MICROS(a.event_timestamp)) AS dt
                                            FROM `gainyapp.analytics_287066082.events_*` AS a,
                                                 vars
                                            WHERE exists (
                                                             select 1
                                                             from unnest(a.event_params) as p
                                                             where lower(p.key) = 'uid'
                                                         ) -- only event records with 'uid' param inside
                                              AND (
                                                      select p.value.string_value
                                                      from unnest(a.event_params) as p
                                                      where lower(p.key) = 'source'
                                                  ) = 'app' -- 'app' only (just in case)
                                        )
                                   group by uid
                               ) as q3 on q3.uid = q0.uid
             ) as q0
           , vars
        where least(created_at_earliest_created
                  , uid_at_earliest_event_occured) >= vars.gainyprofiles_earliest_created_from
          and created_at_latest_created <= vars.gainyprofiles_latest_created_upto
          and (not (vars.gainyprofiles_excludeistest)
            OR not (is_test))
    )

   , events as (
                   SELECT (
                              select p.value.string_value from unnest(a.event_params) as p where lower(p.key) = 'uid'
                          )                                     AS uid
                        , (TIMESTAMP_MICROS(a.event_timestamp)) AS dt
                        , a.event_name                          AS event_name
                        , a.event_params
                        , (
                              select p.value.string_value
                              from unnest(a.event_params) as p
                              where lower(p.key) = 'firebase_screen_class'
                          )                                     AS firebase_screen_class
                   FROM `gainyapp.analytics_287066082.events_*` AS a,
                        vars
                   WHERE (
                             select p.value.string_value from unnest(a.event_params) as p where lower(p.key) = 'source'
                         ) = 'app'                                          -- 'app' only (just in case)
                     AND TIMESTAMP_MICROS(a.event_timestamp) >= gainyprofiles_earliest_created_from
                     AND (ARRAY_LENGTH(vars.filter_events_include) = 0
                       OR event_name IN unnest(vars.filter_events_include)) -- filter events by list (include)
                     AND (ARRAY_LENGTH(vars.filter_events_exclude) = 0
                       OR event_name NOT IN unnest(vars.filter_events_exclude)) -- filter events by list
               )
   , purchased_users as
    (
        select uid,
               STRING_AGG(
                       (
                           select p.value.string_value || ' on ' || DATE_TRUNC(events.dt, DAY)
                           from unnest(events.event_params) as p
                           where p.key = 'productId'
                       )
                   ) as product_ids
        from events
                 join gainy_legal_uid_profileid using (uid)
        where event_name = 'purchase_completed'
        group by uid
    )
   , user_stats_config as
    (
        select uid,
               case
                   when purchased_users.uid is not null
                       then uid
                   else 'not_purchased_' || extract(month from gainy_legal_uid_profileid.created_at)
                   end     as stats_id,
               product_ids as stats_description,
        from gainy_legal_uid_profileid
                 left join purchased_users using (uid)
    )
--    , session_events as
--     (
--         select uid,
--                stats_id,
--                dt,
--                event_name,
--                event_params,
--                firebase_screen_class,
--                sum(new_session_start) over (partition by uid order by dt) as session_id
--         from (
--                  select events.*,
--                         cast(COALESCE(
--                                     date_diff(dt, lag(dt) over (partition by uid order by dt), minute) >
--                                     5,
--                                     true) as int) as new_session_start,
--                  from events
--                  order by uid, dt
--              ) t
--                  join user_stats_config using (uid)
--                  left join purchased_users using (uid)
--     )
   , stats_config as
    (
        select distinct stats_id, stats_description
        from user_stats_config
    )
--    , session_duration as
--     (
--         select uid,
--                stats_id,
--                session_id,
--                DATETIME_DIFF(max(dt), min(dt), SECOND) as duration
--         from session_events
--         where event_name not in ('app_close', 'app_open')
--         group by stats_id, uid, session_id
--     )
--    , session_cnt as
--     (
--         with t as
--                  (
--                      select uid,
--                             stats_id,
--                             count(distinct session_id) as session_cnt,
--                      from session_events
--                      group by uid, stats_id
--                  )
--         select stats_id,
--                session_cnt_sum,
--                session_cnt_avg,
--                session_cnt_median,
--         from (
--                  select stats_id,
--                         sum(session_cnt) as session_cnt_sum,
--                         avg(session_cnt) as session_cnt_avg
--                  from t
--                  group by stats_id
--              ) group_metrics
--                  join (
--
--                           select distinct stats_id,
--                                           PERCENTILE_DISC(session_cnt, 0.5) OVER (PARTITION BY stats_id) as session_cnt_median,
--                           from t
--                       ) window_metrics using (stats_id)
--     )
--    , session_metrics as
--     (
--         select stats_id,
--                session_cnt_sum,
--                session_cnt_avg,
--                session_cnt_median,
--                session_duration_sum,
--                session_duration_avg,
--                session_duration_median,
--                session_duration_percentile_75,
--                session_duration_percentile_90,
--         from (
--                  select stats_id,
--                         sum(duration) as session_duration_sum,
--                         avg(duration) as session_duration_avg
--                  from session_duration
--                  group by stats_id
--              ) group_metrics
--                  join (
--
--                           select distinct stats_id,
--                                           PERCENTILE_DISC(duration, 0.5) OVER (PARTITION BY stats_id)  as session_duration_median,
--                                           PERCENTILE_DISC(duration, 0.75) OVER (PARTITION BY stats_id) as session_duration_percentile_75,
--                                           PERCENTILE_DISC(duration, 0.9) OVER (PARTITION BY stats_id)  as session_duration_percentile_90,
--                           from session_duration
--                       ) window_metrics using (stats_id)
--                  join session_cnt using (stats_id)
--     )

--    , user_metrics as
--     (
--         with ttf_metrics as
--                  (
--                      select uid,
--                             stats_id,
--                             event_name,
--                             count(collection_id)          as cnt,
--                             count(distinct collection_id) as cnt_distinct,
--                      from (
--                               select uid,
--                                      stats_id,
--                                      event_name,
--                                      (
--                                          select p.value.int_value
--                                          from unnest(events.event_params) as p
--                                          where p.key = 'collectionID'
--                                      ) as collection_id
--                               from events
--                                        join user_stats_config using (uid)
--                               where event_name in ('ttf_view', 'wl_add')
--                           ) ttf_events
--                      group by uid, stats_id, event_name
--                  )
--         select stats_id,
--                ttf_view_cnt_avg,
--                ttf_view_cnt_median,
--                ttf_view_cnt_percentile_75,
--                ttf_view_cnt_percentile_90,
--                ttf_view_cnt_distinct_avg,
--                ttf_view_cnt_distinct_median,
--                ttf_view_cnt_distinct_percentile_75,
--                ttf_view_cnt_distinct_percentile_90,
--                ttf_wl_add_cnt_distinct_avg,
--                ttf_wl_add_cnt_distinct_median,
--                ttf_wl_add_cnt_distinct_percentile_75,
--                ttf_wl_add_cnt_distinct_percentile_90,
--         from stats_config
--                  left join (
--                                select user_stats_config.stats_id,
--                                       avg(coalesce(cnt, 0)) as ttf_view_cnt_avg,
--                                       avg(coalesce(cnt_distinct, 0)) as ttf_view_cnt_distinct_avg,
--                                from user_stats_config
--                                         left join ttf_metrics
--                                                   on ttf_metrics.uid = user_stats_config.uid
--                                                       and ttf_metrics.event_name = 'ttf_view'
--                                group by user_stats_config.stats_id
--                            ) ttf_view_grouped_metrics using (stats_id)
--                  left join (
--                                select distinct user_stats_config.stats_id,
--                                                PERCENTILE_DISC(coalesce(cnt, 0), 0.5) OVER (PARTITION BY user_stats_config.stats_id) as ttf_view_cnt_median,
--                                                PERCENTILE_DISC(coalesce(cnt, 0), 0.75) OVER (PARTITION BY user_stats_config.stats_id) as ttf_view_cnt_percentile_75,
--                                                PERCENTILE_DISC(coalesce(cnt, 0), 0.9) OVER (PARTITION BY user_stats_config.stats_id) as ttf_view_cnt_percentile_90,
--                                                PERCENTILE_DISC(coalesce(cnt_distinct, 0), 0.5) OVER (PARTITION BY user_stats_config.stats_id) as ttf_view_cnt_distinct_median,
--                                                PERCENTILE_DISC(coalesce(cnt_distinct, 0), 0.75) OVER (PARTITION BY user_stats_config.stats_id) as ttf_view_cnt_distinct_percentile_75,
--                                                PERCENTILE_DISC(coalesce(cnt_distinct, 0), 0.9) OVER (PARTITION BY user_stats_config.stats_id) as ttf_view_cnt_distinct_percentile_90,
--                                from user_stats_config
--                                         left join ttf_metrics
--                                                   on ttf_metrics.uid = user_stats_config.uid
--                                                       and ttf_metrics.event_name = 'ttf_view'
--                            ) ttf_view_window_metrics using (stats_id)
--                  left join (
--                                select user_stats_config.stats_id,
--                                       avg(coalesce(cnt_distinct, 0)) as ttf_wl_add_cnt_distinct_avg,
--                                from user_stats_config
--                                         left join ttf_metrics
--                                                   on ttf_metrics.uid = user_stats_config.uid
--                                                       and ttf_metrics.event_name = 'wl_add'
--                                group by user_stats_config.stats_id
--                            ) wl_add_grouped_metrics using (stats_id)
--                  left join (
--                                select distinct user_stats_config.stats_id,
--                                                PERCENTILE_DISC(coalesce(cnt_distinct, 0), 0.5)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as ttf_wl_add_cnt_distinct_median,
--                                                PERCENTILE_DISC(coalesce(cnt_distinct, 0), 0.75)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as ttf_wl_add_cnt_distinct_percentile_75,
--                                                PERCENTILE_DISC(coalesce(cnt_distinct, 0), 0.9)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as ttf_wl_add_cnt_distinct_percentile_90,
--                                from user_stats_config
--                                         left join ttf_metrics
--                                                   on ttf_metrics.uid = user_stats_config.uid
--                                                       and ttf_metrics.event_name = 'wl_add'
--                            ) wl_add_window_metrics using (stats_id)
--
--     )

--    , user_metrics as
--     (
--         with ttf_metrics as
--                  (
--                      select uid,
--                             stats_id,
--                             event_name,
--                             count(collection_id)          as cnt,
--                             count(distinct collection_id) as cnt_distinct,
--                      from (
--                               select uid,
--                                      stats_id,
--                                      event_name,
--                                      (
--                                          select p.value.int_value
--                                          from unnest(events.event_params) as p
--                                          where p.key = 'collectionID'
--                                      ) as collection_id
--                               from events
--                                        join user_stats_config using (uid)
--                               where event_name in ('invest_pressed_ttf')
--                           ) ttf_events
--                      group by uid, stats_id, event_name
--                  ),
--              event_metrics as
--                  (
--                      select uid,
--                             stats_id,
--                             event_name,
--                             count(uid) as cnt,
--                             1          as cnt_distinct,
--                      from (
--                               select uid,
--                                      stats_id,
--                                      event_name,
--                               from events
--                                        join user_stats_config using (uid)
--                               where event_name in ('discover_collections_pressed', 'recommended_collection_pressed',
--                                                    'your_collection_pressed')
--                           ) ttf_events
--                      group by uid, stats_id, event_name
--              )
--         select stats_id,
--                ttf_invest_pressed_ttf_cnt_distinct_avg,
--                discover_collections_pressed_cnt_distinct_avg,
--                recommended_collection_pressed_cnt_avg,
--                recommended_collection_pressed_cnt_distinct_avg,
--                recommended_collection_pressed_cnt_median,
--                recommended_collection_pressed_cnt_percentile_75,
--                recommended_collection_pressed_cnt_percentile_90,
--                your_collection_pressed_cnt_avg,
--                your_collection_pressed_cnt_distinct_avg,
--         from stats_config
--                  left join (
--                                select user_stats_config.stats_id,
--                                       avg(coalesce(cnt_distinct, 0)) as ttf_invest_pressed_ttf_cnt_distinct_avg,
--                                from user_stats_config
--                                         left join ttf_metrics
--                                                   on ttf_metrics.uid = user_stats_config.uid
--                                                       and ttf_metrics.event_name = 'invest_pressed_ttf'
--                                group by user_stats_config.stats_id
--                            ) invest_pressed_ttf_grouped_metrics using (stats_id)
--                  left join (
--                                select user_stats_config.stats_id,
--                                       avg(coalesce(em0.cnt_distinct, 0)) as discover_collections_pressed_cnt_distinct_avg,
--                                       avg(coalesce(em1.cnt, 0))          as recommended_collection_pressed_cnt_avg,
--                                       avg(coalesce(em1.cnt_distinct, 0)) as recommended_collection_pressed_cnt_distinct_avg,
--                                       avg(coalesce(em2.cnt, 0))          as your_collection_pressed_cnt_avg,
--                                       avg(coalesce(em2.cnt_distinct, 0)) as your_collection_pressed_cnt_distinct_avg,
--                                from user_stats_config
--                                         left join event_metrics em0
--                                                   on em0.uid = user_stats_config.uid
--                                                       and em0.event_name = 'discover_collections_pressed'
--                                         left join event_metrics em1
--                                                   on em1.uid = user_stats_config.uid
--                                                       and em1.event_name = 'recommended_collection_pressed'
--                                         left join event_metrics em2
--                                                   on em2.uid = user_stats_config.uid
--                                                       and em2.event_name = 'your_collection_pressed'
--                                group by user_stats_config.stats_id
--                            ) t using (stats_id)
--                  left join (
--                                select distinct user_stats_config.stats_id,
--                                                PERCENTILE_DISC(COALESCE(cnt, 0), 0.5)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as recommended_collection_pressed_cnt_median,
--                                                PERCENTILE_DISC(COALESCE(cnt, 0), 0.75)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as recommended_collection_pressed_cnt_percentile_75,
--                                                PERCENTILE_DISC(COALESCE(cnt, 0), 0.9)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as recommended_collection_pressed_cnt_percentile_90,
--                                from user_stats_config
--                                         left join event_metrics
--                                                   on event_metrics.uid = user_stats_config.uid
--                                                       and event_metrics.event_name = 'recommended_collection_pressed'
--                            ) recommended_collection_pressed_window_metrics using (stats_id)
--     )

--    , user_metrics as
--     (
--         with event_metrics as
--                  (
--                      select uid,
--                             stats_id,
--                             event_name,
--                             count(uid) as cnt,
--                             1          as cnt_distinct,
--                      from (
--                               select uid,
--                                      stats_id,
--                                      event_name,
--                               from events
--                                        join user_stats_config using (uid)
--                               where event_name in ('unlock_details', 'show_purchase_view')
--                           ) ttf_events
--                      group by uid, stats_id, event_name
--                  )
--         select stats_id,
--                unlock_details_cnt_avg,
--                unlock_details_cnt_distinct_avg,
--                unlock_details_cnt_median,
--                unlock_details_cnt_percentile_75,
--                unlock_details_cnt_percentile_90,
--                show_purchase_view_cnt_avg,
--                show_purchase_view_cnt_distinct_avg,
--         from stats_config
--                  left join (
--                                select user_stats_config.stats_id,
--                                       avg(coalesce(em3.cnt, 0))          as unlock_details_cnt_avg,
--                                       avg(coalesce(em3.cnt_distinct, 0)) as unlock_details_cnt_distinct_avg,
--                                       avg(coalesce(em4.cnt, 0))          as show_purchase_view_cnt_avg,
--                                       avg(coalesce(em4.cnt_distinct, 0)) as show_purchase_view_cnt_distinct_avg,
--                                from user_stats_config
--                                         left join event_metrics em3
--                                                   on em3.uid = user_stats_config.uid
--                                                       and em3.event_name = 'unlock_details'
--                                         left join event_metrics em4
--                                                   on em4.uid = user_stats_config.uid
--                                                       and em4.event_name = 'show_purchase_view'
--                                group by user_stats_config.stats_id
--                            ) t using (stats_id)
--                  left join (
--                                select distinct user_stats_config.stats_id,
--                                                PERCENTILE_DISC(COALESCE(cnt, 0), 0.5)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as unlock_details_cnt_median,
--                                                PERCENTILE_DISC(COALESCE(cnt, 0), 0.75)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as unlock_details_cnt_percentile_75,
--                                                PERCENTILE_DISC(COALESCE(cnt, 0), 0.9)
--                                                                OVER (PARTITION BY user_stats_config.stats_id) as unlock_details_cnt_percentile_90,
--                                from user_stats_config
--                                         left join event_metrics
--                                                   on event_metrics.uid = user_stats_config.uid
--                                                       and event_metrics.event_name = 'unlock_details'
--                            ) unlock_details_window_metrics using (stats_id)
--     )

   , user_metrics as
    (
        with event_metrics as
                 (
                     select uid,
                            stats_id,
                            event_name,
                            count(uid) as cnt,
                            1          as cnt_distinct,
                     from (
                              select uid,
                                     stats_id,
                                     event_name,
                              from events
                                       join user_stats_config using (uid)
                              where event_name in ('try_premium_tapped',
                                                   'purchase_details',
                                                   'portfolio_plaid_link_success')
                          ) ttf_events
                     group by uid, stats_id, event_name
                 )
        select stats_id,
               users_cnt,
               try_premium_tapped_cnt_avg,
               try_premium_tapped_cnt_distinct_avg,
               purchase_details_cnt_avg,
               purchase_details_cnt_distinct_avg,
portfolio_plaid_link_success_cnt_distinct_avg,
        from stats_config
                 left join (
                               select user_stats_config.stats_id,
                                      count(distinct user_stats_config.uid) as users_cnt,
                                      avg(coalesce(em5.cnt, 0))          as try_premium_tapped_cnt_avg,
                                      avg(coalesce(em5.cnt_distinct, 0)) as try_premium_tapped_cnt_distinct_avg,
                                      avg(coalesce(em6.cnt, 0))          as purchase_details_cnt_avg,
                                      avg(coalesce(em6.cnt_distinct, 0)) as purchase_details_cnt_distinct_avg,
                                      avg(coalesce(em7.cnt_distinct, 0)) as portfolio_plaid_link_success_cnt_distinct_avg,
                               from user_stats_config
                                        left join event_metrics em5
                                                  on em5.uid = user_stats_config.uid
                                                      and em5.event_name = 'try_premium_tapped'
                                        left join event_metrics em6
                                                  on em6.uid = user_stats_config.uid
                                                      and em6.event_name = 'purchase_details'
                                        left join event_metrics em7
                                                  on em7.uid = user_stats_config.uid
                                                      and em7.event_name = 'portfolio_plaid_link_success'
                               group by user_stats_config.stats_id
                           ) t using (stats_id)
                 left join (
                               select distinct user_stats_config.stats_id,
                                               PERCENTILE_DISC(COALESCE(cnt, 0), 0.5)
                                                               OVER (PARTITION BY user_stats_config.stats_id) as try_premium_tapped_cnt_median,
                                               PERCENTILE_DISC(COALESCE(cnt, 0), 0.75)
                                                               OVER (PARTITION BY user_stats_config.stats_id) as try_premium_tapped_cnt_percentile_75,
                                               PERCENTILE_DISC(COALESCE(cnt, 0), 0.9)
                                                               OVER (PARTITION BY user_stats_config.stats_id) as try_premium_tapped_cnt_percentile_90,
                               from user_stats_config
                                        left join event_metrics
                                                  on event_metrics.uid = user_stats_config.uid
                                                      and event_metrics.event_name = 'try_premium_tapped'
                           ) try_premium_tapped_window_metrics using (stats_id)
                 left join (
                               select distinct user_stats_config.stats_id,
                                               PERCENTILE_DISC(COALESCE(cnt, 0), 0.5)
                                                               OVER (PARTITION BY user_stats_config.stats_id) as purchase_details_cnt_median,
                                               PERCENTILE_DISC(COALESCE(cnt, 0), 0.75)
                                                               OVER (PARTITION BY user_stats_config.stats_id) as purchase_details_cnt_percentile_75,
                                               PERCENTILE_DISC(COALESCE(cnt, 0), 0.9)
                                                               OVER (PARTITION BY user_stats_config.stats_id) as purchase_details_cnt_percentile_90,
                               from user_stats_config
                                        left join event_metrics
                                                  on event_metrics.uid = user_stats_config.uid
                                                      and event_metrics.event_name = 'purchase_details'
                           ) purchase_details_window_metrics using (stats_id)

    )

-- select stats_id,
--        stats_description,
--        session_cnt_sum,
--        session_cnt_avg,
--        session_cnt_median,
--        session_duration_sum,
--        session_duration_avg,
--        session_duration_median,
--        session_duration_percentile_75,
--        session_duration_percentile_90,
-- from session_metrics
--          join stats_config using (stats_id)
-- order by stats_id;

-- select stats_id,
--        ttf_view_cnt_avg,
--        ttf_view_cnt_median,
--        ttf_view_cnt_percentile_75,
--        ttf_view_cnt_percentile_90,
--        ttf_view_cnt_distinct_avg,
--        ttf_view_cnt_distinct_median,
--        ttf_view_cnt_distinct_percentile_75,
--        ttf_view_cnt_distinct_percentile_90,
--        ttf_wl_add_cnt_distinct_avg,
--        ttf_wl_add_cnt_distinct_median,
--        ttf_wl_add_cnt_distinct_percentile_75,
--        ttf_wl_add_cnt_distinct_percentile_90,
-- from user_metrics0
--          join stats_config using (stats_id)
-- order by stats_id;

-- select stats_id,
--        ttf_invest_pressed_ttf_cnt_distinct_avg,
--        discover_collections_pressed_cnt_distinct_avg,
--        recommended_collection_pressed_cnt_avg,
--        recommended_collection_pressed_cnt_distinct_avg,
--        recommended_collection_pressed_cnt_median,
--        recommended_collection_pressed_cnt_percentile_75,
--        recommended_collection_pressed_cnt_percentile_90,
--        your_collection_pressed_cnt_avg,
--        your_collection_pressed_cnt_distinct_avg,
-- from user_metrics1
--          join stats_config using (stats_id)
-- order by stats_id;

select user_metrics.*
from user_metrics
         join stats_config using (stats_id)
order by stats_id;
