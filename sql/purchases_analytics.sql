select profile_scoring_settings.*
from app.profile_scoring_settings
         join app.profiles on profiles.id = profile_id
where profiles.user_id in
      ('3wuhWkjgaPTi42pQoXlcRSiujOH2', 'H5gbDtd8djNfwegUjFT61WeYwxo1', 'hhybvmGt4gTJgHE77oqiRZ87fXC2');


select date_trunc('month', profiles.created_at)::date,
       avg(risk_level)                    as risk_level_avg,
       avg(average_market_return)         as average_market_return_avg,
       avg(investment_horizon)            as investment_horizon_avg,
       avg(damage_of_failure)             as damage_of_failure_avg,
       avg(if_market_drops_20_i_will_buy) as if_market_drops_20_i_will_buy_avg,
       avg(if_market_drops_40_i_will_buy) as if_market_drops_40_i_will_buy_avg,
       avg(risk_score)                    as risk_score_avg
from app.profile_scoring_settings
         join app.profiles on profiles.id = profile_id
where profiles.user_id not in
      ('3wuhWkjgaPTi42pQoXlcRSiujOH2', 'H5gbDtd8djNfwegUjFT61WeYwxo1', 'hhybvmGt4gTJgHE77oqiRZ87fXC2')
  and not (
            email ilike '%gainy.app'
        or email ilike '%test%'
        or last_name ilike '%test%'
        or first_name ilike '%test%')
group by date_trunc('month', profiles.created_at);


with raw_stats as
         (
             select date_trunc('month', profiles.created_at)::date as date,
                    unexpected_purchases_source,
                    count(distinct profile_id)                     as cnt
             from app.profile_scoring_settings
                      join app.profiles on profiles.id = profile_id
             where profiles.user_id not in
                   ('3wuhWkjgaPTi42pQoXlcRSiujOH2', 'H5gbDtd8djNfwegUjFT61WeYwxo1', 'hhybvmGt4gTJgHE77oqiRZ87fXC2')
               and not (
                         email ilike '%gainy.app'
                     or email ilike '%test%'
                     or last_name ilike '%test%'
                     or first_name ilike '%test%')
             group by date_trunc('month', profiles.created_at), unexpected_purchases_source
             order by date_trunc('month', profiles.created_at) desc, unexpected_purchases_source
         )
select date,
       unexpected_purchases_source,
       cnt / cnt_sum
from raw_stats
         join (
                  select date, sum(cnt) cnt_sum
                  from raw_stats
                  group by date
              ) t using (date);


with raw_stats as
         (
             select date_trunc('month', profiles.created_at)::date as date,
                    trading_experience,
                    count(distinct profile_id)                     as cnt
             from app.profile_scoring_settings
                      join app.profiles on profiles.id = profile_id
             where profiles.user_id not in
                   ('3wuhWkjgaPTi42pQoXlcRSiujOH2', 'H5gbDtd8djNfwegUjFT61WeYwxo1', 'hhybvmGt4gTJgHE77oqiRZ87fXC2')
               and not (
                         email ilike '%gainy.app'
                     or email ilike '%test%'
                     or last_name ilike '%test%'
                     or first_name ilike '%test%')
             group by date_trunc('month', profiles.created_at), trading_experience
             order by date_trunc('month', profiles.created_at) desc, trading_experience
         )
select date,
       trading_experience,
       cnt / cnt_sum
from raw_stats
         join (
                  select date, sum(cnt) cnt_sum
                  from raw_stats
                  group by date
              ) t using (date);


with raw_stats as
         (
             select date_trunc('month', profiles.created_at)::date as date,
                    stock_market_risk_level,
                    count(distinct profile_id)                     as cnt
             from app.profile_scoring_settings
                      join app.profiles on profiles.id = profile_id
             where profiles.user_id not in
                   ('3wuhWkjgaPTi42pQoXlcRSiujOH2', 'H5gbDtd8djNfwegUjFT61WeYwxo1', 'hhybvmGt4gTJgHE77oqiRZ87fXC2')
               and not (
                         email ilike '%gainy.app'
                     or email ilike '%test%'
                     or last_name ilike '%test%'
                     or first_name ilike '%test%')
             group by date_trunc('month', profiles.created_at), stock_market_risk_level
             order by date_trunc('month', profiles.created_at) desc, stock_market_risk_level
         )
select date,
       stock_market_risk_level,
       cnt / cnt_sum
from raw_stats
         join (
                  select date, sum(cnt) cnt_sum
                  from raw_stats
                  group by date
              ) t using (date);


select profile_favorite_collections.*,
       collections.name,
       favorite_collection_cnt::double precision / profiles_with_fav_col_cnt as favorite_collection_percent
from app.profile_favorite_collections
         join app.profiles on profiles.id = profile_id
         join collections on collections.id = collection_id
         join (
                  select collection_id,
                         count(distinct profile_id) as favorite_collection_cnt
                  from app.profile_favorite_collections
                           join app.profiles on profiles.id = profile_id
                  where not (email ilike '%gainy.app'
                      or email ilike '%test%'
                      or last_name ilike '%test%'
                      or first_name ilike '%test%')
                  group by collection_id
              ) t using (collection_id)
         join (
                  select count(distinct profile_id) as profiles_with_fav_col_cnt
                  from app.profile_favorite_collections
                           join app.profiles on profiles.id = profile_id
                  where not (email ilike '%gainy.app'
                      or email ilike '%test%'
                      or last_name ilike '%test%'
                      or first_name ilike '%test%')
              ) t2 on true
where profiles.user_id in
      ('3wuhWkjgaPTi42pQoXlcRSiujOH2', 'H5gbDtd8djNfwegUjFT61WeYwxo1', 'hhybvmGt4gTJgHE77oqiRZ87fXC2');

select collection_id,
       collections.name,
       favorite_collection_cnt::double precision / profiles_with_fav_col_cnt as favorite_collection_percent

from (
         select collection_id,
                count(distinct profile_id) as favorite_collection_cnt
         from app.profile_favorite_collections
                  join app.profiles on profiles.id = profile_id
         where not (email ilike '%gainy.app'
             or email ilike '%test%'
             or last_name ilike '%test%'
             or first_name ilike '%test%')
         group by collection_id
     ) t
         join (
                  select count(distinct profile_id) as profiles_with_fav_col_cnt
                  from app.profile_favorite_collections
                           join app.profiles on profiles.id = profile_id
                  where not (email ilike '%gainy.app'
                      or email ilike '%test%'
                      or last_name ilike '%test%'
                      or first_name ilike '%test%')
              ) t2 on true
         join collections on collections.id = collection_id
order by favorite_collection_cnt desc;
