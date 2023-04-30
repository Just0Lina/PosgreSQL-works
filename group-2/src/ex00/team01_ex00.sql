WITH 
-- Убираем null в атрибутах таблицы user
     us AS (SELECT id, coalesce(name, 'not defined') as name, coalesce(lastname, 'not defined') as lastname
            FROM "user"), 
-- Получаем суммарный баланс
     bal AS (SELECT user_id, type, currency_id, sum(money) as volume
             FROM balance
             GROUP BY user_id, type, currency_id
             ORDER BY user_id, type),
--   Получаем последнюю дату обновления курса
     cur_tmp AS (SELECT id,
                        name,
                        rate_to_usd,
                        updated,
                        max(updated) OVER (PARTITION BY name) as max_updated
                 FROM currency),
-- Оставляем нужные строки обновления курса
     cur AS (SELECT id, name, rate_to_usd, updated
             FROM cur_tmp
             WHERE updated = max_updated)

-- Вывод итоговой выборки
SELECT us.name                                   as name,
       us.lastname                               as lastname,
       bal.type                                  as type,
       bal.volume                                as volume,
       coalesce(cur.name, 'not defined')         as currency_name,
       coalesce(cur.rate_to_usd, 1)              as last_rate_to_usd,
       bal.volume * coalesce(cur.rate_to_usd, 1) as total_volume_in_usd
FROM us
         JOIN bal ON us.id = bal.user_id
         LEFT JOIN cur ON cur.id = bal.currency_id
ORDER BY us.name desc, us.lastname asc, bal.type asc;
