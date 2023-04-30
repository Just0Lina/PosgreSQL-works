insert into currency
values (100, 'EUR', 0.85, '2022-01-01 13:29');
insert into currency
values (100, 'EUR', 0.79, '2022-01-08 13:29');

-- Получаем ближайший курс
CREATE OR REPLACE FUNCTION func_get_curses(IN date_balance timestamp with time zone,
                                           IN cur_id bigint) RETURNS numeric AS
$code$
DECLARE
    res numeric;
BEGIN
    res := (SELECT rate_to_usd
            FROM currency
            WHERE id = cur_id
              AND updated <= date_balance
            ORDER BY updated desc
            LIMIT 1);

    IF res ISNULL THEN
        res := (SELECT rate_to_usd
                FROM currency
                WHERE id = cur_id
                  AND updated >= date_balance
                ORDER BY updated
                LIMIT 1);
    END IF;

    RETURN res;
END;
$code$ LANGUAGE plpgsql;

-- Получаем имя валюты
CREATE OR REPLACE FUNCTION func_get_currency_name(IN cur_id bigint) RETURNS varchar AS
$code$
SELECT DISTINCT name
FROM currency
WHERE id = cur_id;
$code$ LANGUAGE SQL;

-- Вывод конечной выборки
SELECT coalesce(u.name, 'not defined')                                  as name,
       coalesce(u.lastname, 'not defined')                              as lastname,
       coalesce(func_get_currency_name(b.currency_id), 'not defined')   as currency_name,
       b.money * func_get_curses(b.updated, b.currency_id)              as currency_in_usd
FROM balance b
         FULL JOIN "user" u on u.id = b.user_id
WHERE b.money * func_get_curses(b.updated, b.currency_id) IS NOT NULL
ORDER BY name desc, lastname, currency_name;
