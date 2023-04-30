-- Создаем таблицу с исходными данными и заполняем ее
CREATE TABLE taxination
(
    id      serial PRIMARY KEY NOT NULL,
    point_1 varchar            NOT NULL,
    point_2 varchar            NOT NULL,
    cost    integer            NOT NULL,
    CONSTRAINT check_name CHECK (cost > 0),
    CONSTRAINT check_node_1 CHECK (point_1 = ANY (ARRAY ['A', 'B', 'C', 'D'])),
    CONSTRAINT check_node_2 CHECK (point_2 = ANY (ARRAY ['A', 'B', 'C', 'D']))
);
COMMIT;

INSERT INTO taxination (point_1, point_2, cost)
VALUES ('A', 'B', 10),
       ('B', 'A', 10),
       ('A', 'C', 15),
       ('C', 'A', 15),
       ('A', 'D', 20),
       ('D', 'A', 20),
       ('B', 'C', 35),
       ('C', 'B', 35),
       ('B', 'D', 25),
       ('D', 'B', 25),
       ('C', 'D', 30),
       ('D', 'C', 30);
COMMIT;

-- Создаем VIEW
CREATE MATERIALIZED VIEW mv_all_tours AS
-- Рекурсивный запрос
WITH RECURSIVE all_tour AS (
-- Стартовая часть
(SELECT point_1 as tour,
        point_1,
        point_2,
        cost as total_cost
FROM taxination
WHERE point_1 = 'A')
UNION
-- Рекурсивная часть
(SELECT old.tour || ',' || new.point_1 as tour,
        new.point_1,
        new.point_2,
        old.total_cost + new.cost as total_cost
FROM taxination as new
JOIN all_tour as old on new.point_1 = old.point_2
WHERE tour not like ('%' || new.point_1 || '%')))

SELECT at.total_cost,
       concat('{', at.tour, ',', at.point_2, '}') as tour
FROM all_tour at;