-- EX00
SELECT *
FROM mv_all_tours
WHERE length(tour) = 11
  AND tour LIKE '{A%A}'
  AND total_cost = (SELECT min(total_cost) FROM mv_all_tours WHERE length(tour) = 11)
ORDER BY 1, 2;

