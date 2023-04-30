-- EX01
SELECT *
FROM mv_all_tours
WHERE length(tour) = 11
  AND tour like '{A%A}'
ORDER BY 1, 2;

