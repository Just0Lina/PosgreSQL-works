# _Traveling Salesman Problem_

## Exercise 00 - Classical TSP


![T00_02](misc/images/T00_02.png)

There are 4 cities (a, b, c and d) and arcs between them with cost (or taxination). Actually the cost (a,b) = (b,a).

Created a table with name nodes by using structure {point1, point2, cost} and filled data based on a picture (there are direct and reverse paths between 2 nodes).
There is one SQL statement that returns all tours (aka paths) with minimal traveling cost if we start from city "a".
And find the cheapest way of visiting all the cities and returning to your starting point. For example, the tour looks like that a -> b -> c -> d -> a.

The sample of output data you can find below. Data sorted by total_cost and then by tour.

| total_cost | tour |
| ------ | ------ |
| 80 | {a,b,d,c,a} |
| ... | ... |

## Exercise 01 - Opposite TSP


Added a possibility to see additional rows with the most expensive cost to the SQL from previous exercise. Data sorted by total_cost and then by tour.

| total_cost | tour |
| ------ | ------ |
| 80 | {a,b,d,c,a} |
| ... | ... |
| 95 | {a,d,c,b,a} |


