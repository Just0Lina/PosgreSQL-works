--ex1
DROP FUNCTION IF EXISTS getTransferredPoints;
CREATE OR REPLACE FUNCTION getTransferredPoints()
    RETURNS TABLE
            (
                Peer1        varchar,
                Peer2        varchar,
                PointsAmount int
            )
AS
$$
WITH nicks(checkingpeer, checkedpeer) AS (SELECT nickname1.nickname Peer1, nickname2.nickname Peer2
                                          FROM peers nickname1
                                                   JOIN peers nickname2 ON nickname2.nickname > nickname1.nickname),
     points AS (SELECT COALESCE(tr1.checkingpeer, tr2.checkedpeer)                        Peer1,
                       COALESCE(tr1.checkedpeer, tr2.checkingpeer)                        Peer2,
                       (COALESCE(tr1.pointsamount, 0) - COALESCE(tr2.pointsamount, 0)) AS PointsAmount
                FROM transferredpoints tr1
                         FULL JOIN transferredpoints tr2
                                   ON tr1.checkingpeer = tr2.checkedpeer AND tr2.checkingpeer = tr1.checkedpeer)
SELECT nicks.checkingpeer peer1, nicks.checkedpeer peer2, PointsAmount
FROM nicks
         INNER JOIN points ON Peer1 = checkingpeer AND Peer2 = checkedpeer;
$$ LANGUAGE SQL;

SELECT *
FROM getTransferredPoints();

-- ex2
DROP FUNCTION IF EXISTS getStatisticsSuccessChecks();
CREATE OR REPLACE FUNCTION getStatisticsSuccessChecks()
    RETURNS TABLE
            (
                peer VARCHAR(255),
                task VARCHAR(255),
                xp   INTEGER
            )
AS
$$

SELECT c.peer AS peer, c.titletask AS task, x.XpAmount AS xp
FROM checks AS c
         JOIN p2p p ON c.id = p.idcheck
         JOIN verter v ON c.id = v.idcheck
         JOIN xp x ON c.id = x.idcheck
WHERE p.state = 'Success'
  AND v.state = 'Success';
$$ LANGUAGE SQL;

SELECT *
FROM getStatisticsSuccessChecks();

-- ex3
DROP FUNCTION IF EXISTS getPeersInCampus(day date);
CREATE OR REPLACE FUNCTION getPeersInCampus(day date)
    RETURNS TABLE
            (
                Peer varchar
            )
AS
$$
WITH peers_start(peer, datee, time) AS (SELECT DISTINCT ON (1) peer,
                                                               eventdate,
                                                               eventtime
                                        FROM timetracking t
                                        WHERE eventstate = 1
                                          AND eventdate < day
                                        ORDER BY 1, eventdate, eventtime DESC),
     peers_finish AS (SELECT DISTINCT ON (1) t.peer, eventdate
                      FROM timetracking t
                               JOIN peers_start ON peers_start.peer = t.peer
                      WHERE eventstate = 2
                          AND eventdate > peers_start.datee
                         OR (eventdate = peers_start.datee AND eventtime > peers_start.time)
                      ORDER BY 1, eventdate, eventtime ASC)
SELECT peers_start.peer
FROM peers_start
         INNER JOIN peers_finish USING (peer)
WHERE eventdate > day;
$$ LANGUAGE SQL;

SELECT *
FROM getPeersInCampus('2022-04-22');

-- 4ex
DROP PROCEDURE IF EXISTS checksSuccess();
CREATE OR REPLACE PROCEDURE checksSuccess(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH success AS (SELECT idcheck FROM p2p WHERE state = 'Success'),
             SuccessfulChecks AS (SELECT COUNT(*) success
                                  FROM success
                                  EXCEPT
                                  SELECT v.idcheck
                                  FROM verter v
                                  WHERE state = 'Failure'),
             UnsuccessfulChecks AS (SELECT COUNT(id) - (SELECT * FROM SuccessfulChecks) FROM checks)
        SELECT COALESCE(((SELECT * FROM SuccessfulChecks) * 100 / NULLIF((SELECT COUNT(id) FROM checks), 0)),
                        0) AS successfulchecks,
               COALESCE(((SELECT * FROM UnsuccessfulChecks) * 100 / NULLIF((SELECT COUNT(id) FROM checks), 0)),
                        0) AS unsuccessfulchecks;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL checksSuccess();
FETCH ALL FROM "result";
END;

--ex5
DROP PROCEDURE IF EXISTS pointChange;
CREATE OR REPLACE PROCEDURE pointChange(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH earned(nickname, sum) AS (SELECT nickname, SUM(COALESCE(t.pointsamount, 0))
                                       FROM peers
                                                LEFT JOIN transferredpoints t ON peers.nickname = t.checkingpeer
                                       GROUP BY nickname),
             spend(nickname, sum) AS (SELECT nickname, SUM(COALESCE(t.pointsamount, 0))
                                      FROM peers
                                               LEFT JOIN transferredpoints t ON peers.nickname = t.checkedpeer
                                      GROUP BY nickname)
        SELECT earned.nickname, earned.sum - spend.sum points
        FROM earned
                 JOIN spend ON earned.nickname = spend.nickname
        ORDER BY points DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL pointChange();
FETCH ALL FROM "result";
END;

-- ex6
DROP PROCEDURE IF EXISTS PeerPointsChange(INOUT ref refcursor);
CREATE OR REPLACE PROCEDURE PeerPointsChange(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH pair_points AS
                     (SELECT * FROM getTransferredPoints()),
             peer1(nickname, sum) AS (SELECT nickname, SUM(COALESCE(t.pointsamount, 0))
                                      FROM peers
                                               LEFT JOIN pair_points t ON peers.nickname = t.Peer1
                                      GROUP BY nickname),
             peer2(nickname, sum) AS (SELECT nickname, SUM(COALESCE(t.pointsamount, 0))
                                      FROM peers
                                               LEFT JOIN pair_points t ON peers.nickname = t.Peer2
                                      GROUP BY nickname)
        SELECT p1.nickname, p1.sum - p2.sum points
        FROM peer1 p1
                 JOIN peer2 p2 ON p1.nickname = p2.nickname
        ORDER BY points DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL PeerPointsChange();
FETCH ALL FROM "result";
END;

-- ex7
DROP PROCEDURE IF EXISTS mostPopularTaskOfTheDay;
CREATE OR REPLACE PROCEDURE mostPopularTaskOfTheDay(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH count_table AS (SELECT COUNT(titletask) count, titletask, datecheck
                             FROM checks
                             GROUP BY datecheck, titletask),
             ans AS (SELECT MAX(count), datecheck FROM count_table GROUP BY datecheck)
        SELECT c.titletask, c.datecheck
        FROM count_table c
                 JOIN ans a ON a.datecheck = c.datecheck AND a.max = c.count
        ORDER BY datecheck;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL mostPopularTaskOfTheDay();
FETCH ALL FROM "result";
END;

-- ex8
DROP PROCEDURE IF EXISTS timeLastP2PCheck();
CREATE OR REPLACE PROCEDURE timeLastP2PCheck(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH lastCheck(id, idcheck, peer, state, timestart) AS (SELECT *
                                                                FROM p2p
                                                                WHERE state = 'Start'
                                                                ORDER BY id DESC
                                                                LIMIT 1)

        SELECT (p2p.timep2p - lastCheck.timestart)::time AS timeLastCheck
        FROM p2p
                 JOIN lastCheck ON p2p.idcheck = lastCheck.idcheck
        WHERE p2p.state != 'Start';
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL timeLastP2PCheck();
FETCH ALL FROM "result";
END;

-- ex9
DROP PROCEDURE IF EXISTS peersFinishedBlock;
CREATE OR REPLACE PROCEDURE peersFinishedBlock(taskname varchar, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR SELECT peer AS peer, datecheck AS day
                 FROM checks
                          JOIN verter v ON checks.id = v.idcheck
                 WHERE state = 'Success'
--                  AND checks.titletask = titletask
                   AND checks.titletask = (SELECT MAX(title)
                                           FROM (SELECT UNNEST(REGEXP_MATCHES(checks.titletask, CONCAT('(', taskname, '\d.*)'))) AS title
                                                 FROM checks) data)
                 ORDER BY datecheck;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peersFinishedBlock('CPP');
FETCH ALL FROM "result";
END;

-- ex10
DROP PROCEDURE IF EXISTS peerRecommendation();
CREATE OR REPLACE PROCEDURE peerRecommendation(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH t(nick, recom) AS (SELECT p.nickname, r.peerrecommendation AS rec
                                FROM peers AS p
                                         JOIN friends f ON (p.nickname = f.peer1)
                                         JOIN recommendations r
                                              ON (r.peer = f.peer2 AND p.nickname != r.peerrecommendation)
                                UNION ALL
                                SELECT p.nickname, r.peerrecommendation AS rec
                                FROM peers AS p
                                         JOIN friends f ON (p.nickname = f.peer2)
                                         JOIN recommendations r
                                              ON (r.peer = f.peer1 AND p.nickname != r.peerrecommendation))

        SELECT DISTINCT ON (tt.nick) tt.nick, tt.rec
        FROM (SELECT t.nick AS nick, t.recom AS rec, COUNT(*) AS count
              FROM t
              GROUP BY t.nick, t.recom) AS tt
        ORDER BY tt.nick, tt.count DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peerRecommendation();
FETCH ALL FROM "result";
END;

--ex11
DROP PROCEDURE IF EXISTS peerBlockstats;
CREATE OR REPLACE PROCEDURE peerBlockstats(block1 varchar, block2 varchar, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH firB(peer) AS (SELECT DISTINCT peer
                            FROM (SELECT peer,
                                         UNNEST(REGEXP_MATCHES(checks.titletask, CONCAT('(', 'CPP', '\d.*)'))) block
                                  FROM checks) block),
             secB(peer) AS (SELECT DISTINCT peer
                            FROM (SELECT peer, UNNEST(REGEXP_MATCHES(checks.titletask, CONCAT('(', 'C', '\d.*)'))) block
                                  FROM checks) block),
             botrh(peer) AS (SELECT peer
                             FROM firB
                                      NATURAL JOIN secB),
             none(peer) AS (SELECT (SELECT COUNT(*) FROM peers) - (SELECT COUNT(*) FROM firB) -
                                   (SELECT COUNT(*) FROM secB) + (SELECT COUNT(*) FROM botrh) res)
        SELECT (SELECT COUNT(*) FROM firB) * 100 / (SELECT COUNT(*) FROM peers)  StartedBlock1,
               (SELECT COUNT(*) FROM secB) * 100 / (SELECT COUNT(*) FROM peers)  StartedBlock2,
               (SELECT COUNT(*) FROM botrh) * 100 / (SELECT COUNT(*) FROM peers) StartedBothBlocks,
               (SELECT * FROM none) * 100 / (SELECT COUNT(*) FROM peers)         DidntStartAnyBlock;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peerBlockstats('CPP', 'C');
FETCH ALL FROM "result";
END;

-- ex12
DROP PROCEDURE IF EXISTS peersHaveTheMostFriends(n integer);
CREATE OR REPLACE PROCEDURE peersHaveTheMostFriends(n integer, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH t1(peer, friend) AS (SELECT nickname, peer2
                                  FROM peers AS p
                                           LEFT JOIN friends AS f ON p.nickname = f.peer1
                                  UNION ALL
                                  SELECT nickname, peer1
                                  FROM peers AS p
                                           LEFT JOIN friends AS f ON p.nickname = f.peer2),

             t2(peer, friend, count) AS (SELECT t1.peer, t1.friend, CASE WHEN t1.friend IS NOT NULL THEN 1 ELSE 0 END
                                         FROM t1),

             t3 (peer, FriendsCount) AS
                 (SELECT t2.peer AS p, SUM(t2.count) AS c FROM t2 GROUP BY p ORDER BY c DESC)

        SELECT *
        FROM t3
        LIMIT n;

END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peersHaveTheMostFriends(12);
FETCH ALL FROM "result";
END;

-- ex13
DROP PROCEDURE IF EXISTS birthdaySuccess;
CREATE OR REPLACE PROCEDURE birthdaySuccess(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH info AS (SELECT *
                      FROM peers
                               JOIN checks ON
                                  DATE_PART('day', birthday) =
                                  DATE_PART('day', datecheck)
                              AND DATE_PART('month', birthday) =
                                  DATE_PART('month', datecheck)
                              AND checks.peer = nickname
                               JOIN p2p ON idcheck = checks.id)
        SELECT COALESCE(((SELECT COUNT(state)
                          FROM info
                          WHERE state = 'Success') * 100 / NULLIF((SELECT COUNT(state)
                                                                   FROM info
                                                                   WHERE state = 'Start'), 0)), 0) AS successfulchecks,
               COALESCE(((SELECT COUNT(state)
                          FROM info
                          WHERE state = 'Failure') * 100 / NULLIF((SELECT COUNT(state)
                                                                   FROM info
                                                                   WHERE state = 'Start'), 0)),
                        0)                                                                         AS unsuccessfulchecks;

END
$$ LANGUAGE plpgsql;

BEGIN;
CALL birthdaySuccess();
FETCH ALL FROM "result";
END;

-- ex14
DROP PROCEDURE IF EXISTS getSumXpEarnedPeers();
CREATE OR REPLACE PROCEDURE getSumXpEarnedPeers(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH allSuccessChecked (nick, id, task, xp)
                 AS (SELECT peer, idcheck, titletask, xpamount
                     FROM xp
                              JOIN checks c ON xp.idcheck = c.id
                     ORDER BY peer),

             allUnicSuccessChecked(nick, task, xp) AS (SELECT nick, task, MAX(xp)
                                                       FROM allSuccessChecked
                                                       GROUP BY nick, task)

        SELECT nickname AS peer, SUM(COALESCE(a.xp, 0)) AS task
        FROM peers
                 LEFT JOIN allUnicSuccessChecked AS a ON a.nick = nickname
        GROUP BY nickname;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL getSumXpEarnedPeers();
FETCH ALL FROM "result";
END;


--ex 15
DROP PROCEDURE IF EXISTS did2of3tasks;
CREATE OR REPLACE PROCEDURE did2of3tasks(task1 varchar, task2 varchar, task3 varchar, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH checks1(id, peer) AS (SELECT id, peer FROM checks WHERE titletask = task1),
             succesP2P1(id, peer) AS (SELECT c.id, peer
                                      FROM p2p
                                               JOIN checks1 c ON c.id = p2p.idcheck AND state = 'Success'),
             successVer1 AS (SELECT idcheck, peer
                             FROM verter
                                      JOIN succesP2P1 s ON s.id = verter.idcheck AND state = 'Success'
                             UNION
                             SELECT succesP2P1.id, peer
                             FROM succesP2P1
                             WHERE succesP2P1.id NOT IN (SELECT idcheck FROM verter)),
             checks2(id, peer) AS (SELECT id, peer FROM checks WHERE titletask = task2),
             succesP2P2(id, peer) AS (SELECT c.id, peer
                                      FROM p2p
                                               JOIN checks2 c ON c.id = p2p.idcheck AND state = 'Success'),
             successVer2(id, peer) AS (SELECT idcheck, peer
                                       FROM verter
                                                JOIN succesP2P2 s ON s.id = verter.idcheck AND state = 'Success'
                                       UNION
                                       SELECT id, peer
                                       FROM succesP2P2
                                       WHERE succesP2P2.id NOT IN (SELECT idcheck FROM verter)),
             checks3(id, peer) AS (SELECT id, peer FROM checks WHERE titletask = task3),
             succesP2P3(id, peer) AS (SELECT c.id, peer
                                      FROM p2p
                                               JOIN checks3 c ON c.id = p2p.idcheck AND state = 'Success'),
             successVer3(id, peer) AS (SELECT idcheck, peer
                                       FROM verter
                                                JOIN succesP2P3 s ON s.id = verter.idcheck AND state = 'Success'
                                       UNION
                                       SELECT id, peer
                                       FROM succesP2P3
                                       WHERE succesP2P3.id NOT IN (SELECT idcheck FROM verter))

        SELECT peer
        FROM successVer1
        INTERSECT
        SELECT peer
        FROM successVer2
        EXCEPT
        SELECT peer
        FROM successVer3;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL did2of3tasks('CPP1_s21_matrix+', 'C3_s21_string+', 'C2_SimpleBashUtils');
FETCH ALL FROM "result";
END;

--ex16
DROP PROCEDURE IF EXISTS getTreeTaskView;
CREATE OR REPLACE PROCEDURE getTreeTaskView(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        (WITH RECURSIVE all_previous AS
                            (SELECT title AS Task, parenttask, 0 AS PrevCount
                             FROM tasks
                             WHERE parenttask IS NULL
                             UNION ALL
                             SELECT tasks.title AS Task, tasks.parenttask, ap.PrevCount + 1 AS PrevCount
                             FROM all_previous ap
                                      INNER JOIN tasks ON tasks.parenttask = ap.Task)

         SELECT ap.Task, ap.PrevCount
         FROM all_previous ap
         ORDER BY ap.Task);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL getTreeTaskView();
FETCH ALL FROM "result";
END;


--ex17
DROP PROCEDURE IF EXISTS succesfulDay(N numeric);
CREATE OR REPLACE PROCEDURE succesfulDay(N numeric, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH success AS (SELECT idcheck FROM p2p WHERE state = 'Success'),
             SuccessfulChecks AS (SELECT idcheck
                                  FROM success
                                  EXCEPT
                                  SELECT v.idcheck
                                  FROM verter v
                                  WHERE state = 'Failure'),
             epMore80per AS (SELECT idcheck
                             FROM xp
                                      JOIN checks c ON xp.idcheck = c.id
                                      JOIN tasks t ON t.title = c.titletask
                             WHERE xpamount * 100 / maxxp >= 80),
             timeNdates AS (SELECT c.id, datecheck, timep2p
                            FROM checks c
                                     JOIN p2p p ON c.id = p.idcheck
                            WHERE state = 'Start'
                            ORDER BY datecheck, timep2p),
             preSelected AS (SELECT datecheck, timep2p, COALESCE(idcheck, 0) id
                             FROM timeNdates
                                      LEFT JOIN (SELECT * FROM SuccessfulChecks s INTERSECT SELECT * FROM epMore80per) needed
                                                ON id = idcheck),
             tableWithNofSerials AS (SELECT datecheck,
                                            timep2p,
                                            id,
                                            COUNT(CASE
                                                      WHEN id != 0
                                                          THEN 1 END)
                                            OVER (PARTITION BY datecheck ORDER BY timep2p) number
                                     FROM preSelected)
        SELECT DISTINCT datecheck
        FROM tableWithNofSerials
        WHERE number >= N;

END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL succesfulDay(1);
FETCH ALL FROM "result";
END;

-- ex18
DROP PROCEDURE IF EXISTS peerMostTaskDone();
CREATE OR REPLACE PROCEDURE peerMostTaskDone(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        SELECT t.peer, COUNT(*) AS tasksDone
        FROM (SELECT checks.peer, checks.titletask
              FROM checks
                       JOIN p2p p ON checks.id = p.idcheck
                       JOIN verter v ON checks.id = v.idcheck
              WHERE p.state = 'Success'
                AND v.state = 'Success'
              GROUP BY peer, titletask) AS t
        GROUP BY t.peer
        ORDER BY tasksDone DESC
        LIMIT 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peerMostTaskDone();
FETCH ALL FROM "result";
END;

-- ex19
DROP PROCEDURE IF EXISTS biggestXP;
CREATE OR REPLACE PROCEDURE biggestXP(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH allSuccessChecked (nick, id, task, xp)
                 AS (SELECT peer, idcheck, titletask, xpamount
                     FROM xp
                              JOIN checks c ON xp.idcheck = c.id
                     ORDER BY peer),

             allUnicSuccessChecked(nick, task, xp) AS (SELECT nick, task, MAX(xp)
                                                       FROM allSuccessChecked
                                                       GROUP BY nick, task)
        SELECT nick, xp
        FROM allUnicSuccessChecked
        WHERE xp = (SELECT MAX(xp) FROM allUnicSuccessChecked);
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL biggestXP();
FETCH ALL FROM "result";
END;

-- ex20 
DROP PROCEDURE IF EXISTS getPeerTheLongestTimeInCampus();
CREATE OR REPLACE PROCEDURE getPeerTheLongestTimeInCampus(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH current_day_time AS
            (SELECT id, peer, eventtime, eventstate
             FROM timetracking
             WHERE eventdate = date(CURRENT_DATE)
             ORDER BY peer, id)

           , total_time AS
            (SELECT peer,
                    SUM((COALESCE((SELECT eventtime
                                   FROM current_day_time t
                                   WHERE eventstate = 2
                                     AND t.id > m.id
                                     AND t.peer = m.peer
                                   LIMIT 1),
                                  CLOCK_TIMESTAMP()::time) - eventtime::time)::interval)::time AS fulltime
             FROM current_day_time m
             WHERE eventstate = 1
             GROUP BY 1
             ORDER BY 2 DESC)

        SELECT peer
        FROM total_time
        LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL getPeerTheLongestTimeInCampus();
FETCH ALL FROM "result";
END;

-- ex21
DROP PROCEDURE IF EXISTS biggestXP;
CREATE OR REPLACE PROCEDURE biggestXP(t time, N numeric, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH times AS (SELECT peer, COUNT(*) times
                       FROM timetracking
                       WHERE eventstate = 1
                         AND eventtime < t
                       GROUP BY peer)
        SELECT peer
        FROM times
        WHERE times >= N;

END
$$ LANGUAGE plpgsql;

BEGIN;
CALL biggestXP('23:50:00', 1);
FETCH ALL FROM "result";
END;

-- ex22
DROP PROCEDURE IF EXISTS peersOutOfCompusNearestDays(days integer, outs integer);
CREATE OR REPLACE PROCEDURE peersOutOfCompusNearestDays(days integer, outs integer, INOUT ref refcursor = 'result')
AS
$$
DECLARE
    currDate date DEFAULT (SELECT CURRENT_DATE);
BEGIN
    OPEN ref FOR
        WITH t1 AS (SELECT *
                    FROM timetracking
                    WHERE eventstate = 2
                      AND eventdate > (currDate - days)
                      AND eventdate <= currDate),

             t2(peer, countOut) AS (SELECT t1.peer, COUNT(t1.eventstate) AS countOut
                                    FROM t1
                                    GROUP BY t1.peer)

        SELECT peer
        FROM t2
        WHERE t2.countOut > outs;


END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peersOutOfCompusNearestDays(1, 1);
FETCH ALL FROM "result";
END;

--ex23
DROP PROCEDURE IF EXISTS peerLastCome(INOUT ref refcursor);
CREATE OR REPLACE PROCEDURE peerLastCome(INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer FROM timetracking ORDER BY eventtime DESC LIMIT 1;
END
$$ LANGUAGE plpgsql;

BEGIN;
CALL peerLastCome();
FETCH ALL FROM "result";
END;

--ex24
DROP PROCEDURE IF EXISTS peersWhoLeftTheCampusYesterdayFor;
CREATE OR REPLACE PROCEDURE peersWhoLeftTheCampusYesterdayFor(minutes integer, INOUT ref refcursor = 'result')
AS
$$
BEGIN
    OPEN ref FOR
        WITH current_day_time AS
                 (SELECT id, peer, eventtime, eventstate
                  FROM timetracking
                  WHERE eventdate = date(CURRENT_DATE - '1 DAY'::INTERVAL)
                  ORDER BY peer, id),

             exit_time AS
                 (SELECT ct.eventtime AS "IN"
                       , (SELECT eventtime
                          FROM current_day_time t
                          WHERE t.eventstate = 1
                            AND ct.id < t.id
                            AND ct.peer = t.peer
                          LIMIT 1) AS "OUT"
                       , ct.eventstate
                       , ct.peer
                  FROM current_day_time ct
                  WHERE ct.eventstate = 2
                  ORDER BY peer)
                ,
             total_out_time AS
                 (SELECT peer, SUM("OUT" - "IN")::TIME AS total_time FROM exit_time GROUP BY peer)

        SELECT peer
        FROM total_out_time
        WHERE total_time > (minutes * INTERVAL '1 minutes');
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peersWhoLeftTheCampusYesterdayFor(15);
FETCH ALL IN "result";
END;

--ex25
CREATE OR REPLACE PROCEDURE prEarlyCamePercent(INOUT curs refcursor = 'result') AS
$$
BEGIN
    OPEN curs FOR WITH months AS (SELECT ROW_NUMBER() OVER () AS number, TO_CHAR(gs, 'Month') AS month
                                  FROM (SELECT generate_series AS gs
                                        FROM GENERATE_SERIES('2023-01-01', '2023-12-31', INTERVAL '1 month')) AS series)
                  SELECT month,
                         COALESCE((SELECT COUNT(*) * 100 / NULLIF((SELECT COUNT(*)
                                                                   FROM timetracking
                                                                            JOIN peers
                                                                                 ON peers.nickname = timetracking.peer
                                                                   WHERE eventstate = 1
                                                                     AND DATE_PART('month', birthday)
                                                                       = DATE_PART('month', eventdate)
                                                                     AND number = DATE_PART('month', eventdate)), 0)
                                   FROM peers
                                            JOIN timetracking
                                                 ON peers.nickname = timetracking.peer
                                   WHERE DATE_PART('month', birthday) =
                                         DATE_PART('month', eventdate)
                                     AND timetracking.eventstate = 1
                                     AND DATE_PART('hour', eventtime) < 12
                                     AND number = DATE_PART('month', eventdate)), 0) AS earlyentries
                  FROM months;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL prEarlyCamePercent();
FETCH ALL IN "result";
END;




