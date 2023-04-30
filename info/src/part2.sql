DROP PROCEDURE IF EXISTS addP2p CASCADE;
CREATE OR REPLACE PROCEDURE addP2p(
    verifiable_nickname varchar(255),
    cheking_nickname varchar(255),
    task_name varchar(255),
    state status,
    event_time time
)
    LANGUAGE plpgsql AS
$$
BEGIN
    IF (state = 'Start') THEN

        IF (SELECT COUNT(*) FROM p2p WHERE checkingpeer = cheking_nickname) % 2 != 0 THEN
            RAISE EXCEPTION 'Error. This peer already start p2p review';
        END IF;

        INSERT INTO checks
        VALUES (COALESCE((SELECT MAX(id) + 1 FROM checks), 1), verifiable_nickname, task_name,
                (SELECT CURRENT_DATE));

        INSERT
        INTO p2p
        VALUES (COALESCE((SELECT MAX(id) + 1 FROM p2p), 1), (SELECT MAX(id) FROM checks), cheking_nickname,
                'Start', event_time);
    ELSE
        IF (SELECT COUNT(*) FROM p2p WHERE checkingpeer = cheking_nickname) % 2 = 0 THEN
            RAISE EXCEPTION 'Error. This peer not yet start p2p review';
        END IF;

        WITH t1 AS (SELECT idcheck, COUNT(*) AS count
                    FROM p2p
                    WHERE checkingpeer = cheking_nickname
                    GROUP BY idcheck)
        INSERT
        INTO p2p
        VALUES ((SELECT MAX(id) + 1 FROM p2p), (SELECT t1.idcheck FROM t1 WHERE t1.count = 1),
                cheking_nickname, state,
                event_time);

    END IF;
END ;
$$;

DROP FUNCTION IF EXISTS fncTrgP2pInsertAudit CASCADE;
CREATE OR REPLACE FUNCTION fncTrgP2pInsertAudit() RETURNS trigger AS
$p2pAuditInsert$
DECLARE
    kCheckedPeer varchar(255);
BEGIN
    kCheckedPeer = (SELECT peer FROM checks WHERE id = new.idcheck);
    IF (new.state = 'Start') THEN

        IF (SELECT COUNT(*)
            FROM transferredpoints AS t
            WHERE t.checkingpeer = new.checkingpeer
              AND t.checkedpeer = kCheckedPeer) = 0 THEN

            INSERT INTO transferredpoints
            VALUES (COALESCE((SELECT MAX(id) + 1 FROM transferredpoints), 1),
                    new.checkingpeer, kCheckedPeer, 1);
        ELSE
            UPDATE transferredpoints
            SET pointsamount = pointsamount + 1
            WHERE checkingpeer = new.checkingpeer
              AND checkedpeer = kCheckedpeer;
        END IF;
    END IF;
    RETURN NEW;
END;
$p2pAuditInsert$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS p2pTrigger ON p2p CASCADE;
CREATE OR REPLACE TRIGGER p2p_trigger
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE FUNCTION fncTrgP2pInsertAudit();

DROP PROCEDURE IF EXISTS addVerter;
CREATE OR REPLACE PROCEDURE addVerter(
    verifiable_nickname varchar(255),
    task_name varchar(255),
    st status,
    event_time time
)
    LANGUAGE plpgsql AS
$$
DECLARE
    idcheckPeer integer;
BEGIN
    idcheckPeer = coalesce((SELECT checks.id
                   FROM checks
                   WHERE peer = verifiable_nickname
                     AND titletask = task_name
                   ORDER BY checks.id DESC
                   LIMIT 1), 0);

    if (idcheckPeer = 0) THEN
        RAISE EXCEPTION 'Error. Peer does not have check or task name is wrong';
    END IF;

    IF ('Success' NOT IN (SELECT state
                          FROM p2p
                          WHERE p2p.idcheck = idcheckPeer)) THEN
        RAISE EXCEPTION 'P2P review dose not success';
    END IF;

    INSERT
    INTO verter
    VALUES (COALESCE((SELECT MAX(id) + 1 FROM verter), 1), idcheckPeer, st, event_time);
END;
$$;

DROP FUNCTION IF EXISTS fncTrgVerterBeforeAudit CASCADE;
CREATE OR REPLACE FUNCTION fncTrgVerterBeforeAudit() RETURNS trigger AS
$VerterAuditInsert$
BEGIN
    IF (new.state = 'Start' AND (SELECT COUNT(*)
                                 FROM verter
                                 WHERE idcheck = new.idcheck
                                   AND state = 'Start') != 0) THEN
        RAISE EXCEPTION 'Error. Review verter with the same idcheck already started';
    END IF;

    IF (new.state = 'Success' OR new.state = 'Failure') THEN
        IF (SELECT COUNT(*) FROM verter WHERE idcheck = new.idcheck AND state = 'Start') = 0 THEN
            RAISE EXCEPTION 'Error. Review verter with the same idcheck not yet start';
        END IF;

        IF (SELECT COUNT(*)
            FROM verter
            WHERE idcheck = new.idcheck
              AND (state = 'Success' OR state = 'Failure')) != 0 THEN
            RAISE EXCEPTION 'Error. Review verter with the same id already finished';
        END IF;
    END IF;

    RETURN NEW;
END;
$VerterAuditInsert$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS verterTrigger ON verter CASCADE;
CREATE OR REPLACE TRIGGER verterTrigger
    BEFORE INSERT
    ON verter
    FOR EACH ROW
EXECUTE FUNCTION fncTrgVerterBeforeAudit();

DROP FUNCTION IF EXISTS fncTrgXpBeforeInsert CASCADE;
CREATE OR REPLACE FUNCTION fncTrgXpBeforeInsert() RETURNS trigger AS
$xpAuditBeforeInsert$
BEGIN

    IF (new.XpAmount > (SELECT MaxXp
                        FROM tasks
                                 JOIN checks c ON tasks.title = c.titletask
                        WHERE new.idcheck = c.id)) THEN
        RAISE EXCEPTION 'Error. XpAmount more then max for this project';
    END IF;

    IF (NOT EXISTS(SELECT state
                   FROM p2p
                   WHERE new.idcheck = p2p.idcheck
                     AND p2p.state = 'Success')) THEN
        RAISE EXCEPTION 'Error. P2P review is not success';
    END IF;


    IF (NOT EXISTS(SELECT state
                   FROM verter
                   WHERE NEW.idcheck = verter.idcheck
                     AND verter.state = 'Success'))
    THEN
        RAISE EXCEPTION 'Error. Verter review is not success';
    END IF;

    RETURN NEW;
END;
$xpAuditBeforeInsert$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS xpTrigger ON xp CASCADE;
CREATE OR REPLACE TRIGGER xpTrigger
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE FUNCTION fncTrgXpBeforeInsert();
