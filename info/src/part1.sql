-- all tables
DROP TABLE IF EXISTS Peers CASCADE;
CREATE TABLE Peers
(
    nickname varchar(255) PRIMARY KEY,
    birthday date NOT NULL
);

DROP TABLE IF EXISTS Tasks CASCADE;
CREATE TABLE Tasks
(
    title      varchar(255) PRIMARY KEY,
    parentTask varchar(255) REFERENCES Tasks (title),
    MaxXp      int NOT NULL CHECK (MaxXp > 0)
);

DROP TABLE IF EXISTS Checks CASCADE;
CREATE TABLE Checks
(
    id        serial PRIMARY KEY,
    peer      varchar(255) NOT NULL REFERENCES Peers (nickname),
    titleTask varchar(255) NOT NULL REFERENCES Tasks (title),
    DateCheck date         NOT NULL
);

DROP TABLE IF EXISTS Xp CASCADE;
CREATE TABLE Xp
(
    id       serial PRIMARY KEY,
    idCheck  int NOT NULL REFERENCES Checks (id),
    XpAmount int NOT NULL
);

DROP TYPE IF EXISTS status CASCADE;
CREATE TYPE status AS enum ('Start', 'Success', 'Failure');

DROP TABLE IF EXISTS P2p CASCADE;
CREATE TABLE P2p
(
    id           serial PRIMARY KEY,
    idCheck      int          NOT NULL REFERENCES Checks (id),
    checkingPeer varchar(255) NOT NULL REFERENCES peers (nickname),
    state        status       NOT NULL,
    timeP2p      time         NOT NULL
);

DROP TABLE IF EXISTS Verter CASCADE;
CREATE TABLE Verter
(
    id        serial PRIMARY KEY,
    idCheck   int    NOT NULL REFERENCES Checks (id),
    state     status NOT NULL,
    eventTime time   NOT NULL
);

DROP TABLE IF EXISTS TransferredPoints CASCADE;
CREATE TABLE TransferredPoints
(
    id           serial PRIMARY KEY,
    checkingPeer varchar(255) NOT NULL REFERENCES peers (nickname),
    checkedPeer  varchar(255) NOT NULL REFERENCES peers (nickname),
    pointsAmount int          NOT NULL
);

DROP TABLE IF EXISTS Friends CASCADE;
CREATE TABLE Friends
(
    id    serial PRIMARY KEY,
    peer1 varchar(255) NOT NULL REFERENCES peers (nickname),
    peer2 varchar(255) NOT NULL REFERENCES peers (nickname)
);

DROP TABLE IF EXISTS Recommendations CASCADE;
CREATE TABLE Recommendations
(
    id                 serial PRIMARY KEY,
    peer               varchar(255) NOT NULL REFERENCES peers (nickname),
    peerRecommendation varchar(255) NOT NULL REFERENCES peers (nickname)
);

DROP TABLE IF EXISTS TimeTracking CASCADE;
CREATE TABLE TimeTracking
(
    id         serial PRIMARY KEY,
    peer       varchar(255) NOT NULL REFERENCES peers (nickname),
    eventDate  date         NOT NULL,
    eventTime  time         NOT NULL,
    eventState int          NOT NULL
);

-- export
DROP PROCEDURE IF EXISTS exportTable;
CREATE OR REPLACE PROCEDURE exportTable(
    table_name varchar(255),
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE FORMAT('COPY %I TO %L DELIMITER %L CSV HEADER', table_name, name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTablePeers;
CREATE OR REPLACE PROCEDURE exportTablePeers(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('peers', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableTasks;
CREATE OR REPLACE PROCEDURE exportTableTasks(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('tasks', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableP2p;
CREATE OR REPLACE PROCEDURE exportTableP2p(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('p2p', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableVerter;
CREATE OR REPLACE PROCEDURE exportTableVerter(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('verter', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableChecks;
CREATE OR REPLACE PROCEDURE exportTableChecks(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('checks', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableTransferredpoints;
CREATE OR REPLACE PROCEDURE exportTableTransferredpoints(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('transferredpoints', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableFriends;
CREATE OR REPLACE PROCEDURE exportTableFriends(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('friends', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableRecommendations;
CREATE OR REPLACE PROCEDURE exportTableRecommendations(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('recommendations', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableXp;
CREATE OR REPLACE PROCEDURE exportTableXp(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('xp', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS exportTableTimetracking;
CREATE OR REPLACE PROCEDURE exportTableTimetracking(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL exportTable('timetracking', name_file, deli);
END;
$$;

-- import
DROP PROCEDURE IF EXISTS importTable;
CREATE OR REPLACE PROCEDURE importTable(
    table_name varchar(255),
    path_to_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    EXECUTE FORMAT('COPY %I FROM %L DELIMITER %L CSV HEADER', table_name, path_to_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTablePeers;
CREATE OR REPLACE PROCEDURE importTablePeers(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('peers', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableTasks;
CREATE OR REPLACE PROCEDURE importTableTasks(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('tasks', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableP2p;
CREATE OR REPLACE PROCEDURE importTableP2p(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('p2p', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableVerter;
CREATE OR REPLACE PROCEDURE importTableVerter(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('verter', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableChecks;
CREATE OR REPLACE PROCEDURE importTableChecks(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('checks', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableTransferredpoints;
CREATE OR REPLACE PROCEDURE importTableTransferredpoints(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('transferredpoints', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableFriends;
CREATE OR REPLACE PROCEDURE importTableFriends(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('friends', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableRecommendations;
CREATE OR REPLACE PROCEDURE importTableRecommendations(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('recommendations', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableXp;
CREATE OR REPLACE PROCEDURE importTableXp(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('xp', name_file, deli);
END;
$$;

DROP PROCEDURE IF EXISTS importTableTimetracking;
CREATE OR REPLACE PROCEDURE importTableTimetracking(
    name_file varchar(255),
    deli nchar(1)
)
    LANGUAGE plpgsql AS
$$
BEGIN
    CALL importTable('timetracking', name_file, deli);
END;
$$;

TRUNCATE peers CASCADE;
TRUNCATE tasks CASCADE;

-- CALL importTablePeers('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/peers.csv', ',');
-- CALL importTableTasks('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/tasks.csv', ',');
-- CALL importTableChecks('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/checks.csv', ',');
-- CALL importTableP2p('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/P2p.csv', ',');
-- CALL importTableVerter('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/verter.csv', ',');
--
-- CALL importTableTransferredpoints('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/transferredpoints.csv', ',');
-- CALL importTableFriends('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/friends.csv', ',');
-- CALL importTableRecommendations(
--         '/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/recommendations.csv', ',');
-- CALL importTableXp('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/xp.csv', ',');
-- CALL importTableTimetracking('/Users/bperegri/Desktop/SQL2_Info21_v1.0-2/src/csv_files/timetracking.csv', ',');