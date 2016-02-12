DELIMITER ;;
/*  ps_history 
    Copyright 2015 Justin Swanhart

    ps_history is free software: you can redistribute it and/or modify
    it under the terms of the Lesser GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    ps_history is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with FlexViews in the file COPYING, and the Lesser extension to
    the GPL (the LGPL) in COPYING.LESSER.
    If not, see <http://www.gnu.org/licenses/>.
*/
SET NAMES UTF8;

DROP DATABASE IF EXISTS ps_history;

CREATE DATABASE IF NOT EXISTS ps_history;

USE ps_history;

DROP PROCEDURE IF EXISTS setup;;

CREATE DEFINER=root@localhost PROCEDURE ps_history.setup()
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_done BOOLEAN DEFAULT FALSE;
    DECLARE v_table VARCHAR(64); 
    DECLARE v_sql TEXT;
    DECLARE table_cur CURSOR 
    FOR 
    SELECT CONCAT('CREATE TABLE ps_history.', TABLE_NAME, '( ', GROUP_CONCAT(CONCAT(COLUMN_NAME, ' ', IF(COLUMN_TYPE = 'timestamp','datetime',COLUMN_TYPE), IF(CHARACTER_SET_NAME IS NOT NULL,CONCAT(' CHARACTER SET ', CHARACTER_SET_NAME),''),if(COLLATION_NAME IS NOT NULL,CONCAT(' COLLATE ', COLLATION_NAME),'')) ORDER BY ORDINAL_POSITION SEPARATOR ',\n') , ',server_id int unsigned,\nts datetime(5),KEY(ts) ) CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI PARTITION BY KEY(ts) PARTITIONS 7') as create_tbl,
           TABLE_NAME 
      FROM INFORMATION_SCHEMA.COLUMNS 
     WHERE TABLE_SCHEMA='performance_schema' 
     GROUP BY TABLE_NAME;

    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

    SET group_concat_max_len := @@max_allowed_packet;

    SET v_done = FALSE;
    OPEN table_cur;
    tableLoop: LOOP

      FETCH table_cur
        INTO v_sql,
             v_table;
          
      IF v_done THEN
        CLOSE table_cur;
        LEAVE tableLoop;
      END IF;

      SET @v_sql := CONCAT('DROP TABLE IF EXISTS ps_history.', v_table,'');
      PREPARE drop_stmt FROM @v_sql;
      EXECUTE drop_stmt;
      DEALLOCATE PREPARE drop_stmt;

      SET @v_sql := v_sql;
      -- print out the CREATE statements so the user knows what is happening
      SELECT @v_sql;
      PREPARE create_stmt FROM @v_sql;
      EXECUTE create_stmt;
      DEALLOCATE PREPARE create_stmt;

    END LOOP;

    -- These are ps_history specific tables.  There are triggers defined on psh_settings below.
    CREATE TABLE ps_history.psh_settings(variable varchar(64), key(variable), value varchar(64)) CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI engine = InnoDB;
    INSERT INTO ps_history.psh_settings VALUES ('interval', '30');
    INSERT INTO ps_history.psh_settings VALUES ('retention_period', '1 WEEK');
    CREATE TABLE ps_history.psh_last_refresh(last_refreshed_at DATETIME(6) NOT NULL) engine=InnoDB CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI;

END;;

DROP PROCEDURE IF EXISTS truncate_tables;;

CREATE DEFINER=root@localhost PROCEDURE ps_history.truncate_tables()
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_done BOOLEAN DEFAULT FALSE;
    DECLARE v_sql TEXT;

    DECLARE table_cur CURSOR FOR
    SELECT CONCAT('TRUNCATE TABLE ps_history.', table_name) 
      FROM INFORMATION_SCHEMA.TABLES
     WHERE table_schema = 'ps_history'
       AND table_name NOT LIKE 'psh%';

    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

    SET v_done = FALSE;
    OPEN table_cur;
    tableLoop: LOOP

        FETCH table_cur
         INTO v_sql;

        IF v_done THEN
            CLOSE table_cur;
            LEAVE tableLoop;
        END IF; 

        SET @v_sql := v_sql;
        PREPARE truncate_stmt FROM @v_sql;
        EXECUTE truncate_stmt;
        DEALLOCATE PREPARE truncate_stmt;

    END LOOP;

END;;

DROP PROCEDURE IF EXISTS cleanup_history;;

CREATE DEFINER=root@localhost PROCEDURE ps_history.cleanup_history(v_interval VARCHAR(64))
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_done BOOLEAN DEFAULT FALSE;
    DECLARE v_sql TEXT;

    DECLARE table_cur CURSOR FOR
    SELECT CONCAT('DELETE FROM ps_history.', table_name, ' WHERE ts < NOW() - INTERVAL ', v_interval) 
      FROM INFORMATION_SCHEMA.TABLES
     WHERE table_schema = 'ps_history'
       AND table_name NOT LIKE 'psh%';

    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

    SET v_done = FALSE;
    OPEN table_cur;
    tableLoop: LOOP

        FETCH table_cur
         INTO v_sql;

        IF v_done THEN
            CLOSE table_cur;
            LEAVE tableLoop;
        END IF; 

        SET @v_sql := v_sql;
        PREPARE truncate_stmt FROM @v_sql;
        EXECUTE truncate_stmt;
        DEALLOCATE PREPARE truncate_stmt;

    END LOOP;

END;;

DROP PROCEDURE IF EXISTS ps_history.auto_cleanup_history;

CREATE DEFINER=root@localhost PROCEDURE ps_history.auto_cleanup_history()
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_retention_period VARCHAR(64);

    -- get the retention period
    SELECT value
      INTO v_retention_period
      FROM ps_history.psh_settings
     WHERE variable = 'retention_period';

    CALL ps_history.cleanup_history(v_retention_period); 

END;;

-- Added to fix consistency with documentation.  
-- Fix issue #2
DROP PROCEDURE IF EXISTS ps_history.set_collection_interval;
CREATE DEFINER=root@localhost PROCEDURE ps_history.set_collection_interval(v_interval INT)
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
  CALL ps_history.set_collect_interval(v_interval);
END;

DROP PROCEDURE IF EXISTS ps_history.set_collect_interval;

CREATE DEFINER=root@localhost PROCEDURE ps_history.set_collect_interval(v_interval INT)
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    START TRANSACTION;
    UPDATE ps_history.psh_settings SET value = v_interval WHERE variable = 'interval';
    SELECT 'Updated interval setting' as message;
    COMMIT;
END;;

DROP PROCEDURE IF EXISTS ps_history.set_retention_period;

CREATE DEFINER=root@localhost PROCEDURE ps_history.set_retention_period(v_retention_period VARCHAR(64))
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    START TRANSACTION;
    CALL ps_history.test_retention_period(v_retention_period);
    UPDATE ps_history.psh_settings SET value = v_retention_period WHERE variable = 'retention_period';
    SELECT 'Updated retention period setting' as message;
    COMMIT;
END;;

DROP PROCEDURE IF EXISTS collect;;

CREATE DEFINER=root@localhost PROCEDURE ps_history.collect()
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_done BOOLEAN DEFAULT FALSE;
    DECLARE v_sql TEXT;
    DECLARE v_count INT;
    DECLARE v_created_table BOOLEAN DEFAULT FALSE;
    DECLARE v_i INTEGER DEFAULT 0;
    DECLARE v_col INTEGER DEFAULT 0;
    DECLARE v_table VARCHAR(64); 
    DECLARE v_collist TEXT;
    DECLARE v_max INT DEFAULT 0;

    DECLARE table_cur CURSOR FOR
    SELECT table_name,
           COUNT(*) cnt
      FROM INFORMATION_SCHEMA.COLUMNS
     WHERE table_schema = 'performance_schema'
     GROUP BY table_name
     ORDER BY count(*) DESC;

    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

    SET group_concat_max_len := @@max_allowed_packet;

    -- moved from end of procedure to start of procedure to fix issue #3
    DELETE FROM ps_history.psh_last_refresh;
    INSERT INTO ps_history.psh_last_refresh VALUES (now());


    SELECT GET_LOCK('ps_snapshot_lock',0) INTO @have_lock;
    IF @have_lock = 1 THEN

        SET v_done = FALSE;
        OPEN table_cur;
        tableLoop: LOOP

            FETCH table_cur
             INTO v_table, 
                  v_count;
          
            IF v_done THEN
                CLOSE table_cur;
                LEAVE tableLoop;
            END IF;

            -- Create a temporary table to store the snapshot in, but only create it on first loop iteration
            IF NOT v_created_table THEN
                SET v_max := v_count; -- sort is descending so first table has the most columns
                SET v_sql = '';
                SET v_created_table = TRUE;
                SET v_col = 1;
	        SET v_i := v_count;
                WHILE(v_i >= 1) DO
                    IF v_sql != '' THEN
                        SET v_sql := CONCAT(v_sql, ',\n');
                    END IF;

                    SET v_sql = CONCAT(v_sql,'col',v_col,' TEXT');
                    SET v_i := v_i - 1; 
                    SET v_col := v_col + 1;
                END WHILE;

                SET v_sql = CONCAT('CREATE TEMPORARY TABLE ps_history.snapshot(table_name varchar(64), server_id INT UNSIGNED, ts DATETIME(6), KEY(table_name),',v_sql,')');

                SET @v_sql := v_sql;
                PREPARE create_stmt FROM @v_sql;
                EXECUTE create_stmt;
                DEALLOCATE PREPARE create_stmt;
                SET v_sql = '';
            END IF;

            IF v_sql != '' THEN
                SET v_sql := CONCAT(v_sql, ' UNION ALL ');
            END IF;

            -- Get the list of columns from the table
            SELECT GROUP_CONCAT(column_name ORDER BY ORDINAL_POSITION SEPARATOR ', ')
              INTO v_collist
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE table_schema = 'performance_schema'
               AND table_name = v_table
             GROUP BY table_name;

            -- PAD the SELECT with NULL values so that the column count is right for insertion into the temp table
            IF v_count < v_max THEN
                SET v_collist := CONCAT(v_collist, REPEAT(",NULL", v_max - v_count));
            END IF;

            SET v_sql := CONCAT(v_sql, '(SELECT \'',v_table,'\',@@server_id,NOW(6),', v_collist,' FROM performance_schema.', v_table, ')');

        END LOOP;

        -- Get the data into the temporary snapshot table
        SET @v_sql := CONCAT('INSERT INTO ps_history.snapshot\n', v_sql);
        PREPARE insert_stmt FROM @v_sql;
        EXECUTE insert_stmt;
        DEALLOCATE PREPARE insert_stmt;

        -- Need to re-open the cursor and take data from snapshot table into ps_history tables 
        SET v_done = FALSE;
        OPEN table_cur;
        tableLoop2: LOOP

            FETCH table_cur
              INTO v_table, 
                   v_count;
          
            IF v_done THEN
                CLOSE table_cur;
                LEAVE tableLoop2;
            END IF;

            SET v_i := 1;
            SET v_sql = '';
            WHILE(v_i <= v_count) DO
                IF v_sql != '' THEN
                    SET v_sql := CONCAT(v_sql, ', ');  
                END IF;
                SET v_sql := CONCAT(v_sql, 'col', v_i);
                SET v_i := v_i + 1;
            END WHILE;

            SET @v_sql = CONCAT('INSERT INTO ps_history.', v_table, ' SELECT ', v_sql, ',server_id, ts FROM ps_history.snapshot where table_name = \'', v_table, '\'');
            PREPARE insert_stmt FROM @v_sql;
            EXECUTE insert_stmt;
            DEALLOCATE PREPARE insert_stmt;

        END LOOP;

        DROP TABLE ps_history.snapshot;

        CALL ps_history.auto_cleanup_history();

    END IF;

END;;

DROP PROCEDURE IF EXISTS ps_history.collect_at_interval;

CREATE DEFINER=root@localhost PROCEDURE ps_history.collect_at_interval()
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_last_refresh DATETIME DEFAULT NULL;
    DECLARE v_seconds INT DEFAULT 0; 
    DECLARE v_refresh_interval INT DEFAULT 30;

    -- get the refresh interval
    SELECT value
      INTO v_refresh_interval
      FROM ps_history.psh_settings
     WHERE variable = 'interval';

    -- Set the last refresh the right amount of time in the past if the data has never been refreshed.
    -- Otherwise use the actual last refresh time
    SELECT IF(max(last_refreshed_at) IS NULL, NOW() - INTERVAL v_refresh_interval SECOND,max(last_refreshed_at))  
      INTO v_last_refresh  
      FROM ps_history.psh_last_refresh ;

    -- Figure out how long ago (in seconds) the data was collected
    SET v_seconds = TO_SECONDS(NOW()) - TO_SECONDS(v_last_refresh);

    IF v_seconds >= v_refresh_interval THEN
       CALL ps_history.collect(); 
    END IF;

END;;

DROP PROCEDURE IF EXISTS ps_history.test_interval;;

CREATE DEFINER=root@localhost PROCEDURE ps_history.test_retention_period(v_interval VARCHAR(64))
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_bad BOOLEAN DEFAULT FALSE;
    DECLARE CONTINUE HANDLER FOR SQLSTATE '42000' SET v_bad=TRUE;
    SET @v_sql = CONCAT('SELECT NOW() - INTERVAL ', v_interval, ' INTO @discard FROM DUAL');
    PREPARE test_stmt FROM @v_sql;
    IF v_bad THEN
        SIGNAL SQLSTATE '99999'   
        SET MESSAGE_TEXT = 'Invalid retention period.  Should be 1 DAY, 1 WEEK, 7200 SECOND, etc';
    END IF;
    EXECUTE test_stmt;
    DEALLOCATE PREPARE test_stmt;
END;;


DROP EVENT IF EXISTS ps_history.snapshot_performance_schema;;

CREATE DEFINER=root@localhost EVENT ps_history.snapshot_performance_schema
ON SCHEDULE
EVERY 1 SECOND
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Collect global performance_schema information'
DO
CALL ps_history.collect_at_interval()
;;

SELECT 'Creating ps_history tables' as message;;
call ps_history.setup();;

/* 
triggers can't be created in stored routines, so they 
have to be created here.  They aren't vital to behavior so
it is not so bad if they go missing
*/
CREATE TRIGGER trg_before_delete before DELETE ON ps_history.psh_settings
FOR EACH ROW 
BEGIN  
    SIGNAL SQLSTATE '99999'   
    SET MESSAGE_TEXT = 'You may not delete rows in this table';
END;;

CREATE TRIGGER trg_before_insert before INSERT ON ps_history.psh_settings
FOR EACH ROW 
BEGIN  
    SIGNAL SQLSTATE '99999'   
    SET MESSAGE_TEXT = 'You may not insert rows in this table';
END;;

CREATE TRIGGER trg_before_update before UPDATE ON ps_history.psh_settings
FOR EACH ROW 
BEGIN  
    IF new.variable != 'interval' AND new.variable != 'retention_period' THEN 
        SIGNAL SQLSTATE '99999'   
        SET MESSAGE_TEXT = 'Only the interval and retention_period variables are supported at this time';
    END IF;
    IF new.variable = 'interval' AND CAST(new.value AS SIGNED) < 1 THEN 
        SIGNAL SQLSTATE '99999'   
        SET MESSAGE_TEXT = 'Interval must be greater than or equal to 1';
    END IF;


END;;

SELECT 'Installation complete' as message;;

SELECT IF(@@event_scheduler='ON','Data will be collected into history tables every 30 seconds, unless you change the collection interval with CALL ps_history.set_collection_interval(X) where X is a number >= 1 and <= MAXINT ',
                              'You must enable the event scheduler ( SET GLOBAL event_scheduler=1 ) to collect data into the performance_schema, or run the ps_history.collect() procedure manually') 
as message;;



DELIMITER ;
