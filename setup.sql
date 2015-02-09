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
    select CONCAT('CREATE TABLE ps_history.', table_name, '( ', group_concat(concat(column_name, ' ', column_type, if(character_set_name is not null,concat(' CHARACTER SET ', character_set_name),''),if(collation_name is not null,concat(' COLLATE ', collation_name),'')) SEPARATOR ',\n'), ',server_id int unsigned,\nts datetime(6) )') as create_tbl,
           table_name 
      from information_schema.columns 
     where table_schema='performance_schema' 
     group by table_name;

    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

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
      PREPARE create_stmt FROM @v_sql;
      EXECUTE create_stmt;
      DEALLOCATE PREPARE create_stmt;

    END LOOP;

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

    END IF;

END;;

DROP EVENT IF EXISTS ps_history.snapshot_performance_schema;;

CREATE DEFINER=root@localhost EVENT ps_history.snapshot_performance_schema
ON SCHEDULE
EVERY 30 SECOND
ON COMPLETION PRESERVE
ENABLE
COMMENT 'Collect global performance_schema information'
DO
CALL ps_history.collect()
;;

SELECT 'Creating ps_history tables' as message;;
call ps_history.setup();;

SELECT 'Installation complete' as message;;

SELECT IF(@@event_scheduler=1,'Data will be collected into history tables every 30 seconds, unless you change the ps_history.snapshot_performance_schema event',
                              'You must enable the event scheduler ( SET GLOBAL event_scheduler=1 ) to collect data into the performance_schema, or run the ps_history.collect() procedure manually') 
as message;;



DELIMITER ;
