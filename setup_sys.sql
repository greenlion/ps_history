DELIMITER ;;
/*  sys_history 
    Copyright 2015,2016 Justin Swanhart

    sys_history is free software: you can redistribute it and/or modify
    it under the terms of the Lesser GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    sys_history is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with FlexViews in the file COPYING, and the Lesser extension to
    the GPL (the LGPL) in COPYING.LESSER.
    If not, see <http://www.gnu.org/licenses/>.
*/
SET NAMES UTF8;;

DROP DATABASE IF EXISTS sys_history;;
CREATE DATABASE IF NOT EXISTS sys_history;;
USE sys_history;;

/* sys_history.rewrite
+==========================================================+
| This block of code will search the from clause for tables|
| and joins, adding linking expressions for the `server_id`|
| and `ts` columns in the ps_history tables between each of|
| the tables, and it will add a projection of the server_id|
| and ts columns from the first table in the query, etc.   |
+==========================================================+
*/

drop function if exists rewrite;;
create function rewrite(v_sql LONGTEXT, v_type enum('VIEW','TABLE','INSERT'), v_table varchar(64) character set utf8 collate utf8_general_ci)
returns LONGTEXT
DETERMINISTIC
BEGIN
  set @at = locate(' from ', v_sql);
  set @new_from = '';
  set @before_from = '';
  set @after_from = '';
  -- if ifnull(@at,0) > 0 then
    set @from := substr(v_sql,@at, length(v_sql)-@at);
    set @before_from := left(v_sql, @at);

    -- from clause ends at a WHERE or GROUP BY clause
    set @at2 := locate(' where ', lower(v_sql));
    if @at2 = 0 then
      set @at2 := locate(' group by ', lower(v_sql));
    end if;
    if @at2 = 0 then
      set @at2 := locate(' order by ', lower(v_sql));
    end if;

    -- extract the from clause from the remaining clauses
    if @at2 != 0 then
      set @from := substr(v_sql,@at,@at2-@at);
      set @after_from := substr(v_sql, @at2);
    end if;

    -- index into the from clause
    set @at := 1;

    -- if in an ON clause
    set @in_on := false;

    -- 1 if new clauses have already been added to this ON clause 
    set @set_it := 0;

    -- count of open parenthesis
    set @p_cnt := 0;

    -- temporary variable for current table name
    set @tmp := null;

    set @new_from = '';

    -- first table is the first table in the FROM clause
    set @first_table = null;

    -- second table is any other table in the query
    set @second_table = '';

    -- flag for if inside a ` escaped object
    set @in_backtick = 0;

    theLoop: LOOP
      if(@at > length(@from)) then
        leave theLoop;
      end if;

      set @c := substr(@from, @at, 1);
      set @next := substr(@from, @at+1, 1);
      set @prev := substr(@from, @at-1, 1);
      set @token := substr(@from, @at, 3);
      set @next2 := substr(@from, @at+1, 2);

      -- if this in an ON clause
      -- this block uses @first_table and @second_table which will have been
      -- set in the else block
      if @token = ' on' or @token = 'on(' or @token = 'n((' then
        set @at := @at + length(@token);
        set @in_on := true;
        set @set_it := 0;
        set @new_from = concat(trim(@new_from), @token);
        iterate theLoop;
      end if;

      set @at := @at + 1;
      
      if @in_on = true then

        if @c = '(' then
          set @p_cnt := @p_cnt + 1;
          set @new_from := concat(@new_from, @c);
          iterate theLoop;
        end if;

        if @c = ')' then
          set @p_cnt := @p_cnt - 1;
          if @p_cnt = 0 then
            set @in_on = false;
          end if;
          set @new_from := concat(@new_from, @c);
          iterate theLoop;
        end if;

        -- insert the new JOIN clauses here so that each JOIN includes the snapshot details
        if @set_it = 0 and @p_cnt > 0  then
          set @set_it := 1;
          set @new_from := CONCAT(@new_from, ' ', @first_table, '.`server_id` = ', @second_table, '.`server_id` AND ');
          set @new_from := CONCAT(@new_from, ' ', @first_table, '.`ts` = ', @second_table, '.`ts` AND ');
        end if;
        set @new_from := concat(@new_from, @c);

      -- find quoted identifiers of form `schema`.`table` and extract them for 
      -- processing, storing them in either @first_table (when this is the first 
      -- table in the query) or @second_table otherwise. 
      else 
        set @new_from := concat(@new_from, @c);
        if @c = '`' or @in_backtick = 1 then
          -- if there is an alias, this will pick up the alias name
          -- instead of the base table name because view expressions
          -- reference columns by alias
          if v_sql like '%join%' then
            
            -- if @in_backtick and @c = '`' and @next2 like '%`%' then
            if @in_backtick and @c = '`' and @next2  = ' `' then
              set @tmp := @token;
              set @in_backtick := 0;
              iterate theLoop;
            end if;
          end if;

          -- This will only match quoted identifiers outside of the ON clauses of 
          -- the query because @in_on is false here.
          if @in_backtick = 0 then
            set @in_backtick := 1;
            set @tmp := '`';
          else
            set @tmp = concat(@tmp, @c);
            if @c = '`' then
              if @prev = '.' or @next = '.' then
                iterate theLoop;
              end if;
              set @in_backtick = 0;
              set @tmp := REPLACE(@tmp,'`performance_schema`','`ps_history`');
              set @tmp := REPLACE(@tmp,'`sys`','`sys_history`');
              if @first_table is null then
                set @first_table := @tmp;
                set @tmp := '';
              else
                set @second_table := @tmp;
                set @tmp := '';
              end if;
            end if;
          end if;
        end if;
      end if;
    END LOOP theLoop;

    set @before_from := REPLACE(@before_from,'`performance_schema`','`ps_history`');
    set @before_from := REPLACE(@before_from,'`sys`','`sys_history`');

    set @new_from := REPLACE(@new_from,'`performance_schema`','`ps_history`');
    set @new_from := REPLACE(@new_from,'`sys`','`sys_history`');

    set @after_from := REPLACE(@after_from,'`performance_schema`','`ps_history`');
    set @after_from := REPLACE(@after_from,'`sys`','`sys_history`');
    if @first_table is not null then 
      set @after_from := REPLACE(@after_from,' group by ', concat(' group by ', @first_table, '.`ts` desc, ', @first_table,'.`server_id` desc, '));
      set @after_from := REPLACE(@after_from,' order by ', concat(' order by ', @first_table, '.`ts` desc, ', @first_table,'.`server_id` desc, '));
    end if;

    if @first_table is null and (@before_from like '%sum(%' or @before_from like '%min(' or @before_from like '%count(%' or @before_from like '%max(%' or @before_from like '%avg(%') then
      set @new_from := concat(@new_from, '`');
      set @after_from = ' group by ts, server_id';
    end if;

    set @before_from := REPLACE(@before_from, '`extract_schema_from_file_name`', '`sys`.`extract_schema_from_file_name`');
    set @before_from := REPLACE(@before_from, '`extract_table_from_file_name`', '`sys`.`extract_table_from_file_name`');
    set @before_from := REPLACE(@before_from, '`sys_history`.`ps_thread_account`', '`sys`.`ps_thread_account`');
    set @before_from := REPLACE(@before_from, '`sys_history`.`format_time`', '`sys`.`format_time`');
    set @before_from := REPLACE(@before_from, '`sys_history`.`format_path`', '`sys`.`format_path`');
    set @before_from := REPLACE(@before_from, '`sys_history`.`format_bytes`', '`sys`.`format_bytes`');
    set @before_from := REPLACE(@before_from, '`sys_history`.`format_statement`', '`sys`.`format_statement`');

    set @after_from := REPLACE(@after_from, '`sys_history`.`format_time`', '`sys`.`format_time`');

    if upper(v_type) = 'TABLE' then
      return CONCAT(
        'create table sys_history.`', v_table,'_m`\n',
        '(server_id bigint, ts datetime, key(ts))\n',
        'CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI PARTITION BY KEY(ts) PARTITIONS 7\n',
        'AS\n',
        -- this is the rewritten SELECT statement for the view
        'select ', @first_table, '.`server_id`, ', @first_table, '.`ts`,\n', 
        substr(@before_from, 8), '\n', @new_from, '\n', @after_from
      );
    end if;

    if upper(v_type) = 'VIEW' then
      return CONCAT(
        'create or replace view sys_history.`', v_table,'`\n',
        'AS\n',
        'select ', 
        IF(@first_table is not null,concat(@first_table, '.`server_id`, '), 'server_id, '), 
        IF(@first_table is not null,concat(@first_table, '.`ts`,'), 'ts, '),
        '\n', 
        substr(@before_from, 8), '\n', @new_from, '\n', @after_from
      );
    end if;

    if upper(v_type) = 'INSERT' then
      return(CONCAT(
        'INSERT INTO sys_history.`', v_table,'_m`\n',
        'select ', @first_table, '.`server_id`, ', @first_table, '.`ts`,\n', 
        substr(@before_from, 8), '\n', @new_from, '\n',
        replace(@after_from, ' where ',  concat(' where ', @first_table, '.`ts` = (select max(`ts`) from ', @first_table, ') and '))
      ));
    end if;

    -- invalid v_type returns empty string
    return(NULL);

END;;

DROP PROCEDURE IF EXISTS setup;;

CREATE DEFINER=root@localhost PROCEDURE sys_history.setup()
MODIFIES SQL DATA
SQL SECURITY DEFINER
BEGIN
    DECLARE v_done BOOLEAN DEFAULT FALSE;
    DECLARE v_table VARCHAR(64); 
    DECLARE v_view TEXT;
    DECLARE v_create TEXT;
    DECLARE v_insert TEXT;

    -- this cursor creates views which access the entire history
    declare cursor0 cursor
    for
    select table_name,
           sys_history.rewrite(view_definition, 'VIEW', table_name) as new_view_definition,
           view_definition, -- sys_history.rewrite(view_definition, 'TABLE', table_name) as new_table_definition,
           NULL  -- sys_history.rewrite(view_definition, 'INSERT', table_name) as insert_definition 
      from information_schema.views 
     where table_schema = 'sys' 
       and table_name 
       NOT IN ( -- these views use the information_schema
         'x$innodb_buffer_stats_by_schema',
         'x$innodb_buffer_stats_by_table',
         'x$innodb_lock_waits',                             
         'x$schema_flattened_keys',
         'x$latest_file_io',
         'innodb_buffer_stats_by_schema', 
         'innodb_buffer_stats_by_table', 
         'innodb_lock_waits',
         'schema_flattened_keys',
         'latest_file_io',
         'session',
         'version'
         -- uses a subquery
         ,'x$ps_digest_95th_percentile_by_avg_us'
       )
       and view_definition not like '%x$innodb_buffer_stats_by_schema%'
       and view_definition not like '%x$innodb_buffer_stats_by_table%'
       and view_definition not like '%x$innodb_lock_waits%'
       and view_definition not like '%x$schema_flattened_keys%'
       and view_definition not like '%x$latest_file_io%'
       and view_definition not like '%x$ps_digest_95th_percentile_by_avg_us%'
       and view_definition not like '%information_schema%'
     -- the x$ views must be created first 
     -- and those x$ views that don't rely on other x$ views must be created first
     -- each not like will return 1 for each table that doesn't match the
     -- expression, so by ordered DESC on both expression, it is ensured that
     -- the views which other views depend on are created first
     order by table_name not like '%x$%', view_definition not like '%x$%' desc;

    DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET v_done=TRUE;

    CREATE DATABASE IF NOT EXISTS sys_history;

    SET group_concat_max_len := @@max_allowed_packet;

    SET v_done = FALSE;
    OPEN cursor0;
    tableLoop: LOOP

      FETCH cursor0
        INTO v_table,
             v_view, 
             v_create, 
             v_insert;
          
      IF v_done THEN
        CLOSE cursor0;
        LEAVE tableLoop;
      END IF;

      -- print out the CREATE statements so the user knows what is happening
      -- SELECT v_table, v_view, v_create, v_insert;
      select v_table;
      select v_create;
      SET @v_sql := v_view;
      SELECT @v_sql as 'creating...';
      PREPARE create_stmt FROM @v_sql;
      EXECUTE create_stmt;
      DEALLOCATE PREPARE create_stmt;

    END LOOP;

    /*

    DROP TABLE IF EXISTS sys_history.sh_settings;
    DROP TABLE IF EXISTS sys_history.sh_last_refresh;

    -- These are sys_history specific tables.  There are triggers defined on sh_settings below.
    CREATE TABLE sys_history.sh_settings(variable varchar(64), key(variable), value varchar(64)) CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI engine = InnoDB;
    INSERT INTO sys_history.sh_settings VALUES ('interval', '30');
    INSERT INTO sys_history.sh_settings VALUES ('retention_period', '1 WEEK');
    CREATE TABLE sys_history.sh_last_refresh(last_refreshed_at DATETIME(6) NOT NULL) engine=InnoDB CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI;
    */

END;;

call setup;;

DELIMITER ;
