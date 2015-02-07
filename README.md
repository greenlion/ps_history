Snapshot creation scripts for performance schema
======
The purpose of these scripts is to provide a mechanism for periodically snapshotting the contents of the MySQL Performance_Schema.  This will allow you to look at historical trends and changes in the Performance_Schema information, something that you can't do out of the box.

Installation depends on the version of the database you are using.

If you are using MySQL/Percona Server 5.6:
======
1. execute creates_56.sql
2. execute collect_snapshot_event_56.sql
3. ensure the event scheduler is turned on: SET GLOBAL event_scheduler=1; 
4. make sure the event scheduler is also enabled in my.cnf

If you are using MySQL 5.5 or 5.7, or if you are using MariaDB:
======
1. Create the creates_XX.sql file use an INFORMATION_SCHEMA query:

    select concat('CREATE TABLE ps_history.', table_name, ' (ts datetime(6), server_id int unsigned) as select * from  performance_schema.', table_name, ';') 
      from information_schema.tables 
     where table_schema='performance_schema' 
      into outfile '/tmp/creates.sql';

2. CREATE DATABASE ps_history;

3. Execute the /tmp/creates.sql

4. Generate the event script using the provided php (just redirect output to the desired event script name):
php make_collect_snapshot_event.php > /tmp/collect.sql

5. Execute the /tmp/collect.sql

6. ensure the event scheduler is turned on: SET GLOBAL event_scheduler=1; 

7. make sure the event scheduler is also enabled in my.cnf
