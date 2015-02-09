Snapshot creation scripts for performance schema
======
The purpose of these scripts is to provide a mechanism for periodically snapshotting the contents of the MySQL Performance_Schema.  This will allow you to look at historical trends and changes in the Performance_Schema information, something that you can't do out of the box.

Installation
======
To install the ps_history schema, use the provided *setup.sql* script.  It will create the ps_history database and run the *ps_history.setup()* script to create history tables that automatically match the installed performance_schema tables.  

If you are running 5.7 you'll automatically get history into the 5.7 tables, such as the memory tables as the setup will copy all available performance_schema tables.

Event scheduler
======
You should turn the event scheduler on to collect data.  There is a stored routine called *ps_history.collect()* which snapshots the performance schema table data into the history tables in a transactionally consistent manner.  If you don't want to use the event that comes with ps_history, you can run *ps_history.collect()* manually at any time.

By default data is collected every 30 seconds.  You do not need to modify the event to change the collection interval.  Instead, use the *ps_history.set_collection_interval(<seconds>)* procedure.  For example, to collect data every fifteen seconds:
*CALL ps_history.set_collection_interval(15);*

You can also update the psh_settings table directly:
UPDATE ps_history.psh_settings set value = 15 where variable = 'interval';

Automatic data retention and cleanup
======
By default, ps_history will automatically remove history more than one week old.  Depending on your needs, this interval may be too short or too long.  Use *ps_history.set_retenion_period(X);* to set the retention period.  For example, to retain two weeks of history: *CALL ps_history.set_retention_period('2 WEEK');*.  Use the SQL INTERVAL which is appropriate.  The function will error out if you give an invalid SQL INTERVAL.  

Manually cleaning up the ps_history tables
======
There are two stored routines you can use to clean up ps_history data.  You can remove all data with *CALL ps_history.truncate_tables();*.  You can remove only a subset of older data using *CALL ps_history.cleanup_history('1 DAY');* to delete all records older than 1 days old.  You can use any argument to the INTERVAL keyword, so to delete data 1 week old you could do *CALL ps_history.cleanup_history('1 WEEK');*.
