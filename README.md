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

