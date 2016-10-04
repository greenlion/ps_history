select 'Settting up ps_history'
\. setup_ps.sql
select 'do not worry if there are errors adding the indexes, you can ignore them'
\. add_ps_history_indexes.sql
select 'do not worry if there were errors adding the indexes, you can ignore them'
select 'Setting up sys_history'
\. setup_sys.sql

SELECT 'Installation complete' as message;

SELECT IF(@@event_scheduler='ON','EVENT SCHEDULER IS ON:\na snapshot will be collected into history tables every 30 seconds, unless you change the collection interval with CALL ps_history.set_collection_interval(X) where X is a number >= 1 and <= MAXINT ' ,
                              'EVENT SCHEDULER IS OFF:\nYou must enable the event scheduler ( SET GLOBAL event_scheduler=1 ) to collect data into the performance_schema, or run the ps_history.collect() procedure manually') 
as message;

