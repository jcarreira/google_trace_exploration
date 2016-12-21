awk -F, '{print $6}' table_task_events.csv | sort | uniq -c
