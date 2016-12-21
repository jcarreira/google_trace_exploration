awk -F, '{print $4}' table_job_events.csv | sort | uniq -c
