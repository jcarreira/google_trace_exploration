#!/usr/bin/python
import glob, os
import subprocess

#decompress.py  machine_attributes  MD5SUM  schema.csv  SHA256SUM         task_events
#job_events     machine_events      README  SHA1SUM     task_constraints  task_usage

print("generating output" ,file)
for file in sorted(glob.glob("part*csv")):
    print file
    cmd = "cat " + file + " >> table_machine_attributes.csv"
    print cmd
    subprocess.call(cmd, shell=True)
#subprocess.call(['cat','results.csv', 'file', '>','results.csv2'], shell=True)


