#!/usr/bin/python
import glob, os
import subprocess

#decompress.py  machine_attributes  MD5SUM  schema.csv  SHA256SUM         task_events
#job_events     machine_events      README  SHA1SUM     task_constraints  task_usage

print("decompresing job_events" ,file)
os.chdir("./job_events")
for file in glob.glob("*.csv.gz"):
    subprocess.call(['gzip','-d', file])

print("decompresing machine_attributes" ,file)
os.chdir("../machine_attributes")
for file in glob.glob("*.csv.gz"):
    subprocess.call(['gzip','-d', file])

print("decompresing machine_events" ,file)
os.chdir("../machine_events")
for file in glob.glob("*.csv.gz"):
    subprocess.call(['gzip','-d', file])
    
print("decompresing task_constraints" ,file)
os.chdir("../task_constraints")
for file in glob.glob("*.csv.gz"):
    subprocess.call(['gzip','-d', file])

print("decompresing task_events" ,file)
os.chdir("../task_events")
for file in glob.glob("*.csv.gz"):
    subprocess.call(['gzip','-d', file])

print("decompresing task_usage" ,file)
os.chdir("../task_usage")
for file in glob.glob("*.csv.gz"):
    subprocess.call(['gzip','-d', file])
