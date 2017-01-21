all: analyze_task_evictions analyze analyze_machine_cpu_mem analyze_task_cpu_mem analyze_allocated_resources analyze_fragmentation

CFLAGS=-ggdb --std=c++1z -O3

analyze_task_evictions: analyze_task_evictions.cpp
	g++ $(CFLAGS) -o analyze_task_evictions analyze_task_evictions.cpp

analyze: analyze.cpp
	g++ $(CFLAGS) -o analyze analyze.cpp

analyze_machine_cpu_mem: analyze_machine_cpu_mem.cpp
	g++ $(CFLAGS) -o analyze_machine_cpu_mem analyze_machine_cpu_mem.cpp

analyze_task_cpu_mem: analyze_task_cpu_mem.cpp
	g++ $(CFLAGS) -o analyze_task_cpu_mem analyze_task_cpu_mem.cpp

analyze_allocated_resources:analyze_allocated_resources.cpp
	g++ $(CFLAGS) -o analyze_allocated_resources analyze_allocated_resources.cpp

analyze_fragmentation: analyze_fragmentation.cpp
	g++ $(CFLAGS) -o analyze_fragmentation analyze_fragmentation.cpp

clean:
	rm -rf analyze_task_evictions analyze analyze_machine_cpu_mem analyze_task_cpu_mem analyze_allocated_resources analyze_fragmentation
