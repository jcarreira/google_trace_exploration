all: analyze_task_evictions analyze analyze_machine_cpu_mem analyze_task_cpu_mem

analyze_task_evictions: analyze_task_evictions.cpp
	g++ -ggdb -o analyze_task_evictions analyze_task_evictions.cpp --std=c++1z -O3

analyze: analyze.cpp
	g++ -ggdb -o analyze analyze.cpp --std=c++1z -O3

analyze_machine_cpu_mem: analyze_machine_cpu_mem.cpp
	g++ -ggdb -o analyze_machine_cpu_mem analyze_machine_cpu_mem.cpp --std=c++1z -O3


analyze_task_cpu_mem: analyze_task_cpu_mem.cpp
	g++ -ggdb -o analyze_task_cpu_mem analyze_task_cpu_mem.cpp --std=c++1z -O3

clean:
	rm -rf analyze_task_evictions
