CPPFLAGS = -O3 -std=c++1z

all: analyze_assigned_max_mem_usage convert analyze_resource_unpredictability analyze_resources_used analyze_resource_unpredictability2

analyze_assigned_max_mem_usage: analyze_assigned_max_mem_usage.cpp
	g++ -O3 -o $2 $^ --std=c++1z

analyze_resources_used: analyze_resources_used.cpp
	g++ -O3 -o analyze_resources_used analyze_resources_used.cpp --std=c++1z

convert: convert.cpp
	g++ -O3 -o convert convert.cpp --std=c++1z

analyze_resource_unpredictability: analyze_resource_unpredictability.cpp
	g++ -O3 -o analyze_resource_unpredictability analyze_resource_unpredictability.cpp --std=c++1z

analyze_resource_unpredictability2: analyze_resource_unpredictability2.cpp
	g++ -O3 -o analyze_resource_unpredictability2 analyze_resource_unpredictability2.cpp --std=c++1z
