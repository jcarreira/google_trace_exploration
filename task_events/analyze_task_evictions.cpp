#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>
#include <cstring>
#include <cassert>

/*
 In this analysis we plot the amount of resources (separately mem and cpu)
 belonging to running tasks that are evicted
  */

using namespace std;

static const uint64_t GB = (1024*1024*1024);

uint64_t read_file(char*& data, const std::string& file, uint64_t fsize) {
    data = new char[fsize];
    memset(data, 0, fsize);
    if (!data)
        exit(-1);

    FILE* fin = fopen(file.c_str(), "r");

    uint64_t size = fread(data, 1, fsize, fin);

    std::cout << "Read size: " << size << std::endl;

    fclose(fin);
    return size;
}

template<class T>
T string_to_T(const std::string& str) {
    std::istringstream iss(str);
    T ret;
    iss >> ret;
    return ret;
}

void Tokenize(const char* data, uint64_t first, uint64_t last,
        vector<string>& tokens,
        const string& delimiters = " ")
{
    uint64_t i = first;
    uint64_t j = first;

    while (1) {
        while(data[j] && data[j] != delimiters[0])
            ++j;

        tokens.push_back(std::string(data + i, j - i));

        ++j; // skip delimiter
        if (j >= last) break;

        i = j;
    }
}

void read_machine_attributes(std::map<std::string, double>& machine_to_cpu,
        std::map<std::string, double>& machine_to_mem) {
    char* data;
    uint64_t size = read_file(data,
            "../machine_events/table_machine_events.csv",
            3 * GB);

    uint64_t first = 0, last = 0;
    while (1) {
        if (last >= size || !data[last])
            break;

        // update left pointer
        first = last;
        while (last < size && data[last] != '\n' && data[last])
            last++;

        last++; // jump the newline. if its null we hope the next is null too
        
        std::vector<std::string> tokens;
        Tokenize(data, first, last, tokens, ",");
        

        std::string machine_id = tokens[1];
        std::string cpu = tokens[4];
        std::string mem = tokens[5];
        
        std::cout << machine_id << " "
            << cpu << " "
            << mem
            << std::endl;

        machine_to_cpu[machine_id] = string_to_T<double>(cpu);
        machine_to_mem[machine_id] = string_to_T<double>(mem);

    }

    delete[] data;
}

struct Event {
    std::string timestamp;
    std::string unique_task_id; // job_id+task_id
    char event_type;
    double mem;
    double cpu;
    bool is_evicted;
};

void read_task_events(std::vector<Event> events) {
    char *data;
    const uint64_t file_size = 110*GB;
    uint64_t size = read_file(data, "table_task_events.csv", file_size);

    uint64_t first = 0, last = 0;

    // find lines
    while (1) {
        if (last >= size || !data[last])
            break;

        // update left pointer
        first = last;
        while (last < size && data[last] != '\n' && data[last])
            last++;

        last++; // jump the newline. if its null we hope the next is null too
        
        std::vector<std::string> tokens;
        Tokenize(data, first, last, tokens, ",");

        char event_type = tokens[5][0];

        if (event_type == '0') { // submitted
            continue;
        }

        std::string timestamp = tokens[0];
        std::string cpu = tokens[9];
        std::string mem = tokens[10];
        std::string unique_task_id = tokens[2] + "-" + tokens[3];
        double cpu_double = string_to_T<double>(cpu);
        double mem_double = string_to_T<double>(mem);

        if (event_type == '1') {
            events.push_back(Event{timestamp,
                    unique_task_id,
                    event_type,
                    mem_double,
                    cpu_double,
                    false});
        }
        // if it is an eviction, we go back and we mark submission event
        else if (event_type == '2') {
            uint64_t i = events.size() - 1;
            while (i >= 0) {
                if (events[i].unique_task_id == unique_task_id) {
                    if (events[i].event_type != '1') {
                        std::cout << "Error task_id: " << unique_task_id << std::endl;
                        break;
                    } else {
                        events[i].is_evicted = true;
                        events.push_back(Event{timestamp,
                                unique_task_id,
                                '3', // mark task as finished
                                mem_double,
                                cpu_double,
                                true});
                    }
                }
                --i;
            }
        } else {
            // we ignore
        }
    }

    delete[] data;

}

int main() {
    std::string line;
    std::map<std::string, int> m;
    int l = 0;

    // read task events
    std::cout << "Reading task events" << std::endl;
    std::vector<Event> events;
    read_task_events(events);

    uint64_t i = 0;
    double total_cpu = 0;
    double total_mem = 0;
    for (; i < events.size(); ++i) {
        if (events[i].is_evicted) {
            if (events[i].event_type == '1') {
                total_cpu += events[i].cpu;
                total_mem += events[i].mem;
            } else {
                total_cpu -= events[i].cpu;
                total_mem -= events[i].mem;
            }
            std::cout << events[i].timestamp << " "
                << total_cpu << " "
                << total_mem
                << "\n";
        }
    }


    return 0;
}
