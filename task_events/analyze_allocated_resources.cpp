#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>
#include <cstring>
#include <utility>

using namespace std;

// we print for each timestamp the
// total amount of allocated resources

static const uint64_t MILLION = 1000000;
static const uint64_t GB = (1024*1024*1024);
static const uint64_t FILE_SIZE = 20*GB;
char* data;

template<class T>
T string_to_T(const std::string& str) {
    std::istringstream iss(str);
    T ret;
    iss >> ret;
    return ret;
}

uint64_t read_file(char*& data, const std::string& file) {
    data = new char[FILE_SIZE];
    memset(data, 0, FILE_SIZE);
    if (!data)
        exit(-1);

    FILE* fin = fopen(file.c_str(), "r");

    uint64_t size = fread(data, 1, FILE_SIZE, fin);

    std::cout << "Read file: " << file << " size: " << size << std::endl;

    fclose(fin);
    return size;
}

void Tokenize(uint64_t first, uint64_t last,
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

int main(int argc, char* argv[]) {
    if (argc != 2) {
        puts("./analyze_allocated_resources <file>");
        return -1;
    }
    std::string line;
    int l = 0;

    uint64_t size = read_file(data, argv[1]);
    uint64_t first = 0, last = 0;

    std::map<std::string, double> task_to_cpu;
    std::map<std::string, double> task_to_mem;

    std::vector<std::pair<std::string, std::pair<double,double>>> events;
    // total resources scheduled on machines
    double total_mem = 0;
    double total_cpu = 0;

    std::cout << "Starting analysis" << std::endl;

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
        Tokenize(first, last, tokens, ",");

        if (tokens.size() != 13)
            throw std::runtime_error("Wrong number of fields");

        std::string timestamp = tokens[0];
        std::string job_id = tokens[2];
        std::string task_id = tokens[3];
        std::string machine_id = tokens[4];
        std::string task_unique_id = job_id + "-" + task_id;
        //std::string task_unique_id = machine_id + "-" + job_id + "-" + task_id;
        std::string event_type = tokens[5];
        std::string cpu = tokens[9];
        std::string mem = tokens[10];
        double cpu_double = string_to_T<double>(cpu);
        double mem_double = string_to_T<double>(mem);


//        if (machine_id == "" ||
//                event_type == "" ||
//                cpu == "" ||
//                mem == "")
//            continue;

        if (event_type == "1" ||
                event_type == "7" ||
                event_type == "8") {
            // task being scheduled in machine
        
            if (cpu_double > 1 || cpu_double < 0 || mem_double < 0 || mem_double > 1)
                continue;

            if (task_to_cpu.find(task_unique_id) != task_to_cpu.end()) {
                if (task_to_mem.find(task_unique_id) == task_to_mem.end())
                    throw std::runtime_error("Error");

                // already exist fix that
                total_cpu -= task_to_cpu[task_unique_id];
                total_mem -= task_to_mem[task_unique_id];
            }

            task_to_cpu[task_unique_id] = cpu_double;
            task_to_mem[task_unique_id] = mem_double;

            total_cpu += cpu_double;
            total_mem += mem_double;

            events.push_back(std::make_pair(timestamp,
                        std::make_pair(total_cpu, total_mem)));
        } else if (event_type == "2" ||
                event_type == "3" ||
                event_type == "4" ||
                event_type == "5" ||
                event_type == "6") {

            if (task_to_cpu.find(task_unique_id) != task_to_cpu.end()) {
                if (task_to_mem.find(task_unique_id) == task_to_mem.end())
                    throw std::runtime_error("Error2");

                total_cpu -= task_to_cpu[task_unique_id];
                task_to_cpu.erase(task_to_cpu.find(task_unique_id));

                total_mem -= task_to_mem[task_unique_id];
                task_to_mem.erase(task_to_mem.find(task_unique_id));
            }
        }

        l++;
        if (l % MILLION / 10 == 0)
            std::cout << "Line: " << l << std::endl;

//        if (l > 5000000)
//            break;
    }

    std::cout << "Printing output. # events: " << events.size() << std::endl;

    for (uint64_t i = 0; i < events.size(); ++i) {
        if (i < events.size() - 1 && events[i+1].first == events[i].first)
            continue;
        std::cout << events[i].first << " "
            << events[i].second.first << " "
            << events[i].second.second
            << "\n";
    }

    return 0;
}

