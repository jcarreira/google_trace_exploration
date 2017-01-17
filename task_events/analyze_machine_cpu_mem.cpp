#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>
#include <cstring>

using namespace std;

// for each machine we output
// the CPU/MEM of allocated resources
// at the point when CPU allocation is maximum

static const uint64_t GB = (1024*1024*1024);
static const char file[] = "table_task_events.csv";
static const uint64_t FILE_SIZE = 20*GB;
char* data;

template<class T>
T string_to_T(const std::string& str) {
    std::istringstream iss(str);
    T ret;
    iss >> ret;
    return ret;
}

uint64_t read_file(char*& data) {
    data = new char[FILE_SIZE];
    memset(data, 0, FILE_SIZE);
    if (!data)
        exit(-1);

    FILE* fin = fopen(file, "r");

    uint64_t size = fread(data, 1, FILE_SIZE, fin);

    std::cout << "Read size: " << size << std::endl;

    fclose(fin);
    return size;
}

void Tokenize2(uint64_t first, uint64_t last,
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

int main() {
    std::ifstream fin(file);

    std::string line;
    int l = 0;

    uint64_t size = read_file(data);
    uint64_t first = 0, last = 0;

    std::map<std::string, double> task_to_cpu;
    std::map<std::string, double> task_to_mem;
    std::map<std::string, std::string> task_to_machine;
    std::map<std::string, double> machine_to_cpu; // record machine allocation
    std::map<std::string, double> machine_to_mem; // record machine allocation
    std::map<std::string, double> machine_to_cpu_max; // max machine allocation
    std::map<std::string, double> machine_to_mem_max; // max machine allocation (when cpu is max)

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
        Tokenize2(first, last, tokens, ",");

        std::string job_id = tokens[2];
        std::string task_id = tokens[3];
        std::string task_unique_id = job_id + "-" + task_id;
        std::string machine_id = tokens[4];
        std::string event_type = tokens[5];
        std::string cpu = tokens[9];
        std::string mem = tokens[10];

        if (machine_id == "" ||
                event_type == "" ||
                cpu == "" ||
                mem == "")
            continue;

        if (event_type == "1") {
            // task being scheduled in machine
            double cpu_double = string_to_T<double>(cpu);
            double mem_double = string_to_T<double>(mem);

            task_to_cpu[task_unique_id] = cpu_double;
            task_to_mem[task_unique_id] = mem_double;
            task_to_machine[task_unique_id] = machine_id;

            machine_to_cpu[machine_id] += cpu_double;
        } else if (event_type == "2" ||
                event_type == "3" ||
                event_type == "4" ||
                event_type == "5" ||
                event_type == "6") {

        } else if (event_type == "7" ||
                event_type == "8") {

        }

        l++;
        if (l % 1000000 == 0)
            std::cout << "Processing at line: " << l << std::endl;
    }


    return 0;
}
