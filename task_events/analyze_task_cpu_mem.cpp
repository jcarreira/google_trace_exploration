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
 For each task we print the ratio between task allocated resources and machine resources
 */

using namespace std;

static const uint64_t GB = (1024*1024*1024);
static const uint64_t FILE_SIZE = 17*GB;
char* data;

uint64_t read_file(char*& data, const std::string& file, uint64_t fsize = FILE_SIZE) {
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

int main() {
    std::string line;
    std::map<std::string, int> m;
    int l = 0;

    std::map<std::string, double> machine_to_cpu;
    std::map<std::string, double> machine_to_mem;

    // read attributes from the machines
    std::cout << "Reading machine atts" << std::endl;
    read_machine_attributes(machine_to_cpu, machine_to_mem);
    // read task events
    std::cout << "Reading task events" << std::endl;
    uint64_t size = read_file(data, "table_task_events.csv");
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

        std::string event_type = tokens[5];

        if (event_type != "1")
            continue;

        std::string cpu = tokens[9];
        std::string mem = tokens[10];
        std::string machine_id = tokens[4];

        double cpu_double = string_to_T<double>(cpu);
        double mem_double = string_to_T<double>(mem);

        std::cout << "Debug. machine_id: " << machine_id <<
            " cpu: " << cpu << " mem: " <<  mem << "\n";

        // if machine is not found we ignore
        if (machine_to_mem.find(machine_id) == machine_to_mem.end() ||
                machine_to_cpu.find(machine_id) == machine_to_cpu.end())
            continue;

        std::cout << cpu_double / machine_to_cpu[machine_id] << " " 
            << mem_double / machine_to_mem[machine_id]
            << "\n";
    }

    return 0;
}
