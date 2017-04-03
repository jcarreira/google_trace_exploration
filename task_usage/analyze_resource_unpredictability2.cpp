#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <stdexcept>
#include <map>

// check ratio max / average

using namespace std;

void Tokenize(const string& str,
        vector<string>& tokens,
        const string& delimiters = " ")
{
    int i = 0;
    int j = 0;

    while (1) {
        while (str[j] && str[j] != delimiters[0])
            ++j;

        std::string s(str.c_str() + i, j - i);
        if (s.size() == 0)
            s = "-1";

        tokens.push_back(s);

        ++j; // skip delimiter
        if (j >= str.size())
            break;

        i = j;
    }
}

template <class T>
T to_T(const std::string& str) {
    std::istringstream iss(str);
    T res;
    iss >> res;
    return res;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "./analyze <file>" << std::endl;
        return -1;
    }

    std::ifstream fin(argv[1]);
    std::string line;
    std::map<std::string, double> task_to_mem_max;
    std::map<std::string, double> task_to_cpu_max;
    std::map<std::string, uint64_t> task_to_size;
    std::map<std::string, double> task_to_mem_sum;
    std::map<std::string, double> task_to_cpu_sum;
    uint64_t line_count = 0;

    std::cout << "Starting analysis" << std::endl;
    
    while (std::getline(fin, line)) {
        line_count++;
        std::vector<std::string> tokens;

        // compressed file has an issue
        line.erase(line.begin());
        Tokenize(line, tokens, ",");

        uint64_t start_time = to_T<uint64_t>(tokens[0]);
        uint64_t end_time = to_T<uint64_t>(tokens[1]);
        std::string job_id = tokens[2];
        std::string task_id = tokens[3];
        std::string machine_id = tokens[4];
        std::string unique_task_id = machine_id + "-" + job_id + "-" + task_id;
        double cpu = to_T<double>(tokens[5]);
        double mem = to_T<double>(tokens[6]);

        if (cpu > 1 || cpu <= 0 || mem > 1 || mem <= 0)
            continue;

        if (end_time <= start_time)
            continue;

        uint64_t elapsed = end_time - start_time;

        task_to_mem_sum[unique_task_id] += mem * elapsed;
        task_to_cpu_sum[unique_task_id] += cpu * elapsed;
        task_to_size[unique_task_id] += elapsed;
        task_to_mem_max[unique_task_id] = std::max(
                task_to_mem_max[unique_task_id],
                mem);
        task_to_cpu_max[unique_task_id] = std::max(
                task_to_cpu_max[unique_task_id],
                cpu);

        if (line_count % 100000 == 0)
            std::cout << "Line: " << line_count << std::endl;

        if (line_count > 100000000)
            break;
    }

    for (auto it = task_to_size.begin(); it != task_to_size.end(); ++it) {
        std::string unique_task_id = it->first;

        uint64_t total_time = task_to_size[unique_task_id];

        double cpu_max = task_to_cpu_max[unique_task_id];
        double mem_max = task_to_mem_max[unique_task_id];
        double cpu_sum = task_to_cpu_sum[unique_task_id];
        double mem_sum = task_to_mem_sum[unique_task_id];
        double cpu_avg = cpu_sum / total_time;
        double mem_avg = mem_sum / total_time;

        if (cpu_sum == 0 || mem_sum == 0)
            continue;

        double ratio_mem = mem_max / mem_avg;
        double ratio_cpu = cpu_max / cpu_avg;

        std::cout
            << cpu_max << " "
            << mem_max << " "
            << cpu_avg << " "
            << mem_avg << " "
            << ratio_cpu << " "
            << ratio_mem << " "
            << "\n";
    }

    return 0;
}

