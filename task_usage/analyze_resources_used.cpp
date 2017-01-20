#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <stdexcept>
#include <map>

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

template<class T>
T to_T(const std::string& str) {
    std::istringstream iss(str);
    T res;
    iss >> res;
    return res;
}

using Elem = std::pair<std::string,std::pair<double, double>>;

struct cmp {
    bool operator()(const Elem& a, const Elem& b) const {
        uint64_t t1 = to_T<uint64_t>(a.first);
        uint64_t t2 = to_T<uint64_t>(b.first);
        return t1 < t2;
    }
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cout << "./analyze <file>" << std::endl;
        return -1;
    }

    std::ifstream fin(argv[1]);
    std::string line;
    uint64_t line_count = 0;

    std::map<uint64_t, double> events_mem;
    std::map<uint64_t, double> events_cpu;
    //std::vector<Elem> events;

    while (std::getline(fin, line)) {
        line_count++;
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

        std::string start = tokens[0];
        std::string end = tokens[1];
        std::string cpu = tokens[5];
        std::string mem = tokens[6];
        double cpu_double = to_T<double>(cpu);
        double mem_double = to_T<double>(mem);
        uint64_t start_time = to_T<uint64_t>(start);
        uint64_t end_time = to_T<uint64_t>(end);

        if (cpu_double < 0 || cpu_double > 1 || mem_double < 0 || mem_double > 1)
            continue;

        events_mem[start_time] += mem_double;
        events_cpu[start_time] += cpu_double;
        events_mem[end_time] -= mem_double;
        events_cpu[end_time] -= cpu_double;
    }

    double total_cpu = 0;
    double total_mem = 0;
    uint64_t counter = 0;
    for (auto it_mem = events_mem.begin(), it_cpu = events_cpu.begin();
            it_mem != events_mem.end(); ++it_mem, ++it_cpu) {

        if (it_mem->first != it_cpu->first)
            throw std::runtime_error("Error");

        auto time = it_cpu->first;
        total_cpu += it_cpu->second;
        total_mem += it_mem->second;

        //if (counter %3 == 0) {
            std::cout << time << " "
                << total_cpu << " "
                << total_mem
                << "\n";
        //}

        counter++;
    }

    return 0;
}
