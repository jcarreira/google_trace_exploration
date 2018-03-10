#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <stdexcept>
#include <map>

// We start by exploring the difference between reserved and used memory

// sanity checks
// 1. check that maximum memory usage is strictly less than assigned memory usage

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

        //std::cout << "Pushing " << s << std::endl;
        tokens.push_back(s);

        ++j; // skip delimiter
        if (j >= str.size())
            break;

        i = j;
    }
}

double to_double(const std::string& str) {
    std::istringstream iss(str);
    double res;
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
    std::map<std::string, int> m;
    uint64_t line_count = 0;
    uint64_t nonzero_count = 0;
    
    double assigned_memory_usage = 0;
    double maximum_memory_usage = 0;

    while (std::getline(fin, line)) {
        line_count++;
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

        //std::cout << line << std::endl;
        //std::cout << tokens[0] << " " << tokens[1] << " " << tokens[2] << std::endl;
        
//         print assigned memory usage and maximum memory usage

        if (line_count % 400000 == 0) {
            std::cout << "Cumulative assigned memory usage: " << assigned_memory_usage << std::endl;
            std::cout << "Cumulative max memory usage: " << maximum_memory_usage << std::endl;
            std::cout << "Average assigned memory usage: " << assigned_memory_usage / nonzero_count << std::endl;
            std::cout << "Average max memory usage: " << maximum_memory_usage/ nonzero_count << std::endl;
            std::cout << "Avg difference: " << (assigned_memory_usage - maximum_memory_usage) 
                / nonzero_count << std::endl;
        }

        // assigned memory usage
        double amu = to_double(tokens[7]);
        // maximum memory usage
        double mmu = to_double(tokens[10]);

        if (amu == 0 || mmu == 0)
            continue;

        nonzero_count++;
        
//        std::cout << "8: " << tokens[7]
//            << " 9:" << tokens[8] 
//            << " 10:" << tokens[9] 
//            << " 11:" << tokens[10] 
//            << std::endl;

        // make sure the values make sense
        if (!
                ((amu >= 0 && amu <= 1) && (mmu >= 0 && mmu <= 1))) {
            std::cout << "Wrong tokens line: " << line_count << std::endl;
            continue;
            //throw std::runtime_error("Wrong tokens");
        }

        assigned_memory_usage += amu;
        maximum_memory_usage += mmu;
    }

    std::cout << "Cumulative assigned memory usage: " << assigned_memory_usage << std::endl;
    std::cout << "Cumulative max memory usage: " << maximum_memory_usage << std::endl;
    std::cout << "Average assigned memory usage: " << assigned_memory_usage / nonzero_count << std::endl;
    std::cout << "Average max memory usage: " << maximum_memory_usage/ nonzero_count << std::endl;
    std::cout << "Avg difference: " << (assigned_memory_usage - maximum_memory_usage) 
        / nonzero_count << std::endl;

    return 0;
}
