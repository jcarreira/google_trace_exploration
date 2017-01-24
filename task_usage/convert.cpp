#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <stdexcept>
#include <map>

// sanity checks
// 1. check that maximum memory usage is strictly less than assigned memory usage
// 2. 

// We start by exploring the difference between reserved and used memory

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
        std::cout << "./convert <file>" << std::endl;
        return -1;
    }

    std::ifstream fin(argv[1]);
    std::string line;
    std::map<std::string, int> m;
    uint64_t line_count = 0;
    uint64_t nonzero_count = 0;
    
    while (std::getline(fin, line)) {
        line_count++;
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

        std::cout
            << tokens[0] // start_time
            << "," << tokens[1] // end time
            << "," << tokens[4] // machine id
            << "," << tokens[5] // mean cpu usage
            << "," << tokens[6] // canonical memory usage
            << "\n";

    }

    return 0;
}


