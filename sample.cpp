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

template<class T>
T to_T(const std::string& str) {
    std::istringstream iss(str);
    T res;
    iss >> res;
    return res;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cout << "./sample <period> <file>" << std::endl;
        return -1;
    }

    uint64_t period = to_T<uint64_t>(argv[1]);
    std::ifstream fin(argv[2]);

    std::string line;
    uint64_t line_count = 0;

    while (std::getline(fin, line)) {

        if (line_count % period == 0) {
            printf("%s\n", line.c_str());
        }
        line_count++;

    }

    return 0;
}

