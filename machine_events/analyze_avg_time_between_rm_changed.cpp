#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>

using namespace std;
std::vector<std::vector<std::string > > main_data;

void Tokenize(const string& str,
        vector<string>& tokens,
        const string& delimiters = " ")
{
    // Skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    string::size_type pos     = str.find_first_of(delimiters, lastPos);

    while (string::npos != pos || string::npos != lastPos)
    {
        // Found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
        // Find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
}

uint64_t to_int(const std::string& str) {
    std::istringstream iss(str);
    uint64_t res;
    iss >> res;
    return res;
}

void average_time_between_remove_or_changed() {
    uint64_t i = 0;
    while (main_data[i][0] == "0")
        i++;
    while (main_data[i][2] == "0")
        i++;

    std::cout << "We are at i: " << i << std::endl;
    uint64_t ts_before = to_int(main_data[i][0]);

    // move to next
    ++i;
    while (main_data[i][2] == "0")
        i++;

    uint64_t count = 0;
    uint64_t total = 0;
    while (i < main_data.size()) {
        uint64_t ts_now = to_int(main_data[i][0]);
        uint64_t interval = ts_now - ts_before;
        total += interval;
        ts_before = ts_now;
        count++;
        ++i;
        while (i < main_data.size() && main_data[i][2] == "0")
            i++;
    }

    std::cout << "Average interval: " << total * 1.0 / count << std::endl;
    std::cout << "count: " << count << std::endl;
    std::cout << "total: " << total << std::endl;
}

int main() {
    std::ifstream fin("table_machine_events.csv");

    std::string line;
    std::map<std::string, int> m;
    while (std::getline(fin, line)) {
        std::cout << "line: " << line << std::endl;
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

     //   std::cout << tokens[0] << " - " << tokens[1] << std::endl;

        m[tokens[2]]++;
        main_data.push_back(tokens);
    }

    average_time_between_remove_or_changed();

    for (auto it = m.begin(); it != m.end(); ++it) {
        std::cout << it->first << " " << it->second << std::endl;
    }

    return 0;
}
