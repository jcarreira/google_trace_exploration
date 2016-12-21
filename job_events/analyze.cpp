#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>

using namespace std;

void Tokenize(const string& str,
        vector<string>& tokens,
        const string& delimiters = " ")
{
    int i = 0;
    int j = 0;

    while (1) {
        while(str[j] && str[j] != delimiters[0])
            ++j;

        //if (j > i) {
            tokens.push_back(std::string(str.c_str() + i, j - i));
        //}

        ++j; // skip delimiter
        //while(str[j] && str[j] == delimiters[0])
        //    ++j;
        if (j >= str.size()) break;

        i = j;
    }
    return;

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

int main() {
    std::ifstream fin("table_job_events.csv");

    std::string line;
    std::map<std::string, int> m;
    while (std::getline(fin, line)) {
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");
  //      std::cout << line << std::endl;
 //       std::cout << tokens[0] << " " << tokens[1] << " " << tokens[2] << std::endl;

        m[tokens[3]]++;
    }

    for (auto it = m.begin(); it != m.end(); ++it) {
        std::cout << it->first << " " << it->second << std::endl;
    }

    return 0;
}
