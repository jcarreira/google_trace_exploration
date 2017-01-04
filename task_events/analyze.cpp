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

static const uint64_t GB = (1024*1024*1024);
//static const char file[] = "minitable";
static const char file[] = "table_task_events.csv";
//static const uint64_t FILE_SIZE = 1*GB;
static const uint64_t FILE_SIZE = 20*GB;
char* data;

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
    std::map<std::string, int> m;
    int l = 0;

    uint64_t size = read_file(data);
    uint64_t first = 0, last = 0;

    // find lines
    while (1) {
       // std::cout << last << " " << size << std::endl;
        if (last >= size || !data[last])
            break;

        // update left pointer
        first = last;
        while (last < size && data[last] != '\n' && data[last])
            last++;

        last++; // jump the newline. if its null we hope the next is null too
        
        std::vector<std::string> tokens;
        Tokenize2(first, last, tokens, ",");

        m[tokens[5]]++;

        l++;
        if (l % 1000000 == 0)
            std::cout << "Processing at line: " << l << std::endl;
    }


    for (auto it = m.begin(); it != m.end(); ++it) {
        std::cout << it->first << " " << it->second << std::endl;
    }

    return 0;
}
