#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>

/** Prints for each timestamp the total amount
  * of resource capacity (CPU and memory) 
  * in the datacenter
  */

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

    while (string::npos != pos || string::npos != lastPos) {
        // Found a token, add it to the vector.
        tokens.push_back(str.substr(lastPos, pos - lastPos));
        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);
        // Find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
}

template<class T>
T to_T(const std::string& str) {
    std::istringstream iss(str);
    T res;
    iss >> res;
    return res;
}

int main() {
    std::ifstream fin("table_machine_events.csv");

    std::string line;
    std::map<std::string, std::pair<double, double>> machine_to_resources; // cpu/mem
    double total_mem = 0;
    double total_cpu = 0;

    std::vector<std::pair<std::string, std::pair<double, double>>> results;

    while (std::getline(fin, line)) {
        std::cout << "line: " << line << std::endl;
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

        if (tokens.size() != 6) {
            continue;
            //throw std::runtime_error("Wrong number of fields");
        }

        std::string timestamp = tokens[0];
        std::string machine_id = tokens[1];
        std::string event_type = tokens[2];
        std::string cpu = tokens[4];
        std::string mem = tokens[5];
        double cpu_d = to_T<double>(cpu);
        double mem_d = to_T<double>(mem);

        if (event_type == "0") {
            total_cpu += cpu_d;
            total_mem += mem_d;
            machine_to_resources[machine_id] = std::make_pair(cpu_d, mem_d);
        } else if (event_type == "1") {
            if (machine_to_resources.find(machine_id) == machine_to_resources.end()) {
                std::cout << "Ignoring1 machine: " << machine_id << "\n";
                continue; // ignore
            }
            total_cpu -= cpu_d;
            total_mem -= mem_d;
            machine_to_resources.erase(machine_to_resources.find(machine_id));
        } else if (event_type == "2") {
            if (machine_to_resources.find(machine_id) == machine_to_resources.end()) {
                std::cout << "Ignoring2 machine: " << machine_id << "\n";
                continue; // ignore
            }
            total_cpu -= machine_to_resources[machine_id].first;
            total_mem -= machine_to_resources[machine_id].second;

            total_mem += mem_d;
            total_cpu += cpu_d;
            
            machine_to_resources[machine_id] = std::make_pair(cpu_d, mem_d);
        }

        results.push_back(std::make_pair(timestamp,
                    std::make_pair(total_cpu, total_mem)));
    }

    for (uint64_t i = 0; i < results.size(); ++i) {
        auto& t = results[i];
        std::string time = t.first;
        double cpu = t.second.first;
        double mem = t.second.second;
        if (i < results.size() - 1 && time == results[i+1].first)
            continue;
        std::cout << time << " "
            << cpu << " "
            << mem
            << "\n";
    }

    return 0;
}
