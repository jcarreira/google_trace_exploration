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
  std::map<std::string, double> task_to_mem_max_old; // old way of computing these two values
  std::map<std::string, double> task_to_cpu_max_old;
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
    Tokenize(line, tokens, " ");

    std::string job_id = tokens[0];
    std::string task_id = tokens[1];
    std::string machine_id = tokens[2];
    std::string unique_task_id = machine_id + "-" + job_id + "-" + task_id;
    std::string mean_cpu_str = tokens[3];
    std::string canonical_mem_str = tokens[4];
    std::string max_memory_str = tokens[5];
    std::string max_cpu_str = tokens[6];

    //std::cout << unique_task_id << std::endl;

    double mean_cpu = to_T<double>(mean_cpu_str);
    double mean_mem = to_T<double>(canonical_mem_str);
    double max_cpu = to_T<double>(max_cpu_str);
    double max_mem = to_T<double>(max_memory_str);

    if (mean_cpu > 1 || mean_cpu <= 0 || mean_mem > 1 || mean_mem <= 0)
      continue;

    task_to_mem_sum[unique_task_id] += mean_mem;
    task_to_cpu_sum[unique_task_id] += mean_cpu;
    task_to_size[unique_task_id]++;

    task_to_mem_max[unique_task_id] = std::max(
        task_to_mem_max[unique_task_id],
        max_mem);
    task_to_mem_max_old[unique_task_id] = std::max(
        task_to_mem_max_old[unique_task_id],
        mean_mem);
    task_to_cpu_max[unique_task_id] = std::max(
        task_to_cpu_max[unique_task_id],
        max_cpu);
    task_to_cpu_max_old[unique_task_id] = std::max(
        task_to_cpu_max_old[unique_task_id],
        mean_cpu);

    if (line_count % 100000 == 0)
      std::cout << "Line: " << line_count << std::endl;

    if (line_count > 10000000)
      break;
  }

  for (auto it = task_to_size.begin(); it != task_to_size.end(); ++it) {
    std::string unique_task_id = it->first;

    uint64_t size = task_to_size[unique_task_id];

    double cpu_max = task_to_cpu_max[unique_task_id];
    double mem_max = task_to_mem_max[unique_task_id];
    double cpu_sum = task_to_cpu_sum[unique_task_id];
    double mem_sum = task_to_mem_sum[unique_task_id];
    double cpu_avg = cpu_sum / size;
    double mem_avg = mem_sum / size;

    if (cpu_sum == 0 || mem_sum == 0)
      continue;

    double ratio_mem = mem_max / mem_avg;
    double ratio_cpu = cpu_max / cpu_avg;
    double ratio_mem_old = task_to_mem_max_old[unique_task_id] / mem_avg;
    double ratio_cpu_old = task_to_cpu_max_old[unique_task_id] / cpu_avg;

    std::cout
      << cpu_max << " "
      << mem_max << " "
      << cpu_avg << " "
      << mem_avg << " "
      << ratio_cpu << " "
      << ratio_mem << " "
      << ratio_cpu_old << " "
      << ratio_mem_old
      << "\n";
  }

  return 0;
}

