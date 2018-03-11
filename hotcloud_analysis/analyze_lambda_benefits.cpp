#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <iterator>
#include <map>
#include <cstring>
#include <utility>
#include <cassert>
#include <set>
#include <array>
#include <cassert>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <mutex>
#include <thread>

using namespace std;

// we print for each timestamp the
// total amount of allocated resources

static const uint64_t MILLION = 1000000;
static const uint64_t GB = (1024*1024*1024);
static const uint64_t FILE_SIZE = 20*GB;
char* data;

template<class T>
T to_T(const std::string& str) {
    std::istringstream iss(str);
    T ret;
    iss >> ret;
    return ret;
}

uint64_t read_file(char*& data, const std::string& file) {
    data = new char[FILE_SIZE];
    memset(data, 0, FILE_SIZE);
    if (!data)
        exit(-1);

    FILE* fin = fopen(file.c_str(), "r");

    uint64_t size = fread(data, 1, FILE_SIZE, fin);

    fclose(fin);
    return size;
}

void Tokenize(const string& str,
        vector<string>& tokens,
        const string& delimiters = " ")
{
    int i = 0, j = 0;

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

void Tokenize(uint64_t first, uint64_t last,
        vector<string>& tokens,
        const string& delimiters = " ")
{
    uint64_t i = first, j = first;

    while (1) {
        while(data[j] && data[j] != delimiters[0])
            ++j;

        tokens.push_back(std::string(data + i, j - i));

        ++j; // skip delimiter
        if (j >= last) break;

        i = j;
    }
}

struct Event {
    Event(const std::string& et, uint64_t mid) :
        event_type(et),
        machine_id(mid)
    {}

    virtual ~Event() = default;

    std::string event_type;
    uint64_t machine_id;
};

struct MachineEvent : public Event {
    MachineEvent(const std::string& event_type,
            uint64_t mid,
            double c,
            double m) :
        Event(event_type, mid),
        cpu(c),
        mem(m) {}

    double cpu;
    double mem;
};

struct UsageEvent : public Event {
    UsageEvent(const std::string& event_type,
            uint64_t mid,
            double avg_cpu,
            double avg_mem,
            double max_cpu,
            double max_mem) :
        Event(event_type, mid),
        avg_cpu(avg_cpu),
        avg_mem(avg_mem),
        max_cpu(max_cpu),
        max_mem(max_mem) {
          if (max_cpu > 0) {
            assert(max_cpu >= avg_cpu);
            assert(max_mem >= avg_mem);
          }
        }

    double avg_cpu;
    double avg_mem;
    double max_cpu;
    double max_mem;
};

#define MILLION (1000000UL)
#define N_MACHINES (40000)
#define N_TASKS (30 * MILLION)
// maps time to list of events
std::map<uint64_t, std::vector<std::shared_ptr<Event>>> events_map;
// maps machine id to amount of CPU/mem allocated on that machine
bool machine_exists[N_MACHINES];
double machine_to_capacity_mem[N_MACHINES];
double machine_to_capacity_cpu[N_MACHINES];

double machine_to_mem_utilized[N_MACHINES];
double machine_to_max_mem_utilized[N_MACHINES];
double machine_to_max_cpu_utilized[N_MACHINES];

uint64_t machine_counter = 0;
std::unordered_map<std::string, uint64_t> machineid_to_uint;

// we keep a list of usage events per machine
std::map<uint64_t, std::map<uint64_t, std::vector<std::shared_ptr<Event>>>> events_per_machine;

void read_usage_events_thread(
    std::ifstream& fin,
    std::mutex& fin_mutex,
    std::mutex& data_mutex,
    std::atomic<int>& counter) {
  std::string line;
  while (1) {
    fin_mutex.lock();
    std::getline(fin, line);
    fin_mutex.unlock();

    std::vector<std::string> tokens;
    Tokenize(line, tokens, " ");

    assert(tokens.size() == 7);

    uint64_t start_time = to_T<double>(tokens[0]);
    uint64_t end_time = to_T<double>(tokens[1]);
    std::string machine_id = tokens[2];
    double avg_cpu = to_T<double>(tokens[3]);
    double avg_mem = to_T<double>(tokens[4]);
    double max_mem = to_T<double>(tokens[5]);
    double max_cpu = to_T<double>(tokens[6]);

    max_cpu = std::max(max_cpu, avg_cpu);
    max_mem = std::max(max_mem, avg_mem);

    if (machine_id == "")
      continue;

    // machineid not found
    if (machineid_to_uint.find(machine_id) == machineid_to_uint.end())
      continue;

    uint64_t machine_uint = machineid_to_uint[machine_id];

    auto ue_start = std::make_shared<UsageEvent>("", machine_uint, avg_cpu, avg_mem, max_cpu, max_mem);
    auto ue_end = std::make_shared<UsageEvent>("", machine_uint, -avg_cpu, -avg_mem, -max_cpu, -max_mem);

    data_mutex.lock();
    events_map[start_time].push_back(ue_start);
    events_map[end_time].push_back(ue_end);
    events_per_machine[machine_uint][start_time].push_back(ue_start);
    events_per_machine[machine_uint][end_time].push_back(ue_end);
    data_mutex.unlock();

    if (counter % 100000 == 0) {
      std::cout << "usage file counter: " << counter << "\n";
    }

    counter += 1;
  }
}

void read_usage_events_parallel(const std::string& file) {
  std::vector<std::shared_ptr<std::thread>> threads;
    
  std::ifstream fin(file);
  std::mutex fin_mutex;
  std::mutex data_mutex;
  std::atomic<int> counter;

  std::atomic_init(&counter, 0);

  for (int i = 0; i < 50; ++i) {
    threads.push_back(
        std::make_shared<std::thread>(
          read_usage_events_thread,
          std::ref(fin),
          std::ref(fin_mutex),
          std::ref(data_mutex),
          std::ref(counter)
          ));
  }

  for (auto& t : threads) {
    t->join();
  }
}

void read_usage_events(const std::string& file) {
    std::ifstream fin(file);
    std::string line;
    uint64_t counter = 0;
    while (std::getline(fin, line)) {
        std::vector<std::string> tokens;
        Tokenize(line, tokens, " ");

        assert(tokens.size() == 7);
        
        uint64_t start_time = to_T<double>(tokens[0]);
        uint64_t end_time = to_T<double>(tokens[1]);
        std::string machine_id = tokens[2];
        double avg_cpu = to_T<double>(tokens[3]);
        double avg_mem = to_T<double>(tokens[4]);
        double max_mem = to_T<double>(tokens[5]);
        double max_cpu = to_T<double>(tokens[6]);

        max_cpu = std::max(max_cpu, avg_cpu);
        max_mem = std::max(max_mem, avg_mem);

        if (machine_id == "")
            continue;

        // machineid not found
        if (machineid_to_uint.find(machine_id) == machineid_to_uint.end())
            continue;

        uint64_t machine_uint = machineid_to_uint[machine_id];

        auto ue_start = std::make_shared<UsageEvent>("", machine_uint, avg_cpu, avg_mem, max_cpu, max_mem);
        auto ue_end = std::make_shared<UsageEvent>("", machine_uint, -avg_cpu, -avg_mem, -max_cpu, -max_mem);
        events_map[start_time].push_back(ue_start);
        events_map[end_time].push_back(ue_end);

        events_per_machine[machine_uint][start_time].push_back(ue_start);
        events_per_machine[machine_uint][end_time].push_back(ue_end);

        if (counter % 100000 == 0) {
            std::cout << "usage file counter: " << counter << "\n";
        }

        ++counter;
    }
}

void read_machine_events(const std::string& file) {
    std::ifstream fin(file);
    std::string line;
    while (std::getline(fin, line)) {
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

        uint64_t time = to_T<double>(tokens[0]);
        std::string machine_id = tokens[1];
        std::string event_type = tokens[2];
        double cpu = to_T<double>(tokens[4]);
        double mem = to_T<double>(tokens[5]);

        uint64_t machine_uint = 0;
       
        if (machineid_to_uint.find(machine_id) != machineid_to_uint.end()) {
            machine_uint = machineid_to_uint[machine_id];
        } else {
            machine_uint = machine_counter++;
            machineid_to_uint[machine_id] = machine_uint;
        }
       
        std::cout << "Machine event: "
            << " machine_id: " << machine_id
            << " machine_uint: " << machine_uint
            << "\n";

        // XXX fix this because we don't consider changes during runtime
        if (event_type == "0" || event_type == "2") { //add or remove
          machine_to_capacity_mem[machine_uint] = mem;
          machine_to_capacity_cpu[machine_uint] = cpu;
        }

        events_map[time].push_back(std::make_shared<MachineEvent>(event_type, machine_uint, cpu, mem));
    }
}

void clear_datastructures() {
    memset(machine_exists, 0, sizeof(machine_exists));
    //memset(task_exists, 0, sizeof(task_exists));
    memset(machine_to_mem_utilized, 0, sizeof(machine_to_mem_utilized));
    memset(machine_to_max_mem_utilized, 0, sizeof(machine_to_max_mem_utilized));
    memset(machine_to_max_cpu_utilized, 0, sizeof(machine_to_max_cpu_utilized));
}

// I go through every machine and I create a new list of
// resource usage based on the max resource usage X seconds after each second
#define MICROSEC_IN_SECOND (1000000)

// NOTE: we need to start allocating lambdas from the start until the end of the machine lifetime

#define US_TO_SEC(T)((T*1.0)/1000000.0)

void compute_events_per_machine(
    int lambda_lifetime_secs,
    double lambda_cpu_size,
    double lambda_mem_size) {
  // we traverse all machines
  for (const auto& it : events_per_machine) {
    uint64_t machine_id = it.first;

    std::map<uint64_t, std::pair<double, double>> time_to_max_resources;
    std::map<uint64_t, std::pair<double, double>> time_to_avg_resources;
    const auto& list_machine_events = it.second;

    // we use this to compute the average of the average cpu and memory over time
    double cum_avg_cpu = 0, cum_avg_mem = 0;
    uint64_t count = 0;
    
    // we keep max_cpu and max_mem values as we traverse over time
    double max_cpu = 0, max_mem = 0;
    double avg_cpu = 0, avg_mem = 0;
    // for each machine we traverse over time all events
    for (const auto& timestamp_events : list_machine_events) {
      uint64_t timestamp = timestamp_events.first;
      const auto& vector_events = timestamp_events.second;

      // for each timestamp we traverse all events in that timestamp
      // and we keep the maximum amount of resources utilized
      for (const auto& usage_event : vector_events) {
        assert (dynamic_cast<UsageEvent*>(usage_event.get()));
        UsageEvent* e = dynamic_cast<UsageEvent*>(usage_event.get());
        max_cpu += e->max_cpu;
        max_mem += e->max_mem;
        avg_cpu += e->avg_cpu;
        avg_mem += e->avg_mem;
      }

      //assert(timestamp > lambda_lifetime_secs * MICROSEC_IN_SECOND);
      time_to_max_resources[timestamp] = std::make_pair(max_cpu, max_mem);
      time_to_avg_resources[timestamp] = std::make_pair(avg_cpu, avg_mem);

      cum_avg_cpu += avg_cpu;
      cum_avg_mem += avg_mem;
      count++;
    }

    double before_avg_avg_cpu = (cum_avg_cpu / count);
    double before_avg_avg_mem = (cum_avg_mem / count);

    cum_avg_cpu = cum_avg_mem = count = 0;
    double machine_cpu_capacity = machine_to_capacity_cpu[machine_id];
    double machine_mem_capacity = machine_to_capacity_mem[machine_id];
    // traverse timestamps again
    for (auto it = list_machine_events.begin(); it != list_machine_events.end(); ++it) {
      uint64_t timestamp = it->first;
      //std::cout <<"at timestamp: "<< timestamp << std::endl;

      auto it2 = it;
      double max_cpu = 0, max_mem = 0;
      for (; it2 != list_machine_events.end() && 
          US_TO_SEC(it2->first - timestamp) < lambda_lifetime_secs; ++it2) {

        //max_cpu = std::max(max_cpu, time_to_max_resources[it2->first].first);
        //max_mem = std::max(max_mem, time_to_max_resources[it2->first].second);
        auto v = time_to_avg_resources[it2->first];
        max_cpu = std::max(max_cpu, v.first);
        max_mem = std::max(max_mem, v.second);
      }

      double hole_cpu = (machine_cpu_capacity - max_cpu);
      double hole_mem = (machine_mem_capacity - max_mem);
      //std::cout //  << "hole_cpu: "<< hole_cpu //  << "hole_mem: "<< hole_mem //  << std::endl;
      //std::cout << "num_lambdas: "<< 0 << "\n";
      //std::cout //  << " machine_id: " << machine_id //  << " max_cpu / machine_cpu_capacity: " << max_cpu / machine_cpu_capacity
      //  << " max_mem / machine_mem_capacity: " << max_mem / machine_mem_capacity //  << std::endl;
      if (max_cpu < machine_cpu_capacity
          && max_mem < machine_mem_capacity
          && hole_cpu >= lambda_cpu_size
          && hole_mem >= lambda_mem_size) {
        double num_lambdas = std::floor(std::min(hole_cpu / lambda_cpu_size, hole_mem / lambda_mem_size));
        assert(num_lambdas >= 0);
        
        //std::cout << "num_lambdas: "<< num_lambdas << "\n";
        
        auto it2 = it;
        for (; it2 != list_machine_events.end() && 
            US_TO_SEC(it2->first - timestamp) < lambda_lifetime_secs; ++it2) {

          auto iter_avg = time_to_avg_resources.find(it2->first);
          auto iter_max = time_to_max_resources.find(it2->first);

          assert(iter_avg != time_to_avg_resources.end());
          assert(iter_max != time_to_max_resources.end());

          iter_avg->second.first += num_lambdas * lambda_cpu_size;
          iter_avg->second.second += num_lambdas * lambda_mem_size;

          // may need to fix the max value
          iter_max->second.first = std::max(iter_max->second.first, iter_avg->second.first);
          iter_max->second.second = std::max(iter_max->second.second, iter_avg->second.second);
        }
      }

      auto v = time_to_avg_resources[timestamp];
      cum_avg_cpu += v.first;
      cum_avg_mem += v.second;
      count++;
    }
    std::cout
      << machine_id
      << " " << (cum_avg_cpu / count) / machine_cpu_capacity
      << " " << before_avg_avg_cpu / machine_cpu_capacity
      << " " << (cum_avg_mem / count) / machine_mem_capacity
      << " " << before_avg_avg_mem / machine_mem_capacity
      << "\n";
  }
}

#if 0
void compute_events_per_machine_simple_policy(
    int lambda_lifetime_secs,
    double lambda_cpu_size,
    double lambda_mem_size) {
  // we traverse all machines
  for (const auto& it : events_per_machine) {
    uint64_t machine_id = it.first;

    std::map<uint64_t, std::pair<double, double>> time_to_max_resources;
    std::map<uint64_t, std::pair<double, double>> time_to_avg_resources;
    //std::map<uint64_t, std::pair<double, double>> time_to_max_resources_prev;
    //std::map<uint64_t, std::pair<double, double>> time_to_avg_resources_prev;
    const auto& list_machine_events = it.second;

    // we use this to compute the average of the average cpu and memory over time
    double cum_avg_cpu = 0, cum_avg_mem = 0;
    uint64_t count = 0;
    
    // we keep max_cpu and max_mem values as we traverse over time
    double max_cpu = 0, max_mem = 0;
    double avg_cpu = 0, avg_mem = 0;
    double max_cpu_prev = 0, max_mem_prev = 0;
    double avg_cpu_prev = 0, avg_mem_prev = 0;
    // for each machine we traverse over time all events
    for (const auto& timestamp_events : list_machine_events) {
      uint64_t timestamp = timestamp_events.first;
      const auto& vector_events = timestamp_events.second;

      //max_cpu_prev = max_cpu;
      //max_mem_prev = mem_cpu;
      //avg_cpu_prev = avg_cpu;
      //avg_mem_prev = avg_mem;

      // for each timestamp we traverse all events in that timestamp
      // and we keep the maximum amount of resources utilized
      for (const auto& usage_event : vector_events) {
        assert (dynamic_cast<UsageEvent*>(usage_event));
        UsageEvent* e = dynamic_cast<UsageEvent*>(usage_event);
        max_cpu += e->max_cpu;
        max_mem += e->max_mem;
        avg_cpu += e->avg_cpu;
        avg_mem += e->avg_mem;
      }

      time_to_max_resources[timestamp] = std::make_pair(max_cpu, max_mem);
      time_to_avg_resources[timestamp] = std::make_pair(avg_cpu, avg_mem);
      //time_to_max_resources_prev[timestamp] = std::make_pair(max_cpu_prev, max_mem_prev);
      //time_to_avg_resources_prev[timestamp] = std::make_pair(avg_cpu_prev, avg_mem_prev);

      cum_avg_cpu += avg_cpu;
      cum_avg_mem += avg_mem;
      count++;
    }

    double before_avg_avg_cpu = (cum_avg_cpu / count);
    double before_avg_avg_mem = (cum_avg_mem / count);

    double machine_cpu_capacity = machine_to_capacity_cpu[machine_id];
    double machine_mem_capacity = machine_to_capacity_mem[machine_id];
    // traverse timestamps again
    for (auto it = list_machine_events.begin(); it != list_machine_events.end(); ++it) {
      uint64_t timestamp = it->first;
      //std::cout <<"at timestamp: "<< timestamp << std::endl;

      // we compute the max_cpu and mem in the seconds before
      auto it2 = it;
      double max_cpu1 = 0, max_mem1 = 0;
      for (; US_TO_SEC(timestamp - it2->first) < lambda_lifetime_secs; --it2) {

        //max_cpu = std::max(max_cpu, time_to_max_resources[it2->first].first);
        //max_mem = std::max(max_mem, time_to_max_resources[it2->first].second);
        auto v = time_to_avg_resources[it2->first];
        max_cpu1 = std::max(max_cpu, v.first);
        max_mem1 = std::max(max_mem, v.second);

        if (it2 == list_machine_events.begin())
          break;
      }
      // we compute the max_cpu and mem in the seconds after
      double max_cpu2 = 0, max_mem2 = 0;
      for (it2 = it; it2 != list_machine_events.end() && US_TO_SEC(it2->first - timestamp) < lambda_lifetime_secs; ++it2) {
        auto v = time_to_avg_resources[it2->first];
        max_cpu2 = std::max(max_cpu, v.first);
        max_mem2 = std::max(max_mem, v.second);
        if (it2 == list_machine_events.begin())
          break;
      }
      double hole_cpu1 = (machine_cpu_capacity - max_cpu1);
      double hole_mem1 = (machine_mem_capacity - max_mem1);
      double hole_cpu2 = (machine_cpu_capacity - max_cpu2);
      double hole_mem2 = (machine_mem_capacity - max_mem2);
      if (max_cpu1 < machine_cpu_capacity
          && max_cpu2 < machine_mem_capacity
          && max_mem1 < machine_mem_capacity
          && max_mem2 < machine_mem_capacity
          && hole_cpu1 >= lambda_cpu_size
          && hole_cpu2 >= lambda_cpu_size
          && hole_mem1 >= lambda_mem_size
          && hole_mem2 >= lambda_mem_size) {
        double num_lambdas = std::floor(
            std::min(
              hole_cpu1 / lambda_cpu_size,
              std::min(
              hole_cpu2 / lambda_cpu_size,
              std::min(
              hole_mem1 / lambda_mem_size,
              hole_mem2 / lambda_mem_size))));
        assert(num_lambdas >= 0);
        if (num_lambdas == 0)
          continue;
        
        auto it2 = it;
        for (; US_TO_SEC(timestamp - it2->first) < lambda_lifetime_secs; --it2) {

          auto iter_avg = time_to_avg_resources.find(it2->first);
          auto iter_max = time_to_max_resources.find(it2->first);

          assert(iter_avg != time_to_avg_resources.end());
          assert(iter_max != time_to_max_resources.end());

          iter_avg->second.first += num_lambdas * lambda_cpu_size;
          iter_avg->second.second += num_lambdas * lambda_mem_size;

          // may need to fix the max value
          iter_max->second.first = std::max(iter_max->second.first, iter_avg->second.first);
          iter_max->second.second = std::max(iter_max->second.second, iter_avg->second.second);
          if (it2 == list_machine_events.begin())
            break;
        }
      }
    }
    cum_avg_cpu = cum_avg_mem = count = 0;
    for (const auto& timestamp_events : list_machine_events) {
      uint64_t timestamp = timestamp_events.first;
      auto v = time_to_avg_resources[timestamp];
      cum_avg_cpu += v.first;
      cum_avg_mem += v.second;
      count++;
    }
    std::cout
      << machine_id
      << " " << (cum_avg_cpu / count) / machine_cpu_capacity
      << " " << before_avg_avg_cpu / machine_cpu_capacity
      << " " << (cum_avg_mem / count) / machine_mem_capacity
      << " " << before_avg_avg_mem / machine_mem_capacity
      << "\n";
  }
}
#endif

int main(int argc, char* argv[]) {
    clear_datastructures();

    std::cout << "Reading machine events" << std::endl;
    read_machine_events(
        "/data/joao/f8_data/table_machine_events.csv");
    std::cout << "Machine events done" << std::endl;
    
    std::cout << "Reading task usage events" << std::endl;
    read_usage_events_parallel(
        "/data/joao/f8_data/table_task_usage.csv_compressed");
    std::cout << "Reading task usage done" << std::endl;

    std::string line;

    //compute_events_per_machine(
    compute_events_per_machine(
        200, // seconds
        0.1, // cpu size,
        0.1); // mem size

    return 0;

    std::set<uint64_t> machines_active;
    double total_mem_used = 0;
    for (auto it = events_map.begin();
            it != events_map.end(); ++it) {
        uint64_t time = it->first;

        // update all the events
        for (std::vector<std::shared_ptr<Event>>::iterator ev = it->second.begin();
                ev != it->second.end(); ++ev) {
            Event* e = ev->get();

            uint64_t machine_id = e->machine_id;
            if (dynamic_cast<MachineEvent*>(e)) {
                MachineEvent* me = dynamic_cast<MachineEvent*>(e);

                if (me->event_type == "0") {//add
                    machine_to_capacity_mem[machine_id] = me->mem;
                    machine_to_capacity_cpu[machine_id] = me->cpu;

                    if (machine_exists[machine_id]) {
                        // machine was already here so we treat it has an update
                        // of machine capacity
                    } else {
                        machines_active.insert(machine_id);
                        //machine_to_allocated[machine_id] = std::make_pair(0.0, 0.0);
                        machine_to_mem_utilized[machine_id] = 0;
                    }
                    machine_exists[machine_id] = true;
                } else if (me->event_type == "1") { //remove
                    if (machine_exists[machine_id]) {
                        machines_active.erase(machine_id);
                    }
                    machine_exists[machine_id] = false;
                } else if (me->event_type == "2") { // update
                    machine_to_capacity_mem[machine_id] = me->mem;
                    machine_to_capacity_cpu[machine_id] = me->cpu;
                    machine_exists[machine_id] = true;
                } else {
                    throw std::runtime_error("Error");
                }
            } else if (dynamic_cast<UsageEvent*>(e)) {
                UsageEvent* ue = dynamic_cast<UsageEvent*>(e);
                machine_to_mem_utilized[machine_id] += ue->avg_mem;
                machine_to_max_mem_utilized[machine_id] += ue->max_mem;
                machine_to_max_cpu_utilized[machine_id] += ue->max_cpu;
                total_mem_used += ue->avg_mem;
            } else {
                throw std::runtime_error("Unknown event");
            }
        }
        // end of loop over all events with same timestamp
    }

    std::cout << "Printing output. # events: " << events_map.size() << std::endl;

    return 0;
}

