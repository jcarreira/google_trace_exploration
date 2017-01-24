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
            double c,
            double m) :
        Event(event_type, mid),
        cpu(c),
        mem(m) {}

    double cpu;
    double mem;
};

struct TaskEvent : public Event {
    TaskEvent(const std::string& event_type,
            uint64_t mid,
            uint64_t tid,
            double c,
            double m) :
        Event(event_type, mid),
        task_id(tid),
        cpu(c),
        mem(m) {}

    uint64_t task_id;
    double cpu;
    double mem;
};

#define MILLION (1000000UL)
#define N_MACHINES (40000)
#define N_TASKS (30 * MILLION)
// maps time to list of events
std::map<uint64_t, std::vector<Event*>> events_map;
// maps machine id to amount of CPU/mem allocated on that machine
bool machine_exists[N_MACHINES];
std::pair<double,double> machine_to_allocated[N_MACHINES];
double machine_to_capacity[N_MACHINES];

double machine_to_mem_utilized[N_MACHINES];
double machine_to_benefits[N_MACHINES];
// maps task id to amount of CPU/Mem allocated to that task
bool task_exists[N_MACHINES];
std::pair<double,double> task_to_allocated[N_TASKS];

static double MEM_LOCAL_FRACTION = 0.9;
static double MEM_REMOTE_FRACTION = 1 - MEM_LOCAL_FRACTION;

uint64_t machine_counter = 0;
std::map<std::string, uint64_t> machineid_to_uint;
uint64_t task_counter = 0;
std::map<std::string, uint64_t> taskid_to_uint;

void read_usage_events(const std::string& file) {
    std::ifstream fin(file);
    std::string line;
    uint64_t counter = 0;
    while (std::getline(fin, line)) {
        std::vector<std::string> tokens;
        Tokenize(line, tokens, ",");

        assert(tokens.size() == 5);
        
        uint64_t start_time = to_T<double>(tokens[0]);
        uint64_t end_time = to_T<double>(tokens[1]);
        std::string machine_id = tokens[2];
        double cpu = to_T<double>(tokens[3]);
        double mem = to_T<double>(tokens[4]);

        if (machine_id == "")
            continue;

        // machineid not found
        if (machineid_to_uint.find(machine_id) == machineid_to_uint.end())
            continue;

        uint64_t machine_uint = machineid_to_uint[machine_id];

        //std::cout << "Creating usage event: "
        //    << " machine_id: " << machine_uint
        //    << "\n";

        events_map[start_time].push_back(new UsageEvent("", machine_uint, cpu, mem));
        events_map[end_time].push_back(new UsageEvent("", machine_uint, -cpu, -mem));

        if (counter % 100000 == 0) {
            std::cout << "counter: " << counter << "\n";
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
            << " machine_id: " << machine_uint
            << "\n";

        events_map[time].push_back(new MachineEvent(event_type, machine_uint, cpu, mem));
    }
}

void read_task_events(const std::string& file) {
    uint64_t size = read_file(data, file);
    uint64_t first = 0, last = 0;
    uint64_t l = 0;

    std::cout << "Starting analysis" << std::endl;

    // find lines
    while (1) {
        if (last >= size || !data[last])
            break;

        // update left pointer
        first = last;
        while (last < size && data[last] != '\n' && data[last])
            last++;

        last++; // jump the newline. if its null we hope the next is null too
        
        std::vector<std::string> tokens;
        Tokenize(first, last, tokens, ",");

        if (tokens.size() != 13)
            throw std::runtime_error("Wrong number of fields");

        uint64_t time = to_T<uint64_t>(tokens[0]);
        std::string job_id = tokens[2];
        std::string task_id = tokens[3];
        std::string machine_id = tokens[4];
        std::string task_unique_id = job_id + "-" + task_id;

        uint64_t task_uint_id = 0;
        if (taskid_to_uint.find(task_unique_id) != taskid_to_uint.end()) {
            task_uint_id = taskid_to_uint[task_unique_id];
        } else {
            task_uint_id = task_counter++;
            taskid_to_uint[task_unique_id] = task_uint_id;
        }


        std::string event_type = tokens[5];
        double cpu_double = to_T<double>(tokens[9]);
        double mem_double = to_T<double>(tokens[10]);

        if (cpu_double < 0 || cpu_double > 1 ||
                mem_double < 0 || mem_double > 1)
            continue;

        if (machine_id == "" && event_type == "1")
            continue;
        
        if (event_type == "7" || event_type == "0")
            continue;

        if (machineid_to_uint.find(machine_id) == machineid_to_uint.end()) {
            continue;
        }

        auto event = new TaskEvent(event_type,
                machineid_to_uint[machine_id],
                task_uint_id,
                cpu_double,
                mem_double);
        
        events_map[time].push_back(event);

        l++;
        if (l % 100000 == 0)
            std::cerr << "Line: " << l << "\n";
    }
}

void clear_datastructures() {
    memset(machine_exists, 0, sizeof(machine_exists));
    memset(task_exists, 0, sizeof(task_exists));
    memset(machine_to_mem_utilized, 0, sizeof(machine_to_mem_utilized));
    memset(machine_to_benefits, 0, sizeof(machine_to_benefits));
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        puts("./analyze_allocated_resources <local_fraction>");
        return -1;
    }

    double local_fraction = to_T<double>(argv[1]);
    MEM_LOCAL_FRACTION = local_fraction;
    MEM_REMOTE_FRACTION = 1 - local_fraction;

    clear_datastructures();

    std::cout << "Reading machine events" << std::endl;
    read_machine_events("../machine_events/table_machine_events.csv");
    std::cout << "Machine events done" << std::endl;
    
    std::cout << "Reading task usage events" << std::endl;
    read_usage_events("../task_usage/table_task_usage.csv_compressed_benefits");
    std::cout << "Reading task usage done" << std::endl;

    //std::cout << "Reading task events" << std::endl;
    //read_task_events("table_task_events.csv_test");
    //std::cout << "Reading task events done" << std::endl;

    std::string line;

    std::set<uint64_t> machines_changed;
    double total_benefit_mem = 0;
    double total_mem_used = 0;

    for (auto it = events_map.begin();
            it != events_map.end(); ++it) {
        uint64_t time = it->first;


        // accumulate all the benefit
        //XXX fix this
        for (const auto& machine_id : machines_changed) {

            if (machine_id >= N_MACHINES)
                throw std::runtime_error("Too big machine id");

            total_benefit_mem -= machine_to_benefits[machine_id];

            //std::cout << "updating machine: " << machine_id 
            //    << " current utilized: " << machine_to_mem_utilized[machine_id] 
            //    << " current benefit: " << machine_to_benefits[machine_id]
            //    << "\n";
            if (machine_exists[machine_id]) {
                double benefit = 1.0 - std::max(MEM_LOCAL_FRACTION, machine_to_mem_utilized[machine_id]);
                //std::cout << " machine: " << machine_id 
                //    << " benefit: " << benefit
                //    << " MEM_LOCAL_FRACTION: " << MEM_LOCAL_FRACTION
                //    << "\n";

                double normalized_benefit = benefit * machine_to_capacity[machine_id];
                machine_to_benefits[machine_id] = normalized_benefit;
                total_benefit_mem += normalized_benefit;
            } else {
                machine_to_benefits[machine_id] = 0;
                machine_to_mem_utilized[machine_id] = 0;
            }
        }

        machines_changed.clear();

        printf("%lu %lf %lf\n", time, total_benefit_mem, total_mem_used);

        // update all the events
        for (std::vector<Event*>::iterator ev = it->second.begin();
                ev != it->second.end(); ++ev) {
            Event* e = *ev;

            uint64_t machine_id = e->machine_id;
            if (dynamic_cast<MachineEvent*>(e)) {
                MachineEvent* me = dynamic_cast<MachineEvent*>(e);

                if (me->event_type == "0") {//add
                    //std::cout << "Adding machine: " << machine_id 
                    //    << "\n";

                    machine_to_capacity[machine_id] = me->mem;

                    if (machine_exists[machine_id]) {
                        // machine was already here so we treat it has an update
                        // of machine capacity
                    } else {
                        machine_to_allocated[machine_id] = std::make_pair(0.0, 0.0);
                        machine_to_mem_utilized[machine_id] = 0;
                    }
                    machine_exists[machine_id] = true;
                } else if (me->event_type == "1") { //remove
                    //std::cout << "Removing machine: " << machine_id 
                    //    << "\n";
                    
                    if (!machine_exists[machine_id]) {
                    //    std::cout << "machine machine: " << machine_id  
                    //        << " was never added\n";
                    }
                    machine_exists[machine_id] = false;
                } else if (me->event_type == "2") { // update
                    //std::cout << "Updating machine: " << machine_id 
                    //    << "\n";
                    machine_to_capacity[machine_id] = me->mem;
                    machine_exists[machine_id] = true;
                } else {
                    throw std::runtime_error("Error");
                }
            //} else if (dynamic_cast<TaskEvent*>(e)) {
            //    TaskEvent* te = dynamic_cast<TaskEvent*>(e);
            //    uint64_t task_id = te->task_id;
            //    if (te->event_type == "1") { // scheduled
            //        if (task_exists[task_id]) {
            //            auto& ma = machine_to_allocated[machine_id];
            //            ma.first -= task_to_allocated[task_id].first;
            //            ma.second -= task_to_allocated[task_id].second;
            //            task_exists[task_id] = false;
            //        }
            //        auto& ta = task_to_allocated[task_id];
            //        ta.first = te->cpu;
            //        ta.second = te->mem;
            //        task_exists[task_id] = true;

            //        auto& ma = machine_to_allocated[machine_id];
            //        ma.first += te->cpu;
            //        ma.second += te->mem;
            //    } else if (te->event_type == "8") {// updated while running
            //        //auto tta = task_to_allocated.find(task_id);
            //        if (task_exists[task_id]) {
            //            // Task does not exist, we ignore
            //        } else {
            //            auto& ma = machine_to_allocated[machine_id];
            //            ma.first -= task_to_allocated[task_id].first;
            //            ma.second -= task_to_allocated[task_id].second;
            //            //task_to_allocated.erase(tta);

            //            auto& ta = task_to_allocated[task_id];
            //            ta.first = te->cpu;
            //            ta.second = te->mem;
            //            ma.first += te->cpu;
            //            ma.second += te->mem;
            //            task_exists[task_id] = true;
            //        }
            //    } else if (te->event_type == "2" ||
            //            (te->event_type == "3") ||
            //            (te->event_type == "4") ||
            //            (te->event_type == "5") ||
            //            (te->event_type == "6")) {
            //        //auto it = task_to_allocated.find(task_id);
            //        if (task_exists[task_id]) {
            //            auto& ma = machine_to_allocated[machine_id];
            //            ma.first -= task_to_allocated[task_id].first;
            //            ma.second -= task_to_allocated[task_id].second;
            //            task_exists[task_id] = false;
            //        }
            //    }
            } else if (dynamic_cast<UsageEvent*>(e)) {
                UsageEvent* ue = dynamic_cast<UsageEvent*>(e);
                //std::cout << "Usage event machine id: " << machine_id 
                //    << " Memory: " << ue->mem
                //    << " machine_to_mem_utilized: " << machine_to_mem_utilized[machine_id]
                //    << "\n";
                machine_to_mem_utilized[machine_id] += ue->mem;
                total_mem_used += ue->mem;
            } else {
                throw std::runtime_error("Unknown event");
            }
            machines_changed.insert(machine_id);
        }
    }

    std::cout << "Printing output. # events: " << events_map.size() << std::endl;

    return 0;
}

