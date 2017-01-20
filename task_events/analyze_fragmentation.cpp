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

    std::cout << "Read file: " << file << " size: " << size << std::endl;

    fclose(fin);
    return size;
}

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

void Tokenize(uint64_t first, uint64_t last,
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
        mem(m)
    {}


    double cpu;
    double mem;
};

struct TaskEvent : public Event {
    TaskEvent(const std::string& event_type,
            uint64_t mid,
            const std::string& tid,
            double c,
            double m) :
        Event(event_type, mid),
        task_id(tid),
        cpu(c),
        mem(m)
    {}

    std::string task_id;
    double cpu;
    double mem;
};

// maps time to list of events
std::map<uint64_t, std::vector<Event*>> events_map;
// maps machine id to amount of CPU/mem allocated on that machine
std::map<uint64_t, std::pair<double,double>> machine_to_allocated;
// maps machine id to amount of CPU/mem available in that machine
std::map<uint64_t, std::pair<double,double>> machine_to_capacity;
// maps task id to amount of CPU/Mem allocated to that task
std::map<std::string, std::pair<double,double>> task_to_allocated;
// list of machine ids of machines that are fragmented
std::set<uint64_t> fragmented_machines;
uint64_t updated_while_running = 0;


bool is_full_mem(uint64_t machine_id) {
    double mem_allocated = machine_to_allocated[machine_id].second;
    double mem_capacity = machine_to_capacity[machine_id].second;
    return mem_allocated >= mem_capacity;
}

bool is_full_cpu(uint64_t machine_id) {
    double cpu_allocated = machine_to_allocated[machine_id].first;
    double cpu_capacity = machine_to_capacity[machine_id].first;

    return cpu_allocated >= cpu_capacity;
}

bool is_fragmented(uint64_t machine_id) {
    return (is_full_cpu(machine_id) && !is_full_mem(machine_id)) ||
        (!is_full_cpu(machine_id) && is_full_mem(machine_id));
}

uint64_t machine_counter = 0;
std::map<std::string, uint64_t> machineid_to_uint;

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

        uint64_t machine_uint = machine_counter++;
        machineid_to_uint[machine_id] = machine_uint;

        events_map[time].push_back(new MachineEvent(event_type, machine_uint, cpu, mem));
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        puts("./analyze_allocated_resources <file>");
        return -1;
    }
    std::cout << "Reading machine events" << std::endl;
    read_machine_events("../machine_events/table_machine_events.csv");
    std::cout << "Machine events done" << std::endl;

    std::string line;
    int l = 0;

    uint64_t size = read_file(data, argv[1]);
    uint64_t first = 0, last = 0;

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
        //std::string task_unique_id = machine_id + "-" + job_id + "-" + task_id;
        std::string event_type = tokens[5];
        double cpu_double = to_T<double>(tokens[9]);
        double mem_double = to_T<double>(tokens[10]);

        if (cpu_double < 0 || cpu_double > 1 ||
                mem_double < 0 || mem_double > 1)
            continue;

        if (machineid_to_uint.find(machine_id) == machineid_to_uint.end())
            continue;

        auto event = new TaskEvent(event_type,
                machineid_to_uint[machine_id],
                task_unique_id,
                cpu_double,
                mem_double);
        
        events_map[time].push_back(event);

        // task being scheduled in machine
        /*
        if (event_type == "1" ||
                event_type == "7" ||
                event_type == "8") {
        
            if (cpu_double > 1 || cpu_double < 0 || mem_double < 0 || mem_double > 1)
                continue;

            // task is already allocated, so remove previous allocation
            if (task_to_cpu.find(task_unique_id) != task_to_cpu.end()) {
                if (task_to_mem.find(task_unique_id) == task_to_mem.end())
                    throw std::runtime_error("Error");

                // already exist fix that
                std::string machine_id = task_to_machine[task_unique_id];

                if (machine_to_resources.find(machine_id) == machine_to_resources.end() ||
                        task_to_machine.find(task_unique_id) == task_to_machine.end())
                    throw std::runtime_error("Error here");

                machine_to_resources[machine_id].first -= task_to_cpu[task_unique_id];
                machine_to_resources[machine_id].second -= task_to_mem[task_unique_id];
            }

            task_to_machine[task_unique_id] = machine_id;
            task_to_cpu[task_unique_id] = cpu_double;
            task_to_mem[task_unique_id] = mem_double;
            machine_to_resources

            total_cpu += cpu_double;
            total_mem += mem_double;

            events.push_back(std::make_pair(timestamp,
                        std::make_pair(total_cpu, total_mem)));
        } else if (event_type == "2" ||
                event_type == "3" ||
                event_type == "4" ||
                event_type == "5" ||
                event_type == "6") {

            if (task_to_cpu.find(task_unique_id) != task_to_cpu.end()) {
                if (task_to_mem.find(task_unique_id) == task_to_mem.end())
                    throw std::runtime_error("Error2");

                total_cpu -= task_to_cpu[task_unique_id];
                task_to_cpu.erase(task_to_cpu.find(task_unique_id));

                total_mem -= task_to_mem[task_unique_id];
                task_to_mem.erase(task_to_mem.find(task_unique_id));
            }
        }*/

        l++;
        if (l % (MILLION / 10) == 0)
            std::cout << "Line: " << l << std::endl;
    }

    for (auto it = events_map.begin();
            it != events_map.end(); ++it) {
        uint64_t time = it->first;

        double fragmented_cpu = 0;
        double fragmented_mem = 0;

        // accumulate all the wasted fragmented resources and print them
        for (const auto& machine_id : fragmented_machines) {
            if (!is_fragmented(machine_id)) {
                std::cout << "Machine: " << machine_id << std::endl;
                std::cout << "cpu capacity: " << machine_to_capacity[machine_id].first << std::endl;
                std::cout << "mem capacity: " << machine_to_capacity[machine_id].second << std::endl;
                std::cout << "cpu allocated: " << machine_to_allocated[machine_id].first << std::endl;
                std::cout << "mem allocated: " << machine_to_allocated[machine_id].second << std::endl;
                throw std::runtime_error("Machine is not fragmented");
            }

            if (is_full_cpu(machine_id)) {
                double mem_allocated = machine_to_allocated[machine_id].second;
                double mem_capacity = machine_to_capacity[machine_id].second;
                double wasted_mem = mem_capacity - mem_allocated;
                assert(wasted_mem > 0);
                fragmented_mem += wasted_mem;
            } else {
                double cpu_allocated = machine_to_allocated[machine_id].first;
                double cpu_capacity = machine_to_capacity[machine_id].first;
                double wasted_cpu = cpu_capacity - cpu_allocated;
                assert(wasted_cpu > 0);
                fragmented_cpu += wasted_cpu;
            }
        }
        std::cout << time << " " << fragmented_cpu << " " << fragmented_mem << "\n";


        // update all the events
        for (std::vector<Event*>::iterator ev = it->second.begin();
                ev != it->second.end(); ++ev) {
            Event* e = *ev;
                
            std::cout << "processing event" << std::endl;

            if (dynamic_cast<MachineEvent*>(e)) {
                //std::cout << "machine event" << std::endl;
                MachineEvent* me = dynamic_cast<MachineEvent*>(e);
                uint64_t machine_id = me->machine_id;

                if (me->event_type == "0") {//add
             //       std::cout << "adding machine" << std::endl;
                        machine_to_capacity[machine_id] = std::make_pair(me->cpu, me->mem);

                        //auto ma = machine_to_allocated.find(machine_id);
                        if (machine_to_allocated.find(machine_id) != machine_to_allocated.end()) {
                            // machine was already here so we treat it has an update
                            //throw std::runtime_error("Machine added when it was already there");
                            if (is_fragmented(machine_id))
                                fragmented_machines.insert(machine_id);
                            if (fragmented_machines.find(machine_id) != fragmented_machines.end() &&
                                    !is_fragmented(machine_id)) {
                                fragmented_machines.erase(machine_id); // remove from fragmented machines
                            }
                        }
                        machine_to_allocated[machine_id] = std::make_pair(0.0, 0.0);
                } else if (me->event_type == "1") { //remove
               //     std::cout << "removing machine" << std::endl;

                    auto mc = machine_to_capacity.find(machine_id);
                    if (mc != machine_to_capacity.end()) {
                        //machine_to_capacity.erase(machine_to_capacity.find(machine_id));
                        machine_to_capacity.erase(mc);
                        machine_to_allocated.erase(machine_to_allocated.find(machine_id));
                    }
                    if (fragmented_machines.find(machine_id) != fragmented_machines.end())
                        fragmented_machines.erase(machine_id); // remove from fragmented machines
                } else if (me->event_type == "2") { // update
                //    std::cout << "updating machine" << std::endl;
                    auto mc = machine_to_capacity.find(machine_id);
                    mc->second = std::make_pair(me->cpu, me->mem);
                    //machine_to_capacity[machine_id].first = me->cpu;
                    //machine_to_capacity[machine_id].second = me->mem;
                    if (is_fragmented(machine_id))
                        fragmented_machines.insert(machine_id);
                    if (fragmented_machines.find(machine_id) != fragmented_machines.end() &&
                            !is_fragmented(machine_id)) {
                        fragmented_machines.erase(machine_id); // remove from fragmented machines
                    }
                } else {
                    throw std::runtime_error("Error");
                }
            } else {
                //std::cout << "task event" << std::endl;
                TaskEvent* te = dynamic_cast<TaskEvent*>(e);
                uint64_t machine_id = te->machine_id;
                const std::string& task_id = te->task_id;
                if (te->event_type == "1") { // scheduled
                    std::cout << "Adding task " << task_id << std::endl;
                    if (task_to_allocated.find(task_id) != task_to_allocated.end()) {
                        throw std::runtime_error("Task already exists");
                    }

                    auto& ta = task_to_allocated[task_id];
                    ta.first = te->cpu;
                    ta.second = te->mem;

                    auto& ma = machine_to_allocated[machine_id];
                    ma.first += te->cpu;
                    ma.second += te->mem;

                    if (is_fragmented(machine_id))
                        fragmented_machines.insert(machine_id);
                    if (fragmented_machines.find(machine_id) != fragmented_machines.end() &&
                            !is_fragmented(machine_id)) {
                        fragmented_machines.erase(fragmented_machines.find(machine_id));
                    }
                } else if (te->event_type == "8") {// updated while running
                    updated_while_running++;
                } else if (te->event_type == "2" ||
                        (te->event_type == "3") ||
                        (te->event_type == "4") ||
                        (te->event_type == "5") ||
                        (te->event_type == "6")) {
                    
                    std::cout << "Removing task " << task_id << std::endl;

                    auto it = task_to_allocated.find(task_id);
                    if (it != task_to_allocated.end()) {
                        auto& ma = machine_to_allocated[machine_id];
                        ma.first -= it->second.first;
                        ma.second -= it->second.second;
                        task_to_allocated.erase(it);

                        if (fragmented_machines.find(machine_id) != fragmented_machines.end() &&
                                !is_fragmented(machine_id)) {
                            fragmented_machines.erase(fragmented_machines.find(machine_id));
                        }
                    }
                }
            }
        }
    }

    std::cout << "Printing output. # events: " << events_map.size() << std::endl;
    std::cout << "updated while running: " << updated_while_running << std::endl;

    return 0;
}

