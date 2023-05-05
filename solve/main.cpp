#pragma GCC optimize(3)
#pragma GCC optimize("inline")

#include "ctime"
#include <iostream>
#include <sstream>
#include <algorithm>
#include "string"
#include "vector"
#include "sys/stat.h"
#include "fstream"
#include "map"
#include "set"
#include "queue"
#include "list"

int MAX_POOL_SIZE;
// 流丢弃端口，选取初始带宽最大的端口
std::vector<int> THROW_PORT_ID_LIST;
// 流池当前大小
unsigned long long flow_pool_size;

class Flow {
public:
    int id;
    int bandwidth;
    int coming_time;
    int occupied_time; // 在端口上的占用时间
    int send_time; // 发送时间
    int send_port; // 发送端口
    int weight; // 用于排序的权重, 由带宽和占用时间决定
    int throw_weight; // 用于丢弃排序的权重, 由带宽和占用时间决定
    int best_leave_time; // 最优离开时间=发送时间+占用时间
    int real_leave_time; // 实际离开时间
public:
    Flow(int id, int bandwidth, int coming_time, int occupied_time) {
        this->id = id;
        this->bandwidth = bandwidth;
        this->coming_time = coming_time;
        this->occupied_time = occupied_time;
        this->send_time = -1;
        this->send_port = -1;
        this->weight = bandwidth / occupied_time;
        this->throw_weight = bandwidth + 120*occupied_time;
        this->best_leave_time = this->coming_time + this->occupied_time;
        this->real_leave_time = -1;
    }
};

class Port {
public:
    int id;
    int max_bandwidth;
    int bandwidth_capacity;
    int max_time; // port中已经占用的最大时间
    std::map<int, int> heights_capacity; // key: 高度, value: 该高度的带宽容量
    std::map<int, std::vector<Flow *>> heights_flows; // key: 高度, value: 该高度上的flows, 储存了flow的指针
    // list of (remaining_time, bandwidth)
    // 储存了端口中已发送的每个flow的剩余时间和带宽
    std::list<std::pair<int, int>> occupies;
    // 端口的排队区
    std::queue<Flow> wait_queue;

public:
    Port(int id, int bandwidth_capacity) {
        this->id = id;
        this->max_bandwidth=bandwidth_capacity;
        this->bandwidth_capacity = bandwidth_capacity;
        this->heights_capacity.insert(std::make_pair(0, bandwidth_capacity));
        this->max_time = 0;
    }
};

void read_files(const std::string &data_path, std::vector<Flow> &flows, std::vector<Port> &ports) {
    // 读取flows.txt
    std::ifstream file(data_path + "/flow.txt");
    // skip first line
    std::string line;
    std::getline(file, line);
    while (std::getline(file, line)) {
        // 读取一行
        std::string id, bandwidth, coming_time, process_time;
        std::stringstream ss(line);
        std::getline(ss, id, ',');
        std::getline(ss, bandwidth, ',');
        std::getline(ss, coming_time, ',');
        std::getline(ss, process_time);
        // 添加到flows
        flows.emplace_back(std::stoi(id), std::stoi(bandwidth), std::stoi(coming_time), std::stoi(process_time));
    }
    file.close();
    // 读取ports.txt
    file.open(data_path + "/port.txt");
    // skip first line
    std::getline(file, line);
    while (std::getline(file, line)) {
        // 读取一行
        std::string id, bandwidth_capacity;
        std::stringstream ss(line);
        std::getline(ss, id, ',');
        std::getline(ss, bandwidth_capacity);
        // 添加到ports
        ports.emplace_back(std::stoi(id), std::stoi(bandwidth_capacity));
    }
}

void write_sorted_flows(const std::string &data_path, const std::vector<Flow> &flows) {
    std::ofstream file(data_path + "/sorted_flows.csv");
    file << "id,bandwidth,coming_time,occupied_time,best_leave_time" << std::endl;
    for (const auto &flow: flows) {
        file << flow.id << "," << flow.bandwidth << "," << flow.coming_time << "," << flow.occupied_time << ","
             << flow.best_leave_time << std::endl;
    }
    file.close();
}

//ports排序按照bandwidth_capacity构造小顶堆优先队列的仿函数
struct ports_queue_cmp {
    bool operator()(const Port &a, const Port &b) {
        return a.bandwidth_capacity > b.bandwidth_capacity;
    }
};

// wait_queue的仿函数，按照weight升序
struct wait_queue_cmp {
    bool operator()(Flow &a, const Flow &b) {
        return a.weight > b.weight;
    }
};

// wait_throw_queue的仿函数，按照occupied_time降序->bandwidth降序排序
//struct wait_throw_queue_cmp {
//    bool operator()(Flow &a, const Flow &b) {
//        if (a.occupied_time==b.occupied_time) {
//            return a.bandwidth<b.bandwidth;
//        } else {
//            return a.occupied_time<b.occupied_time;
//        }
//    }
//};

// wait_throw_queue的仿函数，按照throw_weight升序排序
struct wait_throw_queue_cmp {
    bool operator()(Flow &a, const Flow &b) {
        return a.throw_weight > b.throw_weight;
    }
};


bool put_flow(Flow &flow, int time, std::priority_queue<Port, std::vector<Port>, ports_queue_cmp> &ports_queue,
              std::priority_queue<Flow, std::vector<Flow>, wait_queue_cmp> &wait_queue,
              std::ofstream &file) {
    // 遍历端口，尝试找到能放得下本流的端口
    std::queue<Port> temp_ports;
    while (!ports_queue.empty()) {
        Port port = ports_queue.top();
        ports_queue.pop();
        // 若本流未放置过，放置本流当：1.本流带宽小于该端口带宽容量                 //；2.调度区已满且本流带宽小于该端口最大容量且该端口排队区未满
        if (flow.send_port == -1 && flow.bandwidth <= port.bandwidth_capacity) {
            // 更新flow的send_port
            flow.send_port = port.id;
            // 更新flow的send_time
            flow.send_time = time;
            // 写出安排结果
            int send_time = flow.send_time>flow.coming_time?flow.send_time:flow.coming_time;
            file << flow.id << "," << flow.send_port << "," << send_time << std::endl;
            // debug: print flow
//            std::cout << "flow " << flow.id << " is sent: " << flow.send_port << "\tsend_time: "
//                      << flow.send_time << std::endl;
            // 更新port的带宽容量
            port.bandwidth_capacity -= flow.bandwidth;
            // 更新port的occupies
            port.occupies.emplace_back(flow.occupied_time, flow.bandwidth);
            // 将该端口放入临时队列中
            temp_ports.push(port);
            // 跳出循环
            break;}
        else if (flow.send_port == -1 && flow.bandwidth <= port.max_bandwidth && flow_pool_size>=MAX_POOL_SIZE) {
            // 更新flow的send_port
            flow.send_port = port.id;
            // 更新flow的send_time
            flow.send_time = time;
            // 写出安排结果
            int send_time = flow.send_time>flow.coming_time?flow.send_time:flow.coming_time;
            file << flow.id << "," << flow.send_port << "," << send_time << std::endl;
            // debug: print flow
//            std::cout << "flow " << flow.id << " is sent: " << flow.send_port << "\tsend_time: "
//                      << flow.send_time << std::endl;
            // 若本port的排队区流数量小于30，send_port的排队区加入本流；否则，该流在该端口被抛弃
            if (port.wait_queue.size() < 30)
                port.wait_queue.push(flow);
            // 将该端口放入临时队列中
            temp_ports.push(port);
            // 跳出循环
            break;
        }
        else{
            // 将该端口放入临时队列中
            temp_ports.push(port);
            // 查看下一个带宽容量更大的端口能否放下
            continue;
        }
    }
    // 将临时队列中的端口放回最小堆中
    while (!temp_ports.empty()) {
        ports_queue.push(temp_ports.front());
        temp_ports.pop();
    }
    // 结束时，若flow的send_port仍为-1，说明没有找到能放得下本流的端口
    if (flow.send_port == -1) {
        // 若调度区尚未满，将本流放回等待队列，等待队列不变
        if (wait_queue.size() < MAX_POOL_SIZE) {
            wait_queue.push(flow);
            return false;
        } else {
            // 若调度区已满，把本流抛弃
        }
    }
    return true;
}

void update_ports(std::priority_queue<Port, std::vector<Port>, ports_queue_cmp> &ports_queue, bool &bandwidth_changed) {
    // 遍历端口，更新带宽容量
    std::vector<Port> temp_ports;
    while (!ports_queue.empty()) {
        temp_ports.push_back(ports_queue.top());
        ports_queue.pop();
    }
    for (auto &port: temp_ports) {
        // 遍历port的occupies
        for (auto it = port.occupies.begin(); it != port.occupies.end();) {
            if (it->first > 0) {
                it->first--;
                it++;
            } else {
                port.bandwidth_capacity += it->second;
                it = port.occupies.erase(it);
                bandwidth_changed = true;
            }
        }
        // 若port排队区非空，且排队首元素带宽小于此时端口带宽容量，发出
        if (!port.wait_queue.empty()) {
            Flow first_flow=port.wait_queue.front();
            if (first_flow.bandwidth<=port.bandwidth_capacity) {
                // 更新port的带宽容量
                port.bandwidth_capacity -= first_flow.bandwidth;
                // 更新port的occupies
                port.occupies.emplace_back(first_flow.occupied_time, first_flow.bandwidth);
                port.wait_queue.pop();
            }
        }
        ports_queue.push(port);
    }
}

void solve(std::vector<Flow> &flows, std::vector<Port> &ports, const std::string &data_path) {
    THROW_PORT_ID_LIST.clear();
    std::ofstream file(data_path + "/result.txt");
    // flows排序按照coming_time升序->occupied_time降序->bandwidth降序
    std::sort(flows.begin(), flows.end(), [](const Flow &a, const Flow &b) {
        if (a.coming_time == b.coming_time) {
            if (a.occupied_time == b.occupied_time) {
                return a.bandwidth > b.bandwidth;
            } else {
                return a.occupied_time > b.occupied_time;
            }
        } else {
            return a.coming_time < b.coming_time;
        }
    });
    // 测试用，看flow样本特征
//    write_sorted_flows(data_path, flows);
    // 找到最大带宽端口（若干个），后续作为抛弃流的端口
    int max_bandwidth = 0;
    for (auto &port: ports) {
        if (port.max_bandwidth >= max_bandwidth) {
            max_bandwidth = port.max_bandwidth;
        }
    }
    for (auto &port: ports) {
        if (port.max_bandwidth == max_bandwidth) {
            THROW_PORT_ID_LIST.push_back(port.id);
        }
    }
    // ports排序按照bandwidth_capacity升序，采用优先队列构造成最小堆
    std::priority_queue<Port, std::vector<Port>, ports_queue_cmp> ports_queue;
    for (auto &port: ports) {
        ports_queue.push(port);
    }
    // 等待放置的流队列, 按照带宽从小到大排序
    // 该队列的大小即为当前调度区中流的数量
    std::priority_queue<Flow, std::vector<Flow>, wait_queue_cmp> wait_queue;
    // 发送用时较小的流组成的待抛弃队列，按照occupied_time降序->bandwidth降序排序
    std::priority_queue<Flow,std::vector<Flow>,wait_throw_queue_cmp> wait_throw_queue;
    // 计时器
    int time = 0;
    int now_coming_time;
    int last_coming_time = 0;
    // 调度区最大容量
    MAX_POOL_SIZE = int(ports_queue.size())*20-1;
    // 带宽是否发生变化
    bool bandwidth_changed = false;
    // 放置流是否成功
    bool put_success;
    for (auto &flow: flows) {
        put_success = true;
        now_coming_time = flow.coming_time;
        if (flow.occupied_time<=40) {
            wait_throw_queue.push(flow);
        } else {
            wait_queue.push(flow);
        }
        flow_pool_size = wait_queue.size()+wait_throw_queue.size();
        if (flow_pool_size>=MAX_POOL_SIZE && !wait_throw_queue.empty()) {
            // 若调度区已满，从待丢弃队列中抛弃流
            Flow throw_flow=wait_throw_queue.top();
            wait_throw_queue.pop();
            // 根据THROW_PORT_ID_LIST中的端口id，选择排队最短的那个端口
            int throw_port_id = THROW_PORT_ID_LIST[rand()%THROW_PORT_ID_LIST.size()];
            // 更新flow的send_port
            throw_flow.send_port = throw_port_id;
            // 更新flow的send_time
            throw_flow.send_time = time;
            // 写出安排结果
            int send_time = throw_flow.send_time>throw_flow.coming_time?throw_flow.send_time:throw_flow.coming_time;
            file << throw_flow.id << "," << throw_flow.send_port << "," << send_time << std::endl;
            // debug: print flow
//            std::cout << "flow " << flow.id << " is sent: " << flow.send_port << "\tsend_time: "
//                      << flow.send_time << std::endl;
            // 根据抛弃端口的id，找到抛弃端口的引用
            auto port = std::find_if(ports.begin(), ports.end(), [throw_port_id](const Port &port) {
                return port.id == throw_port_id;
            });
            // 若本port的排队区流数量小于30，send_port的排队区加入本流；否则，该流在该端口被抛弃
            if (port->wait_queue.size() < 30)
                port->wait_queue.push(flow);
        }
        if (now_coming_time > last_coming_time) {
            // 当前时间大于上一个流到达时间，更新时间
            time++;
            last_coming_time = now_coming_time;
            update_ports(ports_queue, bandwidth_changed);
            // 看等待队列中的首个流是否可以放置
            // 若首个流放置了，继续看等待队列中的首num个流是否可以放置 todo：改为若干个流？
            while (!wait_queue.empty() && bandwidth_changed && put_success) {
                Flow wait_flow = wait_queue.top();
                wait_queue.pop();
                put_success = put_flow(wait_flow, time, ports_queue, wait_queue, file);
            }
            bandwidth_changed = false;
        } else {
            // 看等待队列中的首个流是否可以放置
            // 若首个流放置了，继续看等待队列中的首num个流是否可以放置
            while (!wait_queue.empty() && put_success) {
                Flow wait_flow = wait_queue.top();
                wait_queue.pop();
                put_success = put_flow(wait_flow, time, ports_queue, wait_queue, file);
            }
        }
    }
    // 读取flow结束，等待时间中的各流可视为同时到达
    // 将wait_throw_queue中的流全部放入wait_queue中
    while (!wait_throw_queue.empty()) {
        wait_queue.push(wait_throw_queue.top());
        wait_throw_queue.pop();
    }
    while (!wait_queue.empty()) {
        // 更新时间
        time++;
        put_success = true;
        update_ports(ports_queue, bandwidth_changed);
        // 看等待队列中的首个流是否可以放置
        // 若首个流放置了，继续看等待队列中的首num个流是否可以放置
        while (!wait_queue.empty() && bandwidth_changed && put_success) {
            Flow wait_flow = wait_queue.top();
            wait_queue.pop();
            put_success = put_flow(wait_flow, time, ports_queue, wait_queue, file);
        }
        bandwidth_changed = false;
    }
    file.close();
}

// 输出文件result不加第一行描述，不用排序，放在和输入文件同目录
int main() {
    int data_num = 0;
    // 遍历../data文件夹下的输入文件夹
    std::string data_root = "../data";
    while (true) {
        std::string data_path = data_root + "/" + std::to_string(data_num);
        // 判断文件夹是否存在
        struct stat s{};
        if (stat(data_path.c_str(), &s) == 0 && (s.st_mode & S_IFDIR)) {
            // 计时开始
            clock_t start, end;
            start = clock();
            // data_num号样本目录存在，处理数据
            std::vector<Flow> flows;
            std::vector<Port> ports;
            read_files(data_path, flows, ports);
            // 流调度
            solve(flows, ports, data_path);
            // 计时结束
            end = clock();
            std::cout << "data " << data_num << " done in " << (double) (end - start) / CLOCKS_PER_SEC << "s"
                      << std::endl;
            data_num++;
        } else {
            // 终止遍历
            break;
        }
    }
    return 0;
}
