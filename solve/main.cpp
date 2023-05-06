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

// 调度区最大容量
int MAX_POOL_SIZE;

class Flow {
public:
    int id;
    int bandwidth;
    int coming_time;
    int occupied_time; // 在端口上的占用时间
    int send_time; // 发送时间
    int send_port; // 发送端口
    int weight; // 用于排序的权重, 由带宽和占用时间决定
public:
    Flow(int id, int bandwidth, int coming_time, int occupied_time) {
        this->id = id;
        this->bandwidth = bandwidth;
        this->coming_time = coming_time;
        this->occupied_time = occupied_time;
        this->send_time = -1;
        this->send_port = -1;
        this->weight = occupied_time;
    }
};

class Port {
public:
    int id;
    // 端口的最大（初始）带宽
    int max_bandwidth;
    // 端口的当前空闲带宽
    int bandwidth_capacity;
    // list of (remaining_time, bandwidth)
    // 储存了端口中已发送的每个flow的剩余时间和带宽
    std::list<std::pair<int, int>> occupies;
    // 端口的排队区
    std::queue<Flow> wait_queue;

public:
    Port(int id, int bandwidth_capacity) {
        this->id = id;
        this->max_bandwidth = bandwidth_capacity;
        this->bandwidth_capacity = bandwidth_capacity;
    }
};

void read_files(const std::string &data_path, std::vector<Flow> &flows, std::vector<Port> &ports) {
    // 读取flows.txt
    std::ifstream file(data_path + "/flow.txt");
    std::string line;
    if (file.is_open()) {
        // 跳过首行
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
    }
    // 读取ports.txt
    file.open(data_path + "/port.txt");
    if (file.is_open()) {
        // 跳过首行
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
        file.close();
    }
}

//ports排序按照bandwidth_capacity升序
class ports_queue_cmp {
public:
    bool operator()(const Port &a, const Port &b) const {
        return a.bandwidth_capacity < b.bandwidth_capacity;
    }
};

// wait_queue的排序仿函数
class wait_queue_cmp {
public:
    bool operator()(const Flow &a, const Flow &b) const {
        return a.weight < b.weight;
    }
};

bool put_flow(Flow &flow, int time, std::multiset<Port, ports_queue_cmp> &ports_queue,
              std::multiset<Flow, wait_queue_cmp> &wait_queue,
              std::ofstream &file) {
    // 端口临时列表，暂存已经访问过了的端口
    std::vector<Port> temp_ports;
    // 遍历端口，尝试找到能放得下本流的端口
    while (!ports_queue.empty()) {
        Port port = *ports_queue.begin();
        ports_queue.erase(ports_queue.begin());
        // 放置本流当：1.本流带宽小于该端口带宽容量 2.调度区已满且本流带宽小于该端口最大容量
        if (flow.bandwidth <= port.bandwidth_capacity) {
            // 更新flow的send_port
            flow.send_port = port.id;
            // 更新flow的send_time
            flow.send_time = time;
            // 写出安排结果
            file << flow.id << "," << flow.send_port << "," << flow.send_time << std::endl;
            // 更新port的带宽容量
            port.bandwidth_capacity -= flow.bandwidth;
            // 更新port的occupies
            port.occupies.emplace_back(flow.occupied_time, flow.bandwidth);
            // 将该端口放入临时队列中
            temp_ports.emplace_back(port);
            // 跳出循环
            break;
        } else if (flow.bandwidth <= port.max_bandwidth && wait_queue.size() >= MAX_POOL_SIZE) {
            // 更新flow的send_port
            flow.send_port = port.id;
            // 更新flow的send_time
            flow.send_time = time;
            // 写出安排结果
            file << flow.id << "," << flow.send_port << "," << flow.send_time << std::endl;
            // 若本port的排队区流数量小于30，send_port的排队区加入本流；否则，该流在该端口被抛弃
            if (port.wait_queue.size() < 30)
                port.wait_queue.push(flow);
            // 将该端口放入临时队列中
            temp_ports.emplace_back(port);
            // 跳出循环
            break;
        } else {
            // 将该端口放入临时队列中
            temp_ports.emplace_back(port);
            // 查看下一个带宽容量更大的端口能否放下
            continue;
        }
    }
    // 将临时列表端口放回有序列表
    for (auto &temp_port : temp_ports) {
        ports_queue.insert(temp_port);
    }
    // 结束时，若flow的send_port仍为-1，说明没有找到能放得下本流的端口
    if (flow.send_port == -1) {
        // 若调度区尚未满，将本流放回等待队列，等待队列不变
        if (wait_queue.size() < MAX_POOL_SIZE) {
            wait_queue.insert(flow);
            return false;
        } else {
            // 若调度区已满，把本流抛弃
        }
    }
    return true;
}

// 遍历端口，更新带宽容量与排队区
void update_ports(std::multiset<Port, ports_queue_cmp> &ports_queue, bool &bandwidth_changed) {
    // 将ports_queue中的端口暂存到临时列表中
    std::vector<Port> temp_ports;
    for (auto &port: ports_queue) {
        temp_ports.emplace_back(port);
    }
    // 清空ports_queue
    ports_queue.clear();
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
            Flow first_flow = port.wait_queue.front();
            if (first_flow.bandwidth <= port.bandwidth_capacity) {
                // 更新port的带宽容量
                port.bandwidth_capacity -= first_flow.bandwidth;
                // 更新port的occupies
                port.occupies.emplace_back(first_flow.occupied_time, first_flow.bandwidth);
                port.wait_queue.pop();
            }
        }
        ports_queue.insert(port);
    }
}

void solve(std::vector<Flow> &flows, std::vector<Port> &ports, const std::string &data_path) {
    // 流丢弃端口，选取初始带宽最大的端口
    std::vector<int> throw_port_id_list;
    std::ofstream file(data_path + "/result.txt");
    // 计算所有流的平均带宽
    int total_bandwidth = 0;
    for (auto &flow: flows) {
        total_bandwidth += flow.bandwidth;
    }
    double average_bandwidth = (double) total_bandwidth / flows.size();
    // flows排序按照coming_time升序->occupied_time降序->bandwidth降序
    std::sort(flows.begin(), flows.end(), [](const Flow &a, const Flow &b) {
        if (a.coming_time == b.coming_time) {
            if (a.occupied_time == b.occupied_time) {
                return a.bandwidth > b.bandwidth;
            } else {
                return a.occupied_time < b.occupied_time;
            }
        } else {
            return a.coming_time < b.coming_time;
        }
    });
    // 测试用，看flow样本特征
//    write_sorted_flows(data_path, flows);
    // ports排序按照bandwidth_capacity升序
    std::multiset<Port, ports_queue_cmp> ports_queue;
    for (auto &port: ports) {
        ports_queue.insert(port);
    }
    // 等待放置的流双端队列, 每修改后
    // 该队列的大小即为当前调度区中流的数量
    std::multiset<Flow, wait_queue_cmp> wait_queue;
    // 计时器
    int time = 0;
    int last_coming_time = 0;
    // 调度区最大容量
    MAX_POOL_SIZE = int(ports_queue.size()) * 20 - 1;
    // 带宽是否发生变化
    bool bandwidth_changed = false;
    // 放置流是否成功
    bool put_success;
    for (auto &flow: flows) {
        wait_queue.insert(flow);
        if (flow.coming_time > last_coming_time) {
            // 当前流的到达时间大于上一个流到达时间，更新时间
            time= flow.coming_time;
            bandwidth_changed = false;
            for (int i=0;i<time-last_coming_time;i++) {
                update_ports(ports_queue, bandwidth_changed);
            }
            last_coming_time = flow.coming_time;
        }
        // 遍历ports_queue，找到所有排队区满的端口，将其放入throw_port_id_list中
        throw_port_id_list.clear();
        for (auto &port: ports_queue) {
            if (port.wait_queue.size() >= 30) {
                throw_port_id_list.push_back(port.id);
            }
        }
        // 若调度区已满，且有排队区满&最大带宽大于流宽的端口，把等待队列中流拿出来在此处抛弃
        if (wait_queue.size() >= MAX_POOL_SIZE && !throw_port_id_list.empty() &&
            wait_queue.begin()->bandwidth > average_bandwidth) { //
            Flow wait_flow = *wait_queue.begin();
            wait_queue.erase(wait_queue.begin());
            for (auto port_id: throw_port_id_list) {
                auto port = std::find_if(ports_queue.begin(), ports_queue.end(), [port_id](const Port &port) {
                    return port.id == port_id;
                });
                if (port->max_bandwidth >= wait_flow.bandwidth) {
                    // 更新flow的send_port
                    wait_flow.send_port = port_id;
                    // 更新flow的send_time
                    wait_flow.send_time = time;
                    // 写出安排结果
                    int send_time =
                            wait_flow.send_time > wait_flow.coming_time ? wait_flow.send_time : wait_flow.coming_time;
                    file << wait_flow.id << "," << wait_flow.send_port << "," << send_time << std::endl;
                    break;
                }
            }
            // 到此，若找不到能抛弃的端口，将其放回队列
            if (wait_flow.send_port == -1) {
                wait_queue.insert(wait_flow);
            }
        }
        // 看等待队列中的首num个流是否可以放置
        // 若首个流放置了，继续看等待队列中的首num个流是否可以放置
        int see_num = 1;
        while (!wait_queue.empty() && see_num) {
            Flow wait_flow = *wait_queue.begin();
            wait_queue.erase(wait_queue.begin());
            put_success = put_flow(wait_flow, time, ports_queue, wait_queue, file);
            if (put_success) {
                see_num = 1;
            } else {
                see_num--;
            }
        }
    }
    // 读取flow结束，等待时间中的各流可视为同时到达，此时wait_queue只有出没有入，不可能爆调度区
    while (!wait_queue.empty()) {
        // 更新时间
        time++;
        bandwidth_changed = false;
        update_ports(ports_queue, bandwidth_changed);
        // 看等待队列中的首num个流是否可以放置
        // 若首个流放置了，继续看等待队列中的首num个流是否可以放置
        int see_num = 1;
        while (!wait_queue.empty() && bandwidth_changed && see_num) {
            Flow wait_flow = *wait_queue.begin();
            wait_queue.erase(wait_queue.begin());
            put_success = put_flow(wait_flow, time, ports_queue, wait_queue, file);
            if (put_success) {
                see_num = 1;
            } else {
                see_num--;
            }
        }
    }
    file.close();
}

// 输出文件result不加第一行描述，不用排序，放在和输入文件同目录
int main() {
    int data_num = 0;
    int sum_time = 0;
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
            // 计算总时间
            sum_time += (end - start) ;
            data_num++;
        } else {
            // 终止遍历
            break;
        }
    }
    std::cout << "total time: " << (double)sum_time/ CLOCKS_PER_SEC << "s" << std::endl;
    return 0;
}

