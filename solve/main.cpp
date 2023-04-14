#pragma GCC optimize(3)
#pragma GCC optimize("inline")

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

class Flow {
public:
    int id;
    int bandwidth;
    int coming_time;
    int occupied_time; // 在端口上的占用时间
    bool is_sent; // 是否已经发送
    int send_time; // 发送时间
    int send_port; // 发送端口
    int weight; // 用于排序的权重, 由带宽和占用时间决定
    int best_leave_time; // 最优离开时间=发送时间+占用时间
    int real_leave_time; // 实际离开时间
public:
    Flow(int id, int bandwidth, int coming_time, int occupied_time) {
        this->id = id;
        this->bandwidth = bandwidth;
        this->coming_time = coming_time;
        this->occupied_time = occupied_time;
        this->is_sent = false;
        this->send_time = -1;
        this->send_port = -1;
        this->weight = bandwidth * occupied_time;  // TODO: 优化权重
        this->best_leave_time = this->coming_time + this->occupied_time;
        this->real_leave_time = -1;
    }
};

class Port {

public:
    Port(int id, int bandwidth_capacity) {
        this->id = id;
        this->bandwidth_capacity = bandwidth_capacity;
        this->heights_capacity.insert(std::make_pair(0, bandwidth_capacity));
    }

    // 供flows_info优先队列使用的比较仿函数，按照剩余时间从小到大排序
    struct cmp {
        bool operator()(const std::pair<int, int> &a, const std::pair<int, int> &b) {
            return a.first > b.first;
        }
    };

public:
    int id;
    int bandwidth_capacity;
    std::map<int, int> heights_capacity; // key: 高度, value: 该高度的带宽容量
    std::map<int, std::vector<Flow *>> heights_flows; // key: 高度, value: 该高度上的flows, 储存了flow的指针
    // list of (remaining_time, bandwidth)
    std::list<std::pair<int, int>> occupies; // 储存了每个flow的剩余时间和带宽
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

//ports排序按照bandwidth_capacity将序构造优先队列的仿函数
struct ports_queue_cmp {
    bool operator()(const Port &a, const Port &b) {
        return a.bandwidth_capacity < b.bandwidth_capacity;
    }
};

void solve(std::vector<Flow> &flows, std::vector<Port> &ports, const std::string &data_path) {
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
    //ports排序按照bandwidth_capacity升序
//    std::sort(ports.begin(), ports.end(), [](const Port &a, const Port &b) {
//        return a.bandwidth_capacity < b.bandwidth_capacity;
//    });
    auto ports_num = ports.size();
    // ports排序按照bandwidth_capacity降序，采用优先队列构造成最大堆
    std::priority_queue<Port, std::vector<Port>, ports_queue_cmp> ports_queue;
    for (auto &port: ports) {
        ports_queue.push(port);
    }
    // 等待放置的流队列
    std::list<Flow *> flows_waiting_list;
    // 计时器
    int time = 0;
    int now_coming_time = 0;
    int last_coming_time = 0;
    for (auto &flow: flows) {
        now_coming_time = flow.coming_time;
        if (now_coming_time > last_coming_time) {
            // 当前时间大于上一个流到达时间，更新时间
            time++;
            last_coming_time = now_coming_time;
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
                    }
                }
                ports_queue.push(port);
            }
            // 先遍历等待队列，看是否有流可以放置
            for (auto it = flows_waiting_list.begin(); it != flows_waiting_list.end();) {
                // 从最小堆中取出当前带宽容量最大的端口
                Port port = ports_queue.top();
                ports_queue.pop();
                // 若本流带宽小于该端口带宽容量，放置本流
                if ((*it)->bandwidth <= port.bandwidth_capacity) {
                    // 更新flow的send_port
                    (*it)->send_port = port.id;
                    // 更新flow的send_time
                    (*it)->send_time = time;
                    // 写出安排结果
                    file << (*it)->id << "," << (*it)->send_port << "," << (*it)->send_time << std::endl;
                    // debug: print flow
//                    std::cout << "flow " << (*it)->id << " is sent: " << (*it)->send_port << "\tsend_time: "
//                              << (*it)->send_time << std::endl;
                    // 更新port的带宽容量
                    port.bandwidth_capacity -= (*it)->bandwidth;
                    // 更新port的occupies
                    port.occupies.emplace_back((*it)->occupied_time, (*it)->bandwidth);
                    // 从等待队列中删除该流
                    it = flows_waiting_list.erase(it);
                } else {
                    it++;
                }
                ports_queue.push(port);
            }
        }
        // 从最小堆中取出当前带宽容量最大的端口
        Port port = ports_queue.top();
        ports_queue.pop();
        // 若本流带宽小于该端口带宽容量，放置本流
        if (flow.bandwidth <= port.bandwidth_capacity) {
            // 更新flow的send_port
            flow.send_port = port.id;
            // 更新flow的send_time
            flow.send_time = time;
            // 写出安排结果
            file << flow.id << "," << flow.send_port << "," << flow.send_time << std::endl;
            // debug: print flow
//            std::cout << "flow " << flow.id << " is sent: " << flow.send_port << "\tsend_time: "
//                      << flow.send_time << std::endl;
            // 更新port的带宽容量
            port.bandwidth_capacity -= flow.bandwidth;
            // 更新port的occupies
            port.occupies.emplace_back(flow.occupied_time, flow.bandwidth);
        } else {
            // 若本流带宽大于该端口带宽容量，可知此时无法将本流放置任何端口，将本流加入等待下一时刻到来的队列中
            flows_waiting_list.push_back(&flow);
        }
        // 将该端口放回最小堆
        ports_queue.push(port);
    }
    // 读取flow结束，等待时间中的各流可视为同时到达
    while (!flows_waiting_list.empty()) {
        // 更新时间
        time++;
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
                }
            }
            ports_queue.push(port);
        }
        // 遍历等待队列，看是否有流可以放置
        for (auto it = flows_waiting_list.begin(); it != flows_waiting_list.end();) {
            // 从最小堆中取出当前带宽容量最大的端口
            Port port = ports_queue.top();
            ports_queue.pop();
            // 若本流带宽小于该端口带宽容量，放置本流
            if ((*it)->bandwidth <= port.bandwidth_capacity) {
                // 更新flow的send_port
                (*it)->send_port = port.id;
                // 更新flow的send_time
                (*it)->send_time = time;
                // 写出安排结果
                file << (*it)->id << "," << (*it)->send_port << "," << (*it)->send_time << std::endl;
                // debug: print flow
//                std::cout << "flow " << (*it)->id << " is sent: " << (*it)->send_port << "\tsend_time: "
//                          << (*it)->send_time << std::endl;
                // 更新port的带宽容量
                port.bandwidth_capacity -= (*it)->bandwidth;
                // 更新port的occupies
                port.occupies.emplace_back((*it)->occupied_time, (*it)->bandwidth);
                // 从等待队列中删除该流
                it = flows_waiting_list.erase(it);
            } else {
                it++;
            }
            ports_queue.push(port);
        }
    }
    file.close();
}

void write_result(const std::string &data_path, const std::vector<Flow> &flows) {
    std::ofstream file(data_path + "/result.txt");
    for (const auto &flow: flows) {
        int send_time = flow.send_time >= flow.coming_time ? flow.send_time : flow.coming_time;
        file << flow.id << "," << flow.send_port << "," << send_time << std::endl;
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
            // data_num号样本目录存在，处理数据
            std::vector<Flow> flows;
            std::vector<Port> ports;
            read_files(data_path, flows, ports);
            // 流调度
            solve(flows, ports, data_path);
            // 输出结果
//            write_result(data_path, flows);
            data_num++;
        } else {
            // 终止遍历
            break;
        }
    }
    return 0;
}
