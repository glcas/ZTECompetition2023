//#pragma GCC optimize(3)
//#pragma GCC optimize("inline")

#include <iostream>
#include <sstream>
#include <algorithm>
#include "string"
#include "vector"
#include "sys/stat.h"
#include "fstream"
#include "map"
#include "set"

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
    int id;
    int bandwidth_capacity;
    std::map<int, int> heights_capacity; // key: 高度, value: 该高度的带宽容量
    std::map<int, std::vector<Flow *>> heights_flows; // key: 高度, value: 该高度上的flows, 储存了flow的指针

public:
    Port(int id, int bandwidth_capacity) {
        this->id = id;
        this->bandwidth_capacity = bandwidth_capacity;
        this->heights_capacity.insert(std::make_pair(0, bandwidth_capacity));
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

/* 放置flow的平台必须要求容量大于flow的带宽 */
void pass_flow(Port &port, Flow &flow, int send_time) {
    // 更新flow
    flow.is_sent = true;
    flow.send_time = send_time;
    flow.send_port = port.id;
    flow.real_leave_time =
            send_time < flow.coming_time ? flow.coming_time + flow.occupied_time : send_time + flow.occupied_time;
    // 更新port
    // 基座带宽容量减少
    port.heights_capacity[send_time] -= flow.bandwidth;
    port.heights_flows[send_time].push_back(&flow);
    // 顶部搭建带宽容量增加
    if (port.heights_capacity.find(flow.real_leave_time) == port.heights_capacity.end()) {
        port.heights_capacity[flow.real_leave_time] = flow.bandwidth;
    } else {
        port.heights_capacity[flow.real_leave_time] += flow.bandwidth;
    }
}

void put_flow(Flow &flow, std::vector<Port> &ports, std::vector<Flow> &flows) {
    std::vector<std::pair<Port *, std::pair<int, int>>> port_best_capacities;  // (port_id, (height, capacity))，height不超过flow.coming_time
    //遍历端口
    for (auto &port: ports) {
        if (port.heights_capacity.find(flow.coming_time) == port.heights_capacity.end()) {
            // 该端口内尚未堆叠流的到达时平台
            // 逐高度向上寻找，直到找到一个高度，该高度的带宽容量足够，添加进后续优选列表
            for (auto it = port.heights_capacity.begin(); it != port.heights_capacity.end(); it++) {
                if (it->first <= flow.coming_time && it->second >= flow.bandwidth) {
                    port_best_capacities.emplace_back(&port, std::make_pair(it->first, it->second));
                    break;
                }
            }
            // 执行到这，说明该端口内尚未堆叠流的到达时平台，且该端口内没有高度的带宽容量足够，添加距流到达时最近高度的带宽容量(后续必然会进入回溯替换or非最佳放置的流程)
            // 该方案优先选择尽量压缩纵向空隙带来的损失，尚未考虑横向超出部分带来空隙的损失
            for (auto it = port.heights_capacity.rbegin(); it != port.heights_capacity.rend(); it++) {
                if (it->first <= flow.coming_time) {
                    port_best_capacities.emplace_back(&port, std::make_pair(it->first, it->second));
                    break;
                }
            }
        } else {
            // 该端口内堆叠有流的到达时平台
            port_best_capacities.emplace_back(&port, std::make_pair(flow.coming_time,
                                                                    port.heights_capacity[flow.coming_time]));
        }
    }
    // 按照height降序->capacity降序排序
    std::sort(port_best_capacities.begin(), port_best_capacities.end(),
              [](const std::pair<Port *, std::pair<int, int>> &a,
                 const std::pair<Port *, std::pair<int, int>> &b) {
                  if (a.second.first != b.second.first) {
                      return a.second.first > b.second.first;
                  } else {
                      return a.second.second > b.second.second;
                  }
              });
    for (auto pair: port_best_capacities) { // for each port
        Port &port = *pair.first;
        int height = pair.second.first;
        int height_capacity = pair.second.second;
        if (height_capacity >= flow.bandwidth) {
            pass_flow(port, flow, height);
            return;
        } else {
            // 该端口该高度平台带宽不满足流的带宽需求，尝试在该高度为其腾出位置（挪开一个先前放置的流）
            // 功能：查找是否能替换该平台上的其他已放置的流，使得该平台上的流的带宽容量足够放置本流，且剩余带宽容量减小
            std::vector<Flow *> &flows_on_coming_time = port.heights_flows[flow.coming_time];
            std::sort(flows_on_coming_time.begin(), flows_on_coming_time.end(),
                      [](const Flow *a, const Flow *b) {
                          return a->bandwidth < b->bandwidth;
                      });
            int now_capacity = port.heights_capacity[flow.coming_time];
            for (auto flow_on_coming_time: flows_on_coming_time) {
                int new_capacity = now_capacity + flow_on_coming_time->bandwidth - flow.bandwidth;
                if (new_capacity > 0) {
                    if (new_capacity < now_capacity && flow_on_coming_time->coming_time >= flow.coming_time) {
                        // 该平台上的剔除原放置的一个流后余下带宽容量足够放置本流，且剩余带宽容量减小
                        // 从该平台上的流的原位置移除
                        port.heights_capacity[flow_on_coming_time->send_time] += flow_on_coming_time->bandwidth;
                        auto &now_flows = port.heights_flows.find(flow_on_coming_time->send_time)->second;
                        now_flows.erase(
                                std::find(now_flows.begin(),
                                          now_flows.end(),
                                          flow_on_coming_time));
                        // 原流顶部搭建平台容量减小
                        if (port.heights_capacity[flow_on_coming_time->real_leave_time] ==
                            flow_on_coming_time->bandwidth) {
                            port.heights_capacity.erase(flow_on_coming_time->real_leave_time);
                        } else {
                            port.heights_capacity[flow_on_coming_time->real_leave_time] -= flow_on_coming_time->bandwidth;
                        }
                        // 将本流放置到该平台上的流的原位置
                        pass_flow(port, flow, height);
                        // 将挪走的流寻找新的位置放置，递归调用put_flow
                        put_flow(*flow_on_coming_time, ports, flows);
                        return;
                    } else {
                        // 替换原放置的一个流后，剩余带宽容量不减小，后续的flow_on_coming_time也不会减小，跳出循环
                        break;
                    }
                } else {
                    // 该平台上的剔除原放置的一个流后余下带宽容量不足以放置本流，尝试替换下一个更宽的流
                    continue;
                }
            }
            // 执行到此，该端口该平台无法替换流，跳过该端口
        }
    }
    // 执行到此，本流无法于任何端口在best_leave_time离开
    // 求每个端口的最大高度数组
    std::vector<std::pair<Port *, int>> max_heights;  // <point to port, max_height>
    max_heights.reserve(ports.size());
    for (auto port: ports) {
        max_heights.emplace_back(&port, port.heights_capacity.rbegin()->first);
    }
    // 若该流的coming_time已到达每个端口的最大高度，不能再继续递归
    std::pair<Port *, int> max_height = *std::max_element(max_heights.begin(), max_heights.end(),
                                                          [](const std::pair<Port *, int> &a,
                                                             const std::pair<Port *, int> &b) {
                                                              return a.second < b.second;
                                                          });
    if (flow.coming_time >= max_height.second) {
        flow.is_sent = true;
        flow.send_time = flow.coming_time;
        flow.real_leave_time = flow.coming_time + flow.occupied_time;
        // 一个很差的解决方案：该端口本流real_leave_time之前空间都舍弃
        Port &port = *max_height.first;
        port.heights_capacity.clear();
        port.heights_flows.clear();
        port.heights_capacity[flow.real_leave_time] = port.bandwidth_capacity;
        return;
    }
    flow.coming_time++;
    put_flow(flow, ports, flows);
}

void solve(std::vector<Flow> &flows, std::vector<Port> &ports, const std::string &data_path) {
    // flows排序按照best_leave_time升序->coming_time升序->bandwidth降序
    std::sort(flows.begin(), flows.end(), [](const Flow &a, const Flow &b) {
        if (a.best_leave_time != b.best_leave_time) {
            return a.best_leave_time < b.best_leave_time;
        } else if (a.coming_time != b.coming_time) {
            return a.coming_time < b.coming_time;
        } else {
            return a.bandwidth > b.bandwidth;
        }
    });
    // 测试用，看flow样本特征
//    write_sorted_flows(data_path, flows);
    // ports按照bandwidth_capacity降序
    std::sort(ports.begin(), ports.end(), [](const Port &a, const Port &b) {
        return a.bandwidth_capacity > b.bandwidth_capacity;
    });

    for (auto &flow: flows) {
        put_flow(flow, ports, flows);
        std::cout << "flow " << flow.id << " is sent: " << flow.send_port << "\treal_leave_time: "
                  << flow.real_leave_time << std::endl;
    }

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
            write_result(data_path, flows);
            data_num++;
        } else {
            // 终止遍历
            break;
        }
    }
    return 0;
}
