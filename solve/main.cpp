#pragma GCC optimize(3)
#pragma GCC optimize("inline")

#include <iostream>
#include <sstream>
#include <algorithm>
#include "string"
#include "vector"
#include "sys/stat.h"
#include "fstream"

class Flow {
public:
    int id;
    int bandwidth;
    int coming_time;
    int occupied_time; // 在端口上的占用时间
    bool is_sent; // 是否已经发送
    int send_time; // 发送时间
    int send_port; // 发送端口
public:
    Flow(int id, int bandwidth, int coming_time, int occupied_time) {
        this->id = id;
        this->bandwidth = bandwidth;
        this->coming_time = coming_time;
        this->occupied_time = occupied_time;
        this->is_sent = false;
        this->send_time = -1;
        this->send_port = -1;
    }
};

class Port {
public:
    int id;
    int bandwidth_capacity;
public:
    Port(int id, int bandwidth_capacity) {
        this->id = id;
        this->bandwidth_capacity = bandwidth_capacity;
    }
};

void read_files(const std::string& data_path, std::vector<Flow> &flows, std::vector<Port> &ports) {
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

void solve(std::vector<Flow> &flows, std::vector<Port> &ports) {

}

void write_result(const std::string& data_path, const std::vector<Flow>& flows) {
    std::ofstream file(data_path + "/result.txt");
    for (const auto& flow : flows) {
        file << flow.id << "," << flow.send_port << "," << flow.send_time << std::endl;
    }
    file.close();
}

void write_sorted_flows(const std::string& data_path, const std::vector<Flow>& flows) {
    std::ofstream file(data_path + "/sorted_flows.txt");
    for (const auto& flow : flows) {
        file << flow.id << "," << flow.bandwidth << "," << flow.coming_time << "," << flow.occupied_time << std::endl;
    }
    file.close();
}

// 输出文件result不加第一行描述，不用排序，放在和输入文件同目录
int main() {
    int data_num=0;
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
            // flows排序按照coming_time升序->occupied_time降序->bandwidth降序
            std::sort(flows.begin(), flows.end(), [](const Flow& a, const Flow& b) {
                if (a.coming_time != b.coming_time) {
                    return a.coming_time < b.coming_time;
                } else if (a.occupied_time != b.occupied_time) {
                    return a.occupied_time > b.occupied_time;
                } else {
                    return a.bandwidth > b.bandwidth;
                }
            });
            // 测试用，看flow样本特征
            write_sorted_flows(data_path, flows);
            // ports按照bandwidth_capacity降序
            std::sort(ports.begin(), ports.end(), [](const Port& a, const Port& b) {
                return a.bandwidth_capacity > b.bandwidth_capacity;
            });
            // 流调度
            solve(flows, ports);
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
