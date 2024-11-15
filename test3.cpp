#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <queue>
#include <filesystem>
#include <memory>
#include <thread>
#include <future>
#include <string>
#include <cstring>
#include <chrono>
#include <sstream>
#include <iomanip>

#define CACHE_SIZE 1024  // 缓冲区大小
#define BLOCK_SIZE 5000  // 每个数据块的大小
#define MERGE_BATCH_SIZE 2  // 每次合并的文件批次大小

// 缓冲区类，管理内存中存储的整数数据
class Buffer {
public:
    // 构造函数，分配缓冲区内存
    Buffer(size_t size) : size_(size / sizeof(int64_t)), data_(new int64_t[size / sizeof(int64_t)]), write_pos_(0) {}

    // 向缓冲区写入数据
    bool Write(const void* data, size_t size, std::ofstream& output) {
        size_t num_elements = size / sizeof(int64_t);
        // 检查缓冲区是否有足够的空间
        if (write_pos_ + num_elements > size_) {
            return false;  // 缓冲区满
        }
        // 将数据复制到缓冲区
        std::memcpy(data_ + write_pos_, data, size);
        write_pos_ += num_elements;
        return true;
    }

    // 重置缓冲区（清空写入位置）
    void Reset() { write_pos_ = 0; }

    // 检查缓冲区是否为空
    bool IsEmpty() const { return write_pos_ == 0; }

    // 获取缓冲区内的数据指针
    const int64_t* GetBuffer() const { return data_; }

    // 获取当前的写入位置
    size_t GetWritePos() const { return write_pos_; }

private:
    size_t size_;  // 缓冲区的大小（以元素个数为单位）
    int64_t* data_;  // 缓冲区的数据
    size_t write_pos_;  // 当前写入位置
};

// 外部排序类，负责管理外部排序过程
class ExternalSorter {
public:
    // 构造函数，接受输出路径
    ExternalSorter(const std::string& output_path)
        : output_path_(output_path), buffer_(CACHE_SIZE) {}

    // 主排序和合并函数
    void SortAndMerge(const std::vector<std::string>& input_files) {
        std::vector<std::string> temp_files;  // 存储临时文件路径
        SplitAndSort(input_files, temp_files);  // 将输入文件分割并排序
        MergeInBatches(temp_files, output_path_);  // 批量合并排序后的临时文件
        Cleanup(temp_files);  // 清理临时文件
    }

private:
    // 将输入文件拆分为数据块并排序
    void SplitAndSort(const std::vector<std::string>& input_files, std::vector<std::string>& temp_files);

    // 排序数据块并写入临时文件
    void SortAndWriteBlock(std::vector<int64_t>& data_block, std::vector<std::string>& temp_files);

    // 刷新缓冲区，将缓冲区中的数据写入输出文件
    void FlushBuffer(std::ofstream& output);

    // 批量合并排序后的临时文件
    void MergeInBatches(std::vector<std::string>& temp_files, const std::string& output_path);

    // 合并多个文件为一个文件
    std::string MergeFiles(const std::vector<std::string>& files);

    // 清理临时文件
    void Cleanup(const std::vector<std::string>& temp_files);

    std::string output_path_;  // 输出文件路径
    Buffer buffer_;  // 缓冲区
};

// 将输入文件拆分成多个数据块并进行排序
void ExternalSorter::SplitAndSort(const std::vector<std::string>& input_files, std::vector<std::string>& temp_files) {
    // 遍历所有输入文件
    for (const auto& file_path : input_files) {
        std::ifstream input(file_path, std::ios::binary);
        if (!input.is_open()) {
            std::cerr << "无法打开文件: " << file_path << std::endl;
            continue;
        }

        std::vector<int64_t> data_block;  // 存储数据块
        int64_t value;

        // 逐个读取文件中的数据
        while (input.read(reinterpret_cast<char*>(&value), sizeof(value))) {
            data_block.push_back(value);  // 将数据添加到数据块中
            // 如果数据块已满，进行排序并写入临时文件
            if (data_block.size() == BLOCK_SIZE) {
                SortAndWriteBlock(data_block, temp_files);
                data_block.clear();  // 清空数据块
            }
        }

        // 如果数据块非空，进行排序并写入临时文件
        if (!data_block.empty()) {
            SortAndWriteBlock(data_block, temp_files);
        }

        input.close();
    }
}

// 排序数据块并将其写入临时文件
void ExternalSorter::SortAndWriteBlock(std::vector<int64_t>& data_block, std::vector<std::string>& temp_files) {
    std::sort(data_block.begin(), data_block.end());  // 对数据块进行排序

    // 获取当前时间戳作为临时文件名的一部分
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    // 创建唯一的临时文件名
    std::string temp_file = "temp_sort/temp_" + std::to_string(timestamp) + ".bin";

    std::filesystem::create_directory("temp_sort");  // 创建临时文件目录

    std::ofstream output(temp_file, std::ios::binary);
    if (!output.is_open()) {
        std::cerr << "无法创建临时文件: " << temp_file << std::endl;
        return;
    }

    // 将排序后的数据写入临时文件
    for (int64_t value : data_block) {
        if (!buffer_.Write(&value, sizeof(value), output)) {
            FlushBuffer(output);  // 如果缓冲区满了，先刷新缓冲区
            buffer_.Write(&value, sizeof(value), output);  // 将数据写入缓冲区
        }
    }

    FlushBuffer(output);  // 刷新缓冲区
    temp_files.push_back(temp_file);  // 将临时文件路径加入到列表中
}

// 刷新缓冲区，将缓冲区内的数据写入输出文件
void ExternalSorter::FlushBuffer(std::ofstream& output) {
    if (!buffer_.IsEmpty()) {
        output.write(reinterpret_cast<const char*>(buffer_.GetBuffer()), buffer_.GetWritePos() * sizeof(int64_t));  // 写入缓冲区的数据
        buffer_.Reset();  // 重置缓冲区
    }
}

// 批量合并临时文件，直到剩下一个文件为止
void ExternalSorter::MergeInBatches(std::vector<std::string>& temp_files, const std::string& output_path) {
    // 当临时文件数量大于1时，进行批量合并
    while (temp_files.size() > 1) {
        std::vector<std::future<std::string>> futures;  // 存储异步任务
        std::vector<std::string> next_batch_files;  // 存储下一批次合并后的文件

        // 将临时文件分批次进行合并
        for (size_t i = 0; i < temp_files.size(); i += MERGE_BATCH_SIZE) {
            size_t batch_end = std::min(i + MERGE_BATCH_SIZE, temp_files.size());
            std::vector<std::string> batch_files(temp_files.begin() + i, temp_files.begin() + batch_end);

            // 使用异步任务进行文件合并
            futures.push_back(std::async(std::launch::async, &ExternalSorter::MergeFiles, this, batch_files));
        }

        // 等待所有的合并任务完成，并将结果存储到下一批次文件列表中
        for (auto& future : futures) {
            next_batch_files.push_back(future.get());
        }

        temp_files = std::move(next_batch_files);  // 更新临时文件列表
    }

    // 将最终的合并结果重命名为输出文件
    std::filesystem::rename(temp_files[0], output_path);
}

// 合并多个文件为一个文件，使用优先队列保持排序
std::string ExternalSorter::MergeFiles(const std::vector<std::string>& files) {
    std::priority_queue<std::pair<int64_t, std::shared_ptr<std::ifstream>>,
                        std::vector<std::pair<int64_t, std::shared_ptr<std::ifstream>>>,
                        std::greater<std::pair<int64_t, std::shared_ptr<std::ifstream>>>> min_heap;  // 最小堆

    std::vector<std::shared_ptr<std::ifstream>> streams;  // 存储文件流
    for (const auto& file : files) {
        auto input = std::make_shared<std::ifstream>(file, std::ios::binary);
        if (!input->is_open()) {
            std::cerr << "无法打开临时文件: " << file << std::endl;
            continue;
        }

        int64_t value;
        if (input->read(reinterpret_cast<char*>(&value), sizeof(value))) {
            min_heap.push({value, input});  // 将文件流的第一个元素加入最小堆
        } else {
            std::cerr << "无法从临时文件读取: " << file << std::endl;
        }
        streams.push_back(input);  // 保存文件流
    }

    // 创建合并后的文件
    std::string merged_file = "temp_sort/merged_" + std::to_string(rand()) + ".bin";
    std::ofstream output(merged_file, std::ios::binary);
    if (!output.is_open()) {
        std::cerr << "无法打开合并文件: " << merged_file << std::endl;
        return merged_file;
    }

    // 从最小堆中取出最小的元素并写入输出文件
    while (!min_heap.empty()) {
        auto [value, stream] = min_heap.top();
        min_heap.pop();
        output.write(reinterpret_cast<const char*>(&value), sizeof(value));  // 写入文件

        // 如果文件流还有数据，继续读取并加入最小堆
        if (stream->read(reinterpret_cast<char*>(&value), sizeof(value))) {
            min_heap.push({value, stream});
        }
    }

    // 删除已经合并的临时文件
    for (const auto& file : files) {
        std::filesystem::remove(file);
    }

    return merged_file;  // 返回合并后的文件路径
}

// 清理临时文件
void ExternalSorter::Cleanup(const std::vector<std::string>& temp_files) {
    for (const auto& file : temp_files) {
        std::filesystem::remove(file);  // 删除临时文件
    }
}

int main() {
    std::string input_dir = "test_files";  // 输入文件夹路径
    std::string output_file = "sorted_data.bin";  // 输出文件路径
    std::vector<std::string> input_files;

    std::ifstream names_file(input_dir + "/names.txt");  // 打开文件名列表
    if (!names_file.is_open()) {
        std::cerr << "无法打开 names.txt 文件" << std::endl;
        return 1;
    }

    std::string file_name;
    // 读取文件名并加入到输入文件列表
    while (std::getline(names_file, file_name)) {
        input_files.push_back(input_dir + "/" + file_name);
    }

    names_file.close();  // 关闭文件

    // 如果没有找到任何输入文件，则报错
    if (input_files.empty()) {
        std::cerr << "没有找到任何输入文件" << std::endl;
        return 1;
    }

    ExternalSorter sorter(output_file);  // 创建外部排序器
    sorter.SortAndMerge(input_files);  // 执行排序和合并操作

    std::cout << "排序完成，结果保存在: " << output_file << std::endl;
    return 0;
}
