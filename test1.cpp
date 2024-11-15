#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <queue>
#include <filesystem>
#include <memory>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <future>

const size_t MEMORY_LIMIT = 16 * 1024 * 1024; // 16KB的内存限制
const size_t BLOCK_SIZE = MEMORY_LIMIT / sizeof(int64_t); // 每个块的大小，以int64_t为单位
const size_t CACHE_SIZE = 8 * 1024 * 1024; // 8KB的缓存大小
const size_t MERGE_BATCH_SIZE = 8; // 每次合并的文件数

// 缓存类
class Buffer {
public:
    Buffer(size_t size) : size_(size), buffer_(new char[size]), write_pos_(0), read_pos_(0), should_delete_(true) {}

    // 禁用拷贝构造函数和赋值操作符，避免错误的内存管理
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;

    ~Buffer() {
        if (should_delete_) {
            delete[] buffer_;
        }
    }

    bool Write(const void* data, size_t size) {
        if (write_pos_ + size > size_) {
            if (!ExpandBuffer()) {
                throw std::runtime_error("Buffer expansion failed");
            }
        }
        std::memcpy(buffer_ + write_pos_, data, size);
        write_pos_ += size;
        return true;
    }

    bool Read(void* data, size_t size) {
        if (read_pos_ + size > write_pos_) {
            return false; // 数据不足，返回失败
        }
        std::memcpy(data, buffer_ + read_pos_, size);
        read_pos_ += size;
        return true;
    }

    bool IsFull() const { return write_pos_ == size_; }
    bool IsEmpty() const { return write_pos_ == read_pos_; }

    void Reset() { write_pos_ = read_pos_ = 0; }

    char* GetBuffer() { return buffer_; }
    size_t GetWritePos() const { return write_pos_; }

private:
    size_t size_;
    char* buffer_;
    size_t write_pos_;
    size_t read_pos_;
    bool should_delete_; // 控制是否释放内存

    bool ExpandBuffer() {
        size_t new_size = size_ * 2;
        char* new_buffer = new char[new_size];
        if (!new_buffer) {
            return false; // 内存分配失败
        }
        std::memcpy(new_buffer, buffer_, size_);
        if (should_delete_) {
            delete[] buffer_; // 确保原有内存只被释放一次
        }
        buffer_ = new_buffer;
        size_ = new_size;
        should_delete_ = true;
        return true;
    }
};

// 外部排序类
class ExternalSorter {
public:
    ExternalSorter(const std::string& output_path) : output_path_(output_path), buffer_(CACHE_SIZE) {}

    void Sort(const std::vector<std::string>& input_files) {
        std::vector<std::string> temp_files;
        SplitAndSort(input_files, temp_files);
        MergeInBatches(temp_files, output_path_);
        Cleanup(temp_files);
    }

private:
    std::string output_path_;
    Buffer buffer_; // 缓存
    std::mutex file_mutex_; // 保护文件流访问的互斥量

    void SplitAndSort(const std::vector<std::string>& input_files, std::vector<std::string>& temp_files) {
        std::vector<std::future<void>> futures;

        for (const auto& file_path : input_files) {
            // 为每个文件分配一个异步任务
            futures.push_back(std::async(std::launch::async, &ExternalSorter::ProcessFile, this, file_path, std::ref(temp_files)));
        }

        // 等待所有异步任务完成
        for (auto& fut : futures) {
            fut.get();
        }
    }

    void ProcessFile(const std::string& file_path, std::vector<std::string>& temp_files) {
        std::ifstream input(file_path, std::ios::binary);
        if (!input.is_open()) {
            std::cerr << "无法打开文件: " << file_path << std::endl;
            return;
        }

        std::vector<int64_t> data_block;
        int64_t value;
        while (input.read(reinterpret_cast<char*>(&value), sizeof(value))) {
            data_block.push_back(value);
            if (data_block.size() == BLOCK_SIZE) {
                SortAndWriteBlock(data_block, temp_files);
                data_block.clear();
            }
        }

        if (!data_block.empty()) {
            SortAndWriteBlock(data_block, temp_files);
        }

        input.close();
    }

    void SortAndWriteBlock(std::vector<int64_t>& data_block, std::vector<std::string>& temp_files) {
        std::sort(data_block.begin(), data_block.end());

        // 将临时文件路径改为temp_sort文件夹下
        std::string temp_file = "temp_sort/temp_" + std::to_string(std::rand()) + ".bin";

        // 确保temp_sort文件夹存在
        std::filesystem::create_directory("temp_sort");

        std::ofstream output(temp_file, std::ios::binary);
        if (!output.is_open()) {
            std::cerr << "无法打开临时文件: " << temp_file << std::endl;
            return;
        }

        for (int64_t value : data_block) {
            if (!buffer_.Write(&value, sizeof(value))) {
                // 如果缓存满了，先将缓存中的数据写入文件
                FlushBuffer(output);
                buffer_.Write(&value, sizeof(value));
            }
        }

        // 将剩余的数据写入文件
        FlushBuffer(output);
        temp_files.push_back(temp_file);
    }

    void FlushBuffer(std::ofstream& output) {
        if (!buffer_.IsEmpty()) {
            // 使用Buffer的公共方法获取缓存数据和写入位置
            output.write(buffer_.GetBuffer(), buffer_.GetWritePos());
            buffer_.Reset();
        }
    }

    void MergeInBatches(std::vector<std::string>& temp_files, const std::string& output_path) {
        while (temp_files.size() > 1) {
            std::vector<std::string> next_batch_files;
            std::vector<std::future<std::string>> futures;

            // 分批处理每次最多 MERGE_BATCH_SIZE 个文件
            for (size_t i = 0; i < temp_files.size(); i += MERGE_BATCH_SIZE) {
                size_t batch_end = std::min(i + MERGE_BATCH_SIZE, temp_files.size());
                std::vector<std::string> batch_files(temp_files.begin() + i, temp_files.begin() + batch_end);
                futures.push_back(std::async(std::launch::async, &ExternalSorter::MergeFiles, this, batch_files));
            }

            // 等待所有合并任务完成
            for (auto& fut : futures) {
                next_batch_files.push_back(fut.get());
            }

            temp_files = std::move(next_batch_files); // 更新临时文件列表
        }

        // 最终合并文件
        std::filesystem::rename(temp_files[0], output_path);
    }

std::string ExternalSorter::MergeFiles(const std::vector<std::string>& files) {
    std::priority_queue<std::pair<int64_t, std::shared_ptr<std::ifstream>> min_heap;
    std::vector<std::shared_ptr<std::ifstream>> streams;

    for (const auto& file : files) {
        auto input = std::make_shared<std::ifstream>(file, std::ios::binary);
        if (!input->is_open()) {
            std::cerr << "无法打开临时文件: " << file << std::endl;
            continue;
        }

        int64_t value;
        if (input->read(reinterpret_cast<char*>(&value), sizeof(value))) {
            min_heap.push({value, input});
        } else {
            std::cerr << "无法从临时文件读取: " << file << std::endl;
        }
        streams.push_back(input);
    }

    std::string merged_file = "temp_sort/merged_" + std::to_string(std::mt19937(std::random_device())()) + ".bin";
    std::ofstream output(merged_file, std::ios::binary);
    if (!output.is_open()) {
        std::cerr << "无法打开合并文件: " << merged_file << std::endl;
        return merged_file;
    }

    while (!min_heap.empty()) {
        auto [value, stream] = min_heap.top();
        min_heap.pop();
        output.write(reinterpret_cast<const char*>(&value), sizeof(value));

        if (stream->read(reinterpret_cast<char*>(&value), sizeof(value))) {
            min_heap.push({value, stream});
        }
    }

    // 删除临时文件
    for (const auto& file : files) {
        std::filesystem::remove(file);
    }

    return merged_file;
}
            while (!min_heap.empty()) {
                auto [value, stream] = min_heap.top();
                min_heap.pop();
                output.write(reinterpret_cast<const char*>(&value), sizeof(value));

                if (stream->read(reinterpret_cast<char*>(&value), sizeof(value))) {
                    min_heap.push({value, stream});
                }
            }

            // 合并完一个文件后，删除临时文件
            for (const auto& file : files) {
                std::filesystem::remove(file);
            }

            return merged_file;
            }

            void Cleanup(const std::vector<std::string>& temp_files) {
            for (const auto& file : temp_files) {
                std::filesystem::remove(file);
            }
            }
            };

            int main() {
            std::string input_dir = "test_files"; // 更新为包含names.txt的文件夹路径
            std::string output_file = "sorted_data.bin";
            std::vector<std::string> input_files;

            // 从test_files/names.txt文件中读取需要排序的文件名
            std::ifstream name_file(input_dir + "/names.txt"); // 更新文件路径
            if (name_file.is_open()) {
            std::string file_name;
            while (std::getline(name_file, file_name)) {
                input_files.push_back(input_dir + "/" + file_name);
            }
            name_file.close();
            } else {
            std::cerr << "无法打开names.txt" << std::endl;
            return -1;
            }

            ExternalSorter sorter(output_file);
            sorter.Sort(input_files);

            std::cout << "排序完成，结果保存为 " << output_file << std::endl;
            return 0;
            }
