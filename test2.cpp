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
    #include <cstring>  // 添加这个头文件

    #define CACHE_SIZE  1024
    #define BLOCK_SIZE 1000 
    #define MERGE_BATCH_SIZE 2

    class Buffer {
    public:
        Buffer(size_t size) : size_(size), data_(new char[size]), write_pos_(0) {}

        bool Write(const void* data, size_t size) {
            if (write_pos_ + size > size_) {
                return false;  // 缓存已满
            }
            std::memcpy(data_ + write_pos_, data, size);
            write_pos_ += size;
            return true;
        }

        void Reset() { write_pos_ = 0; }

        bool IsEmpty() const { return write_pos_ == 0; }

        const char* GetBuffer() const { return data_; }

        size_t GetWritePos() const { return write_pos_; }

    private:
        size_t size_;
        char* data_;
        size_t write_pos_;
    };

    class ExternalSorter {
    public:
        ExternalSorter(const std::string& output_path)
            : output_path_(output_path), buffer_(CACHE_SIZE) {}

        void SortAndMerge(const std::vector<std::string>& input_files) {
            std::vector<std::string> temp_files;
            SplitAndSort(input_files, temp_files);
            MergeInBatches(temp_files, output_path_);
            Cleanup(temp_files);
        }

    private:
        void SplitAndSort(const std::vector<std::string>& input_files, std::vector<std::string>& temp_files);
        void SortAndWriteBlock(std::vector<int64_t>& data_block, std::vector<std::string>& temp_files);
        void FlushBuffer(std::ofstream& output);

        void MergeInBatches(std::vector<std::string>& temp_files, const std::string& output_path);
        std::string MergeFiles(const std::vector<std::string>& files);

        void Cleanup(const std::vector<std::string>& temp_files);

        std::string output_path_;
        Buffer buffer_;
    };

    void ExternalSorter::SplitAndSort(const std::vector<std::string>& input_files, std::vector<std::string>& temp_files) {
        for (const auto& file_path : input_files) {
            std::ifstream input(file_path, std::ios::binary);
            if (!input.is_open()) {
                std::cerr << "无法打开文件: " << file_path << std::endl;
                continue;
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
    }

    void ExternalSorter::SortAndWriteBlock(std::vector<int64_t>& data_block, std::vector<std::string>& temp_files) {
        std::sort(data_block.begin(), data_block.end());

        std::string temp_file = "temp_sort/temp_" + std::to_string(std::rand()) + ".bin";
        std::filesystem::create_directory("temp_sort");

        std::ofstream output(temp_file, std::ios::binary);
        if (!output.is_open()) {
            std::cerr << "无法打开临时文件: " << temp_file << std::endl;
            return;
        }

        for (int64_t value : data_block) {
            if (!buffer_.Write(&value, sizeof(value))) {
                FlushBuffer(output);
                buffer_.Write(&value, sizeof(value));
            }
        }

        FlushBuffer(output);
        temp_files.push_back(temp_file);
    }

    void ExternalSorter::FlushBuffer(std::ofstream& output) {
        if (!buffer_.IsEmpty()) {
            output.write(buffer_.GetBuffer(), buffer_.GetWritePos());
            buffer_.Reset();
        }
    }

    void ExternalSorter::MergeInBatches(std::vector<std::string>& temp_files, const std::string& output_path) {
        while (temp_files.size() > 1) {
            std::vector<std::string> next_batch_files;
            for (size_t i = 0; i < temp_files.size(); i += MERGE_BATCH_SIZE) {
                size_t batch_end = std::min(i + MERGE_BATCH_SIZE, temp_files.size());
                std::vector<std::string> batch_files(temp_files.begin() + i, temp_files.begin() + batch_end);
                std::string merged_file = MergeFiles(batch_files);
                next_batch_files.push_back(merged_file);
            }
            temp_files = std::move(next_batch_files);
        }

        std::filesystem::rename(temp_files[0], output_path);
    }

    std::string ExternalSorter::MergeFiles(const std::vector<std::string>& files) {
        std::priority_queue<std::pair<int64_t, std::shared_ptr<std::ifstream>>,
                            std::vector<std::pair<int64_t, std::shared_ptr<std::ifstream>>>,
                            std::greater<std::pair<int64_t, std::shared_ptr<std::ifstream>>>> min_heap;

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

        std::string merged_file = "temp_sort/merged_" + std::to_string(rand()) + ".bin";
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

        for (const auto& file : files) {
            std::filesystem::remove(file);
        }

        return merged_file;
    }

    void ExternalSorter::Cleanup(const std::vector<std::string>& temp_files) {
        for (const auto& file : temp_files) {
            std::filesystem::remove(file);
        }
    }

    int main() {
        std::string input_dir = "test_files"; // 更新为包含names.txt的文件夹路径
        std::string output_file = "sorted_data.bin";
        std::vector<std::string> input_files;

        std::ifstream names_file(input_dir + "/names.txt");
        if (!names_file.is_open()) {
            std::cerr << "无法打开 names.txt 文件" << std::endl;
            return 1;
        }

        std::string file_name;
        while (std::getline(names_file, file_name)) {
            input_files.push_back(input_dir + "/" + file_name);
        }

        names_file.close();

        if (input_files.empty()) {
            std::cerr << "没有找到任何输入文件" << std::endl;
            return 1;
        }

        ExternalSorter sorter(output_file);
        sorter.SortAndMerge(input_files);

        std::cout << "排序完成，结果保存在: " << output_file << std::endl;
        return 0;
    }
