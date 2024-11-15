import os
import random
import struct
import logging
import time

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义常量作为宏
NUM_BIN_FILES = 10  # .bin文件的数量
MIN_FILE_SIZE = 1 * 8192  # 最小文件大小（64位整数个数），即1KB
MAX_FILE_SIZE = 128 * 8192  # 最大文件大小（64位整数个数），即1024KB
BATCH_SIZE = 1024  # 批量写入每次的64位整数数量，减少写操作次数

def generate_large_file(file_path, num_integers):
    """生成一个包含随机64位整数的文件"""
    try:
        with open(file_path, 'wb') as f:
            for i in range(0, num_integers, BATCH_SIZE):
                batch_size = min(BATCH_SIZE, num_integers - i)
                batch = [random.randint(-2**63, 2**63 - 1) for _ in range(batch_size)]
                f.write(struct.pack(f'{batch_size}q', *batch))
        logging.info(f"文件 {file_path} 生成成功。")
    except Exception as e:
        logging.error(f"写入文件 {file_path} 时发生错误: {e}")
        raise

def generate_test_files(num_files, directory):
    """生成指定数量的测试文件，并返回文件路径列表"""
    try:
        os.makedirs(directory, exist_ok=True)
        file_paths = []
        for i in range(num_files):
            num_integers = random.randint(MIN_FILE_SIZE, MAX_FILE_SIZE)
            timestamp = int(time.time())  # 使用时间戳避免文件名冲突
            file_path = os.path.join(directory, f"data_{timestamp}_{i+1}.bin")
            generate_large_file(file_path, num_integers)
            file_paths.append(file_path)
        logging.info(f"所有测试文件已生成在 {directory}。")
        return file_paths
    except Exception as e:
        logging.error(f"生成测试文件时发生错误: {e}")
        raise

def generate_names_file(file_paths, names_file_path):
    """生成包含文件名列表的names.txt文件"""
    try:
        with open(names_file_path, 'w') as f:
            for file_path in file_paths:
                f.write(os.path.basename(file_path) + "\n")
        logging.info(f"文件名列表已保存到 {names_file_path}。")
    except Exception as e:
        logging.error(f"写入 {names_file_path} 时发生错误: {e}")
        raise

if __name__ == "__main__":
    directory = "test_files"
    names_file = os.path.join(directory, "names.txt")
    try:
        file_paths = generate_test_files(NUM_BIN_FILES, directory)
        generate_names_file(file_paths, names_file)
        logging.info(f"测试文件已生成，文件列表保存在 {names_file}")
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {e}")
