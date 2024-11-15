import struct
import os
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def count_integers_in_file(file_path):
    """计算文件中64位整数的数量"""
    try:
        count = 0
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(8)  # 每次读取8字节
                if not data:
                    break  # 文件结尾
                # 解码为int64_t类型
                struct.unpack('q', data)  # 'q'表示64位有符号整数
                count += 1  # 增加数据计数
        logging.info(f"文件 {file_path} 包含 {count} 个整数。")
        return count
    except Exception as e:
        logging.error(f"无法读取文件 {file_path}: {e}")
        return 0

def check_if_sorted(file_path):
    """检查sorted_data.bin是否排序"""
    prev_value = None
    try:
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(8)  # 每次读取8字节
                if not data:
                    break  # 文件结尾
                value = struct.unpack('q', data)[0]  # 'q'表示64位有符号整数

                # 检查是否是升序
                if prev_value is not None and prev_value > value:
                    logging.error(f"文件 {file_path} 未按升序排列: {prev_value} > {value}")
                    return False  # 如果未升序，返回 False

                prev_value = value
        logging.info(f"文件 {file_path} 已按升序排列。")
        return True
    except Exception as e:
        logging.error(f"无法读取文件 {file_path}: {e}")
        return False

def check_all_bin_files_in_names(names_file):
    """检查names.txt中列出的所有二进制文件的总数据个数"""
    total_count = 0
    dir_path = os.path.dirname(names_file)  # 获取names.txt文件所在目录

    try:
        with open(names_file, "r") as name_file:
            for line in name_file:
                file_name = line.strip()  # 去掉空格和换行符
                if file_name.endswith(".bin"):
                    file_path = os.path.join(dir_path, file_name)  # 构造完整文件路径
                    if os.path.exists(file_path):
                        logging.info(f"正在检查文件: {file_path}")
                        count = count_integers_in_file(file_path)
                        total_count += count
                    else:
                        logging.warning(f"文件不存在: {file_path}")  # 添加文件不存在的检查
                else:
                    logging.warning(f"无效的文件扩展名: {file_name}")  # 文件扩展名不是.bin
    except Exception as e:
        logging.error(f"无法读取names.txt: {e}")

    return total_count

def compare_file_counts(names_file, sorted_file):
    """比较names.txt列出的所有.bin文件和sorted_data.bin的总数据个数"""
    logging.info("正在检查sorted_data.bin文件是否按升序排列...")
    sorted_status = check_if_sorted(sorted_file)

    if sorted_status:
        logging.info(f"sorted_data.bin 已排序")
    else:
        logging.error(f"sorted_data.bin 未排序")

    logging.info("\n正在检查names.txt中列出的所有文件...")
    total_count_from_names = check_all_bin_files_in_names(names_file)

    logging.info(f"\nnames.txt中所有文件的总数据个数: {total_count_from_names}")
    sorted_count = count_integers_in_file(sorted_file)
    logging.info(f"sorted_data.bin中的数据个数: {sorted_count}")

    if total_count_from_names == sorted_count:
        logging.info("\nnames.txt中的文件总数据个数与sorted_data.bin的数据个数匹配。")
    else:
        logging.error("\nnames.txt中的文件总数据个数与sorted_data.bin的数据个数不匹配。")

if __name__ == "__main__":
    names_file = "test_files/names.txt"  # names.txt文件路径
    sorted_file = "sorted_data.bin"  # sorted_data.bin文件路径

    compare_file_counts(names_file, sorted_file)
