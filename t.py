import os
import glob

# 指定要删除文件的目录
directory = "."

# 使用 glob 模块匹配以 temp 开头的所有文件
files_to_delete = glob.glob(os.path.join(directory, 'merged*'))

# 删除这些文件
for file in files_to_delete:
    try:
        os.remove(file)
        print(f"已删除文件: {file}")
    except Exception as e:
        print(f"删除文件 {file} 失败: {e}")
