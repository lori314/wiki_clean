import json
import os
import glob
import re
from tqdm import tqdm

#设置每个文件块的大小为20M
CHUNK_SIZE_BYTES = 20*1024*1024

def clean_dirty_text(text):
    """
    数据清洗函数，这里是一个比较简单的实现，可以根据实际需求增加数据筛选逻辑

    Args:
    text(str): 待清洗的字符串

    Return:
    str: 处理后的字符串
    """
    text = re.sub(r'<.*?>', '', text)
    return text


def clean_data(dirty_text_stream):
    """
    一个生成器，用来从脏的数据中提取关键信息
    
    Args:
    dirty_text_stream(iterable): 一个可迭代对象，每一个对象都是一个json格式的字符串
    
    Yields:
    str: 一个重新打包后的json格式的字符串，包含训练预处理所需信息并符合格式要求
    """
    for dirty_text in dirty_text_stream:
        dirty_text_dict = json.loads(dirty_text)
        clean_text_dict = {}
        if "text" in dirty_text_dict and len(dirty_text_dict["text"])!=0:
            clean_text_dict["text"] = clean_dirty_text(dirty_text_dict["text"])
        else:
            continue
        meta = {}
        if "title" in dirty_text_dict: 
            meta["title"] = dirty_text_dict["title"]
        if "id" in dirty_text_dict:
            meta["id"] = dirty_text_dict["id"]
        if "url" in dirty_text_dict:
            meta["url"] = dirty_text_dict["url"]
        clean_text_dict["meta"] = meta
        clean_text = json.dumps(clean_text_dict,ensure_ascii=False)
        yield clean_text

def extracted_wiki():
    """
    一个生成器，用来将下载并由wikiextractor处理后的dumps中的json格式字符串按行提取

    Yields:
    str: 一个json格式字符串，按utf-8格式编码，即处理后的dumps中读取到的行
    """
    input_dir = "extracted_test"
    file_paths = glob.glob(os.path.join(input_dir, '**', 'wiki_*'), recursive=True)

    for file_path in tqdm(file_paths, desc="文件处理进度"):
        with open(file_path,"r",encoding='utf-8') as f:
            for line in f:
                yield line

def main():
    """
    主函数，处理目录下的数据并按jsonl格式分块存储
    """
    dirty_text_stream = extracted_wiki()
    clean_text_stream = clean_data(dirty_text_stream)

    current_chunk_index = 0
    current_chunk_size = 0
    current_outputfile = None

    if not os.path.exists("output_dir"):
        os.makedirs("output_dir")

    dir_name = "output_dir"
    #这里要对文件分块存储，以防止单个文件过大而不方便使用
    for clean_text in clean_text_stream:
        if current_outputfile == None or current_chunk_size > CHUNK_SIZE_BYTES:
            if current_outputfile != None:
                current_outputfile.close()
            current_outputfile = open(os.path.join(dir_name,"outputfile" + str(current_chunk_index) + '.jsonl'),"w",encoding='utf-8')
            current_chunk_size = 0
            current_chunk_index += 1
            
        writen_bytes = current_outputfile.write(clean_text + '\n')
        current_chunk_size += writen_bytes

    if current_outputfile and not current_outputfile.closed:
        current_outputfile.close()

    print("--处理完毕--")

if __name__ == "__main__":
    main()