import json
import os
import glob
from tqdm import tqdm

CHUNK_SIZE_BYTES = 20*1024*1024

def clean_data(dirty_text_stream):
    for dirty_text in dirty_text_stream:
        dirty_text_dict = json.loads(dirty_text)
        clean_text_dict = {}
        if "text" in dirty_text_dict:
            clean_text_dict["text"] = dirty_text_dict["text"]
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
        clean_text = json.dumps(clean_text_dict,ensure_ascii=False,indent=4)
        yield clean_text

def extracted_wiki():
    input_dir = "extracted_test"
    file_paths = glob.glob(os.path.join(input_dir, '**', 'wiki_*'), recursive=True)

    for file_path in tqdm(file_paths, desc="文件处理进度"):
        with open(file_path,"r",encoding='utf-8') as f:
            for line in f:
                yield line

def main():
    dirty_text_stream = extracted_wiki()
    clean_text_stream = clean_data(dirty_text_stream)

    current_chunk_index = 0
    current_chunk_size = 0
    current_outputfile = None

    if not os.path.exists("output_dir"):
        os.makedirs("output_dir")

    dir_name = "output_dir"
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