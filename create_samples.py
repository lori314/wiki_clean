import os

SOURCE_CHUNK_PATH = "output_dir/outputfile0.jsonl"
TARGET_CHUNK_PATH = "cleaned_data_samples/samples.jsonl"
LINES_LIMIT = 1000

def main():
    if not os.path.exists("cleaned_data_samples"):
        os.makedirs("cleaned_data_samples")
    
    current_lines = 0
    try:
        with open(SOURCE_CHUNK_PATH,"r",encoding='utf-8') as inputfile:
            with open(TARGET_CHUNK_PATH,"w",encoding='utf-8') as outputfile:
                while current_lines < LINES_LIMIT:
                    clean_text = inputfile.readline().strip()
                    outputfile.write(clean_text + '\n')
                    current_lines += 1
        
        print("1000条已样例生成，保存在cleaned_data_samples目录下")

    except FileNotFoundError:
        print("样例提取失败")
        print("请确保你已成功运行主清洗脚本，并且路径正确")
            
if __name__ == "__main__":
    main()