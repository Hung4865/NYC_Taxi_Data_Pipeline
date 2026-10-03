import os
import urllib.request

def download_taxi_data():
    # URL cho tháng 01/2022 của Yellow Taxi
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
    
    # Tạo cấu trúc thư mục data/2022/
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_dir = os.path.join(base_dir, "data", "2022")
    os.makedirs(target_dir, exist_ok=True)
    
    file_path = os.path.join(target_dir, "yellow_tripdata_2022-01.parquet")
    
    if not os.path.exists(file_path):
        print(f"Downloading data from {url}...")
        print("Please wait, file is about 38MB...")
        urllib.request.urlretrieve(url, file_path)
        print(f"[SUCCESS] Downloaded to: {file_path}")
    else:
        print(f"[SKIPPED] File already exists: {file_path}")

if __name__ == "__main__":
    download_taxi_data()
