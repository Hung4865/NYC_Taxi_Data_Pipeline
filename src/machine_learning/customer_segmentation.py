import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Setup path for shared utils
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'shared_utils'))
sys.path.append(utils_path)

from helpers import load_cfg

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

def create_spark_session(hdfs_host, hdfs_port):
    """Khởi tạo Spark Session kết nối với HDFS"""
    try:
        spark = SparkSession.builder \
            .appName("Customer Segmentation ML") \
            .config("spark.hadoop.fs.defaultFS", f"hdfs://{hdfs_host}:{hdfs_port}") \
            .getOrCreate()
        logging.info("Tạo Spark Session thành công!")
        return spark
    except Exception as e:
        logging.error(f"Lỗi tạo Spark Session: {e}")
        sys.exit(1)

def run_clustering_pipeline():
    # Load config
    cfg_path = os.path.join(os.path.dirname(__file__), "..", "..", "infra", "configurations", "datalake.yaml")
    if not os.path.exists(cfg_path):
        logging.error(f"Không tìm thấy file cấu hình tại {cfg_path}")
        return

    cfg = load_cfg(cfg_path)
    bucket_name = cfg["datalake"]["bucket_name_2"] # Thư mục processed data
    hdfs_host = 'localhost' # Kết nối từ bên ngoài container, dùng localhost
    hdfs_port = 8020
    
    spark = create_spark_session(hdfs_host, hdfs_port)
    
    data_path = f"hdfs://{hdfs_host}:{hdfs_port}/{bucket_name}/processed_yellow_tripdata_2022-01.parquet"
    
    logging.info(f"Đọc dữ liệu từ {data_path}...")
    try:
        # Trong thực tế, bạn sẽ đọc từ HDFS. 
        # Tuy nhiên để test nhanh nếu HDFS chưa có dữ liệu, ta có thể dùng thử file cục bộ.
        df = spark.read.parquet(data_path)
    except Exception as e:
        logging.error(f"Không thể đọc dữ liệu từ HDFS. Vui lòng đảm bảo bạn đã chạy luồng Ingestion trước: {e}")
        # Chế độ dự phòng: Đọc thẳng từ file gốc để demo thuật toán
        fallback_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "2022", "yellow_tripdata_2022-01.parquet")
        logging.info(f"Chuyển sang đọc dữ liệu dự phòng từ: {fallback_path}")
        df = spark.read.parquet(fallback_path)

    # 1. Tiền xử lý dữ liệu (Lọc nhiễu - Outliers)
    logging.info("Đang lọc nhiễu dữ liệu...")
    clean_df = df.filter(
        (col("trip_distance") > 0) & (col("trip_distance") < 100) & 
        (col("fare_amount") > 0) & (col("fare_amount") < 500)
    ).dropna(subset=["trip_distance", "fare_amount", "tip_amount"])

    # Chỉ lấy mẫu 10% dữ liệu để training cho nhanh (vì file có hàng triệu dòng)
    sample_df = clean_df.sample(withReplacement=False, fraction=0.1, seed=42)

    # 2. Xây dựng ML Pipeline: VectorAssembler
    logging.info("Chuẩn bị Features...")
    features_cols = ["trip_distance", "fare_amount", "tip_amount"]
    assembler = VectorAssembler(inputCols=features_cols, outputCol="raw_features")
    assembled_data = assembler.transform(sample_df)

    # 3. Chuẩn hóa dữ liệu (StandardScaler)
    # Rất quan trọng vì trip_distance (miles) và fare_amount ($) khác biệt lớn về thang đo
    logging.info("Đang chuẩn hóa thang đo (StandardScaler)...")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembled_data)
    scaled_data = scaler_model.transform(assembled_data)

    # 4. Đào tạo mô hình K-Means
    k = 4 # Chúng ta giả định chia làm 4 phân khúc khách hàng
    logging.info(f"Đang đào tạo mô hình K-Means với k={k}...")
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=1)
    model = kmeans.fit(scaled_data)

    # Gán nhãn cho dữ liệu
    predictions = model.transform(scaled_data)

    # 5. Đánh giá mô hình
    evaluator = ClusteringEvaluator(predictionCol="cluster", featuresCol="features")
    silhouette = evaluator.evaluate(predictions)
    logging.info(f"Chỉ số Silhouette (từ -1 đến 1, càng cao càng tốt): {silhouette:.4f}")

    # 6. Phân tích kết quả (Business Interpretability)
    # Lưu ý: Các centroid khi lấy ra từ mô hình đang ở dạng đã Scale. 
    # Nhưng để đọc được, lý tưởng nhất là unscale (trong script này tôi sẽ in ra để mang tính chất hình thức).
    logging.info("\n--- ĐẶC ĐIỂM CÁC PHÂN KHÚC KHÁCH HÀNG (Dữ liệu đã chuẩn hóa) ---")
    centers = model.clusterCenters()
    for i, center in enumerate(centers):
        logging.info(f"Cụm {i}: Distance: {center[0]:.2f} | Fare: {center[1]:.2f} | Tip: {center[2]:.2f}")

    logging.info("Hoàn thành Pipeline Học Máy!")

if __name__ == "__main__":
    run_clustering_pipeline()
