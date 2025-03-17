from feature_store import FeatureStore

fs = FeatureStore("streaming-data-for-ml")

metadata = fs.grouping_features(
    blob="data/main_data.parquet",
    feature_group_id="streaming_data_movie_features",
    name="Streaming data for ML",
    description="All streaming data features ready for ML models",
    source="Streaming Availabilit API from Rapid API by Nokia",
    usage="General use cases for ML models"
)