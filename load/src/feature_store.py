import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from transform.src.gcs import CloudStorageOps
import pandas as pd
import io
import datetime
import hashlib
import json

class FeatureStore:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.ops = CloudStorageOps(bucket_name)
        self.version_history = {}
        self.features_metadata = {}

    def metadata(self, feature_group_id, feature_group_name, feature_group_description, source, usage):
        created_at = datetime.datetime.now().isoformat()
    
        metadata = {
            "feature_group_id": feature_group_id,
            "feature_group_name": feature_group_name,
            "feature_group_description": feature_group_description,
            "created_at": created_at,
            "source": source,
            "usage": usage,
            "features": [],
            "version": "1.0"
        }   

        self.features_metadata[feature_group_id] = metadata

        return metadata

    def add_feature_metadata(self, feature_group_id, feature_name, feature_type, description=None, stats=None):
        if feature_group_id not in self.features_metadata:
            raise ValueError(f"Feature groups {feature_group_id} not found.")
            
        feature_info = {
            "name": feature_name,
            "type": feature_type,
            "description": description or "",
            "stats": stats or {}
        }
        
        self.features_metadata[feature_group_id]["features"].append(feature_info)

    def calculate_dataframe_stats(self, df):
        stats = {}
        
        for column in df.columns:
            column_stats = {}
            
            if pd.api.types.is_numeric_dtype(df[column]):
                column_stats = {
                    "min": float(df[column].min()) if not df[column].isna().all() else None,
                    "max": float(df[column].max()) if not df[column].isna().all() else None,
                    "mean": float(df[column].mean()) if not df[column].isna().all() else None,
                    "null_count": int(df[column].isna().sum()),
                    "type": str(df[column].dtype)
                }
            else:
                column_stats = {
                    "unique_values": int(df[column].nunique()),
                    "null_count": int(df[column].isna().sum()),
                    "type": str(df[column].dtype)
                }
                if df[column].nunique() < 10:
                    column_stats["value_counts"] = df[column].value_counts().to_dict()
            
            stats[column] = column_stats
            
        return stats
    
    def version_control(self, feature_group_id, df, version=None):
        df_hash = hashlib.md5(pd.util.hash_pandas_object(df).values).hexdigest()
        
        if feature_group_id not in self.version_history:
            self.version_history[feature_group_id] = []
        
        for v in self.version_history[feature_group_id]:
            if v["hash"] == df_hash:
                return v["version"]

        if version is None:
            if not self.version_history[feature_group_id]:
                new_version = "1.0"
            else:
                last_version = self.version_history[feature_group_id][-1]["version"]
                major, minor = map(int, last_version.split('.'))
                new_version = f"{major}.{minor + 1}"
        else:
            new_version = version
            
        version_info = {
            "version": new_version,
            "hash": df_hash,
            "timestamp": datetime.datetime.now().isoformat(),
            "rows": len(df),
            "columns": list(df.columns)
        }
        
        self.version_history[feature_group_id].append(version_info)
        
        if feature_group_id in self.features_metadata:
            self.features_metadata[feature_group_id]["version"] = new_version
            
        return new_version
    
    def save_metadata(self, feature_group_id, local_path=None):
        if feature_group_id not in self.features_metadata:
            raise ValueError(f"Grupo de features {feature_group_id} não encontrado")
            
        metadata = self.features_metadata[feature_group_id]
        
        if feature_group_id in self.version_history:
            metadata["version_history"] = self.version_history[feature_group_id]
            
        if local_path is None:
            local_path = f"load/feature_store/metadata_{feature_group_id}.json"
            
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
        with open(local_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        cloud_path = f"feature_store/metadata/{feature_group_id}.json"
        self.ops.upload_file_to_bucket(local_path, cloud_path)
        
        return local_path

    def grouping_features(self, blob, feature_group_id, name, description, source, usage):
        print(f"Processing feature group: {name}")
        
        parquet_bytes = self.ops.load_parquet_from_bucket(blob)
        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        
        metadata = self.metadata(
            feature_group_id=feature_group_id,
            feature_group_name=name,
            feature_group_description=description,
            source=source,
            usage=usage
        )
        
        feature_groups = {
            "id_and_metadata": [
                "id",
                "title",
                "originalTitle"
            ],
            "text_content_and_description": [
                "overview",
                "title_length",
                "word_count_overview",
                "unique_word_overview",
                "contains_award",
                "contains_action_words",
                "contains_romance_words",
                "contains_adventure_words",
                "contains_scifi_words",
                "contains_family_words",
                "contains_pet_words",
                "contains_drama_words",
                "contains_horror_words",
                "contains_teen_words"
            ],
            "cast_and_production": [
                "releaseYear",
                "releaseDecade",
                "genres",
                "directors",
                "cast",
                "cast_size",
                "directors_count",
                "is_classic",
                "is_recent",
                "years_since_release"
            ],
            "reviews_and_runtime": [
                "rating",
                "categoricalRating",
                "runtime",
                "rutimeCategories",
                "is_long_movie"
            ],
            "streaming_info": [
                "streaming_service_name",
                "streaming_type",
                "streaming_quality",
                "subtitles",
                "subtitles_count",
                "is_high_quality",
                "is_popular_service",
                "streaming_platform_type"
            ]
        }
        
        for group_name, columns in feature_groups.items():
            valid_columns = [col for col in columns if col in df.columns]
            
            if valid_columns:
                group_df = df[valid_columns]
                
                stats = self.calculate_dataframe_stats(group_df)
                
                for column in valid_columns:
                    feature_type = str(group_df[column].dtype)
                    self.add_feature_metadata(
                        feature_group_id=feature_group_id,
                        feature_name=column,
                        feature_type=feature_type,
                        description=f"Feature do grupo {group_name}",
                        stats=stats[column]
                    )
                
                version = self.version_control(f"{feature_group_id}_{group_name}", group_df)
                print(f"  - Grupo {group_name}: versão {version}, {len(group_df)} linhas, {len(valid_columns)} colunas")
                
                local_path = f"load/feature_store/{group_name}.parquet"
                group_df.to_parquet(local_path)
                
                cloud_path = f"feature_store/{feature_group_id}/{group_name}_v{version}.parquet"
                self.ops.upload_file_to_bucket(local_path, cloud_path)
        
        self.save_metadata(feature_group_id)
        
        return metadata