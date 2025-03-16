from gcs import CloudStorageOps
import gcs
from importlib import reload
import pandas as pd
import io
import ast
import warnings

warnings.filterwarnings("ignore")
reload(gcs)

print("|||| Pipeline iniciado ||||")
ops = CloudStorageOps("streaming-data-for-ml")

parquet_data = [blob.name for blob in ops.storage_client.list_blobs(ops.bucket_name) if blob.name.endswith(".parquet")]

dfs = []
for f in parquet_data:
    print(f"Downloading: {f}")
    parquet_bytes = ops.load_parquet_from_bucket(f)
    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    dfs.append(df)

data = pd.concat(dfs, ignore_index=True)
print("Dados concatenados")

for_ml = [
    "id",
    "title",
    "overview",
    "releaseYear",
    "originalTitle",
    "genres",
    "directors",
    "cast",
    "rating",
    "runtime",
    "streaming_service_name",
    "streaming_type",
    "streaming_quality",
    "subtitles"
]

data_for_ml = data[for_ml]

decade_interval_bins = [1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020, 2030]
decade_labels = ["1910s", "1920s", "1930s", "1940s", "1950s", "1960s", "1970s", "1980s", "1990s", "2000s", "2010s", "2020s"]

data_for_ml["releaseDecade"] = pd.cut(
    data_for_ml["releaseYear"],
    bins=decade_interval_bins,
    labels=decade_labels,
    right=False
)

rating_interval_bins = [0, 20, 50, 70, 80, 90, 100]
rating_labels = ["Very poor", "Bad", "Regular", "Good", "Very Good", "Splendous"]

data_for_ml["categoricalRating"] = pd.cut(
    data_for_ml["rating"],
    bins=rating_interval_bins,
    labels=rating_labels,
    right=False
)

runtime_movies_bins = [0, 30, 40, 70, 100_000]
runtime_movies_categories = ["Not a film", "Short film", "Medium-length film", "Feature film"]

data_for_ml["runtimeCategories"] = pd.cut(
    data_for_ml["runtime"],
    bins=runtime_movies_bins,
    labels=runtime_movies_categories,
    right=False
)

data_for_ml["id"] = data_for_ml["id"].astype("int32")
data_for_ml["releaseDecade"] = data_for_ml["releaseDecade"].astype("object")
data_for_ml["categoricalRating"] = data_for_ml["categoricalRating"].astype("object")
data_for_ml["runtimeCategories"] = data_for_ml["runtimeCategories"].astype("object")

def clean_list_string(list_string):
    """
    Converte uma string que representa uma lista em uma lista Python e remove aspas.
    """
    try:
        list_obj = ast.literal_eval(list_string)
        if isinstance(list_obj, list):
            return ", ".join(list_obj)
        else:
            return "" 
    except (ValueError, SyntaxError):
        return ""

data_for_ml["cast"] = data_for_ml["cast"].apply(clean_list_string)
data_for_ml["directors"] = data_for_ml["directors"].apply(clean_list_string)

data_for_ml["title_length"] = data_for_ml["title"].apply(len) # nlp
data_for_ml["overview_length"] = data_for_ml["overview"].apply(len) # nlp
data_for_ml["word_count_overview"] = data_for_ml["overview"].apply(lambda x: len(x.split())) # nlp
data_for_ml["cast_size"] = data_for_ml["cast"].apply(lambda x: len(x.split(", ")))
data_for_ml["directors_count"] = data_for_ml["directors"].apply(lambda x: len(x.split(", ")))
data_for_ml["genre_count"] = data_for_ml["genres"].apply(lambda x: len(x.split(", ")))
data_for_ml["subtitles_count"] = data_for_ml["subtitles"].apply(lambda x: len(x.split(", ")))
data_for_ml["is_classic"] = data_for_ml["releaseYear"] < 1980
data_for_ml["is_recent"] = data_for_ml["releaseYear"] > 2015
data_for_ml["is_long_movie"] = data_for_ml["runtime"] > 120
data_for_ml["contains_award"] = data_for_ml["overview"].str.contains("oscar|emmy|award", case=False, na=False)
data_for_ml["contains_action_words"] = data_for_ml["overview"].str.contains("explosion|race|shooting", case=False, na=False)
data_for_ml["contains_romance_words"] = data_for_ml["overview"].str.contains("love|romance|kiss", case=False, na=False)
data_for_ml["contains_adventure_words"] = data_for_ml["overview"].str.contains("adventure|exciting", case=False, na=False)
data_for_ml["contains_scifi_words"] = data_for_ml["overview"].str.contains("universe|galaxy|technology", case=False, na=False)
data_for_ml["contains_family_words"] = data_for_ml["overview"].str.contains("family|child", case=False, na=False)
data_for_ml["contains_pet_words"] = data_for_ml["overview"].str.contains("dog|cat|pet", case=False, na=False)
data_for_ml["contains_drama_words"] = data_for_ml["overview"].str.contains("drama|loneliness|depression", case=False, na=False)
data_for_ml["contains_horror_words"] = data_for_ml["overview"].str.contains("scary|suspenseful|killer", case=False, na=False)
data_for_ml["contains_teen_words"] = data_for_ml["overview"].str.contains("teenager|pop|friends|school|music", case=False, na=False)
data_for_ml["unique_word_count"] = data_for_ml["overview"].apply(lambda x: len(set(x.split())))
data_for_ml["years_since_release"] = 2025 - data_for_ml["releaseYear"]
data_for_ml["is_high_quality"] = data_for_ml["streaming_quality"].isin(["hd", "uhd"])
data_for_ml["is_popular_service"] = data_for_ml["streaming_service_name"].isin(["Netflix", "Disney+", "Prime Video", "Apple TV"])
data_for_ml["streaming_platform_type"] = data_for_ml["streaming_service_name"].apply(lambda x: "Premium" if x in ["Netflix", "Apple TV", "Disney+", "Prime Video"] else "Free" if x == "Pluto TV" else "Others")

data_for_ml.to_parquet("transform/data/movies.parquet")
print("\nDados salvos no repositório local.")

ops.upload_file_to_bucket(
    "transform/data/movies.parquet",
    "data/main_data.parquet"
)