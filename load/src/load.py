from feature_store import FeatureStore

fs = FeatureStore("streaming-data-for-ml")

metadata = fs.grouping_features(
    blob="data/main_data.parquet",
    feature_group_id="filmes_features",
    name="Features de Filmes",
    description="Conjunto de features extraídas de dados de filmes",
    source="API de filmes",
    usage="Recomendação de filmes e análise de sentimento"
)

print(metadata)