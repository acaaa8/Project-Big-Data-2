# train_models.py

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import os

# Buat Spark session (bisa lokal atau cluster)
spark = SparkSession.builder.appName("Train Sentiment Model").getOrCreate()

# Baca semua batch
batches = ["batch1.csv", "batch2.csv", "batch3.csv"]
dfs = [spark.read.option("header", True).csv(batch) for batch in batches]

# Gabungkan batch bertahap
combinations = [
    (dfs[0], "model1"),
    (dfs[0].union(dfs[1]), "model2"),
    (dfs[0].union(dfs[1]).union(dfs[2]), "model3")
]

# Pastikan folder models ada
os.makedirs("models", exist_ok=True)

for df, model_name in combinations:
    print(f"Training {model_name}...")

    # Preprocessing
    df = df.select("CommentText", "Sentiment").na.drop()

    # Konversi label ke angka
    label_indexer = StringIndexer(inputCol="Sentiment", outputCol="label")

    tokenizer = Tokenizer(inputCol="CommentText", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)

    # Pipeline
    pipeline = Pipeline(stages=[label_indexer, tokenizer, remover, hashingTF, idf, lr])

    # Train model
    model = pipeline.fit(df)

    # Simpan model
    model_path = f"models/{model_name}"
    model.write().overwrite().save(model_path)

    print(f"{model_name} saved to {model_path}")

spark.stop()