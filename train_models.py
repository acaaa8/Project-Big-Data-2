from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import os
import json
import random

def balance_classes(df, label_col="Sentiment"):
    # Hitung jumlah per kelas
    counts = df.groupBy(label_col).count().collect()
    class_counts = {row[label_col]: row['count'] for row in counts}
    min_count = min(class_counts.values())
    
    balanced_dfs = []
    for cls, count in class_counts.items():
        cls_df = df.filter(col(label_col) == cls)
        # Downsample ke min_count
        fraction = min_count / count
        # Sampel secara acak
        sampled_df = cls_df.sample(withReplacement=False, fraction=fraction, seed=42)
        balanced_dfs.append(sampled_df)
    # Gabungkan kembali jadi dataframe seimbang
    return balanced_dfs[0].unionAll(balanced_dfs[1]).unionAll(balanced_dfs[2])

# Buat Spark session
spark = SparkSession.builder.appName("Train Sentiment Model").getOrCreate()

# Baca semua batch
batches = ["batches/batch1.csv", "batches/batch2.csv", "batches/batch3.csv"]
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

    # Preprocessing: filter label hanya 3 kelas valid dan drop null
    df = df.select("CommentText", "Sentiment").na.drop()
    df = df.filter(df.Sentiment.isin(["Positive", "Negative", "Neutral"]))

    # Balance kelas supaya seimbang
    df_balanced = balance_classes(df, label_col="Sentiment")
    print(f"Balanced class counts for {model_name}:")
    df_balanced.groupBy("Sentiment").count().show()

    # NLP Pipeline
    label_indexer = StringIndexer(inputCol="Sentiment", outputCol="label")
    tokenizer = Tokenizer(inputCol="CommentText", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    count_vec = CountVectorizer(inputCol="filtered", outputCol="rawFeatures", vocabSize=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)

    pipeline = Pipeline(stages=[label_indexer, tokenizer, remover, count_vec, idf, lr])
    model = pipeline.fit(df_balanced)

    # Simpan model
    model_path = f"models/{model_name}"
    model.write().overwrite().save(model_path)
    print(f"{model_name} saved to {model_path}")

    # Simpan label mapping
    labels = model.stages[0].labels  # StringIndexer labels
    label_file = f"models/{model_name}_labels.json"
    with open(label_file, "w") as f:
        json.dump(labels, f)
    print(f"Label mapping saved to {label_file}")

spark.stop()
