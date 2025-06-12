from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import os
import json

# Setup environment (optional, sesuaikan path Python kamu jika diperlukan)
os.environ["PYSPARK_PYTHON"] = r"C:\\Users\\ASUS\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\\Users\\ASUS\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

# Initialize Flask
app = Flask(__name__)

# Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName("SentimentStreamingAPI") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
except Exception as e:
    print(f"Error initializing Spark: {e}")
    spark = None

# Load label mappings
label_mappings = {}
for model_name in ["model1", "model2", "model3"]:
    label_file = f"models/{model_name}_labels.json"
    if os.path.exists(label_file):
        with open(label_file) as f:
            label_mappings[model_name] = json.load(f)

def decode_label(model_name, index):
    try:
        return label_mappings.get(model_name, [])[int(index)]
    except:
        return "unknown"

# Utility to load model safely
def load_model(model_name):
    model_path = f"models/{model_name}"
    if not os.path.exists(model_path):
        return None, jsonify({"error": f"{model_name} not found"}), 404
    return PipelineModel.load(model_path), None, None

# Endpoint 1: Prediksi label sentimen (model1)
@app.route('/predict_sentiment_label', methods=['POST'])
def predict_sentiment_label():
    if not spark:
        return jsonify({"error": "Spark not available"}), 500

    model, error_response, status = load_model("model3")
    if error_response:
        return error_response, status

    data = request.get_json()
    if not data or "text" not in data:
        return jsonify({"error": "Missing 'text' in request"}), 400

    df = spark.createDataFrame([(data["text"],)], ["CommentText"])
    result = model.transform(df)
    prediction = result.select("prediction").first()[0]
    label_text = decode_label("model1", prediction)

    return jsonify({
        "endpoint": "predict_sentiment_label",
        "input_text": data["text"],
        "predicted_class_index": prediction,
        "predicted_class_label": label_text
    })

# Endpoint 2: Prediksi + probabilitas kelas (model2)
@app.route('/predict_sentiment_probability', methods=['POST'])
def predict_sentiment_probability():
    if not spark:
        return jsonify({"error": "Spark not available"}), 500

    model, error_response, status = load_model("model2")
    if error_response:
        return error_response, status

    data = request.get_json()
    if not data or "text" not in data:
        return jsonify({"error": "Missing 'text' in request"}), 400

    df = spark.createDataFrame([(data["text"],)], ["CommentText"])
    result = model.transform(df)
    probs = result.select("probability").first()[0]
    pred = result.select("prediction").first()[0]

    labels = label_mappings.get("model2", [])
    prob_dict = {
        labels[i] if i < len(labels) else f"class_{i}": float(probs[i])
        for i in range(len(probs))
    }

    return jsonify({
        "endpoint": "predict_sentiment_probability",
        "input_text": data["text"],
        "predicted_class_index": pred,
        "predicted_class_label": decode_label("model2", pred),
        "class_probabilities": prob_dict
    })

# Endpoint 3: Ekstraksi keyword/token penting (model3)
@app.route('/extract_keywords', methods=['POST'])
def extract_keywords():
    if not spark:
        return jsonify({"error": "Spark not available"}), 500

    model, error_response, status = load_model("model1")
    if error_response:
        return error_response, status

    data = request.get_json()
    if not data or "text" not in data:
        return jsonify({"error": "Missing 'text' in request"}), 400

    df = spark.createDataFrame([(data["text"],)], ["CommentText"])

    tokenizer = model.stages[1]
    stopwords_remover = model.stages[2]

    tokenized = tokenizer.transform(df)
    filtered = stopwords_remover.transform(tokenized)

    tokens = filtered.select("filtered").first()[0]

    return jsonify({
        "endpoint": "extract_keywords",
        "input_text": data["text"],
        "filtered_tokens": tokens
    })

# Root endpoint
@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "message": "Sentiment Streaming API is running.",
        "endpoints": [
            "POST /predict_sentiment_label",
            "POST /predict_sentiment_probability",
            "POST /extract_keywords"
        ]
    })

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
