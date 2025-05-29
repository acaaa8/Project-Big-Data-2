from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StringType
import os

# Initialize Flask app
app = Flask(__name__)

# Initialize Spark Session (needed to load models and create DataFrames)
# It's good practice to start SparkSession once when the app starts
try:
    spark = SparkSession.builder.appName("SentimentAPI").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Reduce verbosity of Spark logs
except Exception as e:
    print(f"Error initializing Spark: {e}")
    spark = None

# --- Load Models ---
# Define model paths
MODEL_BASE_PATH = "models" # Assuming 'models' folder is in the same directory as app.py
MODEL_PATHS = {
    "model1": os.path.join(MODEL_BASE_PATH, "model1"),
    "model2": os.path.join(MODEL_BASE_PATH, "model2"),
    "model3": os.path.join(MODEL_BASE_PATH, "model3")
}

loaded_models = {}

if spark:
    for name, path in MODEL_PATHS.items():
        try:
            if os.path.exists(path):
                print(f"Loading model: {name} from {path}")
                # Load the entire pipeline model
                loaded_models[name] = PipelineModel.load(path)
                print(f"Model {name} loaded successfully.")
            else:
                print(f"Model path for {name} not found: {path}")
                loaded_models[name] = None
        except Exception as e:
            print(f"Error loading model {name}: {e}")
            loaded_models[name] = None
else:
    print("Spark session not available. Models cannot be loaded.")

# --- Helper function to get sentiment label ---
# The StringIndexer during training created a 'label' column (numeric).
# We need to map this back to the original sentiment string if possible.
# This requires knowing the original labels used by StringIndexer.
# For simplicity, let's assume the labels are consistent and inspect a loaded model
# to find them if the model stores them. Otherwise, we might need to hardcode or retrieve them.

# Let's try to get labels from one of the models if available
# This is a bit of a hack; ideally, you'd save label mappings during training.
sentiment_labels = ["unknown_sentiment_0", "unknown_sentiment_1", "unknown_sentiment_2"] # Default
if spark and "model1" in loaded_models and loaded_models["model1"]:
    try:
        # Access the StringIndexer stage from the pipeline
        # The name 'stringindexer' depends on how it was named or its default position.
        # Your train_models.py used 'label_indexer' as the StringIndexer instance,
        # and its outputCol was "label".
        # The 'labels' attribute of StringIndexerModel holds the mapping.
        # Assuming 'label_indexer' was the first stage that produces 'label' or is identifiable.
        # A common way is to find the StringIndexerModel stage.
        string_indexer_stage_model = None
        for stage in loaded_models["model1"].stages:
            if hasattr(stage, "labels") and stage.getInputCol() == "Sentiment": # A way to identify the StringIndexer for 'Sentiment'
                string_indexer_stage_model = stage
                break
        
        if string_indexer_stage_model:
            sentiment_labels = string_indexer_stage_model.labels
            print(f"Retrieved sentiment labels from model1: {sentiment_labels}")
        else:
            print("Could not retrieve sentiment labels from model1 automatically. Using defaults.")
            print("You might need to explicitly define them based on your training data's unique sentiment values.")
            # Example if you know them: sentiment_labels = ["negative", "neutral", "positive"] assuming 0, 1, 2 mapping.
            # The order in `sentiment_labels` should correspond to the numeric index.
            # Your dataset has 'positive', 'negative', 'neutral'.
            # You'd need to check how StringIndexer ordered them. A common order is by frequency.
            # For now, let's hardcode a plausible order if auto-detection fails.
            # This needs to match the order StringIndexer used!
            # If your dataset has 'positive', 'negative', 'neutral', and StringIndexer maps them to 0, 1, 2
            # in that order, then sentiment_labels = ["positive", "negative", "neutral"]
            # Let's assume a common order for now, but VERIFY THIS from your data/model.
            sentiment_labels = ["negative", "neutral", "positive"] # Common alphabetical or frequency-based order
            print(f"Using hardcoded sentiment labels: {sentiment_labels}. PLEASE VERIFY THIS ORDER.")


    except Exception as e:
        print(f"Error getting labels from model: {e}. Using default labels.")
        # Fallback if dynamic retrieval fails, ensure it matches your StringIndexer's behavior
        sentiment_labels = ["negative", "neutral", "positive"] # Common alphabetical or frequency-based order
        print(f"Using hardcoded sentiment labels: {sentiment_labels}. PLEASE VERIFY THIS ORDER.")


def get_sentiment_from_prediction(prediction_value):
    try:
        # prediction_value will be a float like 0.0, 1.0, 2.0
        idx = int(prediction_value)
        if 0 <= idx < len(sentiment_labels):
            return sentiment_labels[idx]
        else:
            return "unknown_sentiment_index"
    except Exception:
        return "error_converting_prediction"

# --- API Endpoints ---
@app.route('/predict/<model_name>', methods=['POST'])
def predict_sentiment(model_name):
    if not spark:
        return jsonify({"error": "Spark session not available"}), 500

    if model_name not in loaded_models or loaded_models[model_name] is None:
        return jsonify({"error": f"Model {model_name} not found or failed to load"}), 404

    try:
        data = request.get_json()
        if not data or 'text' not in data:
            return jsonify({"error": "Input JSON must contain a 'text' field"}), 400

        comment_text = data['text']

        # Create a Spark DataFrame from the input text
        # The input column for the pipeline is "CommentText" (from train_models.py)
        schema = ["CommentText"]
        input_df = spark.createDataFrame([(comment_text,)], schema)

        # Get the specific model
        model = loaded_models[model_name]

        # Make prediction
        prediction_df = model.transform(input_df)
        
        # The prediction is in the 'prediction' column by default for classifiers
        predicted_sentiment_numeric = prediction_df.select("prediction").first()[0]
        predicted_sentiment_label = get_sentiment_from_prediction(predicted_sentiment_numeric)
        
        # You can also retrieve probabilities if needed and available
        # probabilities = prediction_df.select("probability").first()[0] # This is a DenseVector

        return jsonify({
            "model_used": model_name,
            "input_text": comment_text,
            "predicted_sentiment_numeric": predicted_sentiment_numeric,
            "predicted_sentiment_label": predicted_sentiment_label
            # "probabilities": str(probabilities) # Optionally return probabilities
        })

    except Exception as e:
        print(f"Error during prediction with {model_name}: {e}")
        return jsonify({"error": f"An error occurred during prediction: {str(e)}"}), 500

@app.route('/', methods=['GET'])
def home():
    return "Sentiment Analysis API is running! Use /predict/<model_name> with POST."

# --- Run the app ---
if __name__ == '__main__':
    # Make sure to run this in a way that Spark can be accessed if it's not a local setup
    # For local development:
    app.run(debug=True, host='0.0.0.0', port=5000)