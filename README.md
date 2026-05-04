# Personalized Recipe Recommendation System

**Course:** CS-GY 6513 Big Data  
**Professor:** Amit Patel  
**Semester:** Spring 2026  
**Team:** Bias & Variance  
**Members:** Sharvin Gavad (`sg9469`), Mahima Mariah (`mx2431`), Shashwat Shah (`sns10089`)

## Overview

This project builds a personalized recipe recommendation system using the Food.com Recipes and User Interactions dataset. The pipeline uses PySpark for large-scale preprocessing, Spark MLlib ALS for collaborative filtering, Parquet for processed storage, and a streaming-style feedback demo that simulates Kafka ingestion in a Colab-safe way.

The final implementation is designed around two execution modes:

- **Colab demo mode:** reliable live-demo path using local PySpark in Google Colab, local output storage, and in-memory Kafka-style event simulation.
- **Cluster mode:** optional architecture-complete path with HDFS storage and real Kafka Structured Streaming support.

## Main Files

- `Recipe_Recommendation_Big_Data_Final_Implementation.ipynb`: final demo-safe notebook implementation
- `Recipe Recommendation Big Data (1).ipynb`: original working notebook
- `BigData_Proposal.pdf`: original project proposal

## Dataset

- **Dataset:** Food.com Recipes and User Interactions
- **Source:** Kaggle
- **URL:** <https://www.kaggle.com/datasets/shuyangli94/food-com-recipes-and-user-interactions>
- **Files used:**
  - `RAW_recipes.csv`
  - `RAW_interactions.csv`

## What the Notebook Does

The final notebook covers the full recommendation workflow:

1. Safe setup for Google Colab or cluster execution
2. Kaggle credential handling without printing secrets
3. Robust CSV loading for multiline quoted recipe data
4. Spark-based EDA and large-scale aggregation
5. Feature engineering using recipe metadata such as cooking time, tags, and nutrition presence
6. Sparse user and item filtering for stronger ALS training
7. Parquet persistence of processed data
8. ALS collaborative filtering with Spark MLlib
9. Evaluation using RMSE and MAE
10. Top-10 recommendation generation for trained users
11. Model persistence and metrics export
12. Streaming-style feedback ingestion using an in-memory queue
13. Optional real Kafka and HDFS support in cluster mode

## Quick Start: Google Colab

This is the recommended way to run the project.

### 1. Open the notebook in Colab

Upload `Recipe_Recommendation_Big_Data_Final_Implementation.ipynb` to Google Colab.

### 2. Keep the default execution settings

At the top of the notebook, use:

```python
RUN_MODE = "colab"
USE_REAL_KAFKA = False
```

### 3. Provide the dataset

Use either of the following:

- Upload `RAW_recipes.csv` and `RAW_interactions.csv` into `/content/foodcom`, or
- Let the notebook download the dataset from Kaggle

### 4. Kaggle credential options

The notebook supports two secure options:

- Set `KAGGLE_USERNAME` and `KAGGLE_KEY` as environment variables, or
- Upload `kaggle.json` when prompted in Colab

The notebook does **not** print secrets.

### 5. Run all cells

Use **Runtime > Run all** in Colab.

No GPU is required.

### 6. Outputs

In Colab mode, outputs are saved to:

```text
/content/recipe_recommender_outputs
```

This includes:

- saved ALS model
- processed ratings Parquet
- processed recipe features Parquet
- metrics JSON
- sample recommendation CSV

## Optional Cluster Mode

Cluster mode is included for architecture completeness and production-style deployment.

Use:

```python
RUN_MODE = "cluster"
USE_REAL_KAFKA = True  # only if Kafka is actually available
```

Cluster mode expects:

- HDFS paths for raw, processed, model, and output storage
- Spark runtime with Kafka connector support
- an available Kafka broker

Configured paths in the notebook:

```python
HDFS_BASE_DIR = "hdfs:///user/bigdata/recipe_recommender"
HDFS_RAW_DIR = f"{HDFS_BASE_DIR}/raw"
HDFS_PROCESSED_DIR = f"{HDFS_BASE_DIR}/processed"
HDFS_MODEL_DIR = f"{HDFS_BASE_DIR}/models/als_model"
HDFS_OUTPUT_DIR = f"{HDFS_BASE_DIR}/outputs"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "recipe_ratings"
```

## Architecture Notes

The original proposal included Apache Kafka and HDFS as active pipeline components. The final implementation preserves both through cluster mode, but the live demo uses Colab mode for reliability. In Colab, Kafka-style streaming is simulated with an in-memory queue and outputs are written locally instead of to HDFS.

This was an environment-aware implementation choice, not a change in the core system design.

## Model Configuration

The final ALS model uses:

- `rank = 50`
- `maxIter = 10`
- `regParam = 0.1`
- `coldStartStrategy = "drop"`
- `nonnegative = True`
- `seed = 42`

Filtering before training:

- keep users with at least 5 ratings
- keep recipes with at least 5 ratings

## Final Results Summary

Results below are from the final notebook run:

| Metric | Value |
| --- | --- |
| Raw recipe rows | 231,637 |
| Raw interaction rows | 1,132,367 |
| Recipes after cleaning | 231,636 |
| Valid interactions for analysis | 1,071,520 |
| Filtered ratings rows | 586,159 |
| Filtered unique users | 21,973 |
| Filtered unique recipes | 50,716 |
| User-recipe matrix sparsity | 99.997589% |
| ALS rank | 50 |
| RMSE | 0.6477 |
| MAE | 0.4685 |
| Top-N recommendations generated for trained users | 218,950 |
| Streaming events consumed in demo | 10 |

## Why the Top-N Count Is Lower Than `21,973 x 10`

The filtered dataset contains `21,973` users, but the recommendation count is `218,950`, which equals `21,895 x 10`. This is expected.

The ALS model generates recommendations for users present in the trained model factors. Because the model is fit on `train_df` after a random train/test split, a small number of filtered users may not appear in training. In the final run:

- filtered users: `21,973`
- trained users in `model.userFactors`: `21,895`
- recommendations generated: `21,895 x 10 = 218,950`

So the notebook logic is correct. The count reflects recommendations for **trained users**, not every filtered user in the pre-split dataset.

## Repository Usage Notes

- The notebook is the primary deliverable and is intended to run top-to-bottom.
- Colab mode is the recommended demo path.
- Cluster mode is included to show how Kafka and HDFS fit into the production architecture.
- Final evaluation in the report and notebook uses **RMSE** and **MAE**. Precision@10 is intentionally excluded from the final demo/reporting.

## Future Improvements

- Add hybrid recommendation logic that combines ALS with content-based recipe features
- Tune ALS hyperparameters more systematically
- Run cluster mode on a real distributed environment such as Dataproc or EMR
- Add scheduled retraining over newly ingested feedback events
- Build a lightweight API or front-end for interactive recommendations

## References

- Food.com dataset: <https://www.kaggle.com/datasets/shuyangli94/food-com-recipes-and-user-interactions>
- Original proposal: `BigData_Proposal.pdf`
- Final notebook: `Recipe_Recommendation_Big_Data_Final_Implementation.ipynb`
