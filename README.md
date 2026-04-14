# AniVerse - Anime Hybrid Recommender

A hybrid recommendation system for anime using collaborative filtering (Matrix Factorization) and deep learning (Two-Tower BPR models). Built with PyTorch and optimized for implicit feedback and weighted ratings.

## Dataset Introduction
This dataset contains information about 17.562 anime and the preference from 325.772 different users. In particular, this dataset contain:

- The anime list per user. Include dropped, complete, plan to watch, currently watching and on hold.
- Ratings given by users to the animes that they has watched completely.
- Information about the anime like genre, stats, studio, etc.
- HTML with anime information to do data scrapping. These files contain information such as reviews, synopsis, information about the staff, anime statistics, genre, etc.

Also, the dataset is available at kaggle: https://www.kaggle.com/hernan4444/anime-recommendation-database-2020

## 📊 Dataset Documentation

### 🗂️ Raw Datasets

| File | Description |
|------|-------------|
| `watching_status.csv` | Users' watching behavior |
| `animelist.csv` | User–anime interactions (ratings) |
| `scraped_anime.csv` | Anime reviews |
| `anime_with_synopsis.csv` | Synopsis text |
| `anime.csv` | Anime related data |
| `anime_metadata_ready.csv` | Anime metadata |

### 🔧 Intermediary Datasets

#### Data Processing & Integration

| File | Description |
|------|-------------|
| `anime_complete.csv` | Combined metadata + synopsis into one anime-level dataset |
| `animelist_final_sampled.csv` | Sampled version of user–anime interactions |
| `user_id_map.json` | User ID mapping for modeling |
| `anime_id_map.json` | Anime ID mapping for modeling |

#### Dataset Splits (For Modeling)

| File | Description |
|------|-------------|
| `train.csv`, `val.csv`, `test.csv` | Basic splits with ratings |
| `train_complete.csv`, `val_complete.csv`, `test_complete.csv` | Splits + watching_status features |

#### Feature Engineering Outputs

| File | Description |
|------|-------------|
| `anime_complete_encoded.csv` | Engineered features (numerical / encoded) |

#### Embeddings (Text Representations)

| File | Description |
|------|-------------|
| `synopsis_bert.npy` | BERT embeddings from synopsis |
| `review_bert_embeddings.npy` | BERT embeddings from reviews |

#### Final Anime Features

| File | Description |
|------|-------------|
| `MASTER_ANIME_TOWER_FEATURES_1BASED.npy` | Final anime feature representation used for modeling, combining metadata and text embeddings (synopsis and reviews) |

---



