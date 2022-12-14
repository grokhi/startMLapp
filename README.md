# Recommendation service
Modification of Start ML final project by karpov.courses

* base_model.ipynb - catboost classification with text features extracted from TF-IDF representation with PCA decomposition
* enhanced_model.ipynb - catboost classification with text features extracted from BERT representation with DEC-aug clusterization
* to test application simply run test_app.py

<!-- # Key features:
1. Downloading chunks of data from **PostgresSQL** database using **FastAPI**
2. Content-based recommendation system model was trained using **catboost** and text-derived features using **distilbert-based sentence transformer**
3. **HitRate@5** = 0.595 -->

# Intro

Recently I found an interesting [paper by Subakti et al. (2022)](https://journalofbigdata.springeropen.com/articles/10.1186/s40537-022-00564-9), which relates to the problem of BERT embeddings clusterization. In paper, there are several proposed strategies: proper BERT embeddings handling (pooling&normalization) combined with different techniques, such as KMeans, DEC (Deep Embedded Clustering), IDEC (Improved DEC), fuzzy C-Means. I tried to play with DEC&IDEC because they had the best performance (see Subakti's paper). Unfortunately, there was no acceptable pytorch implementation, so I rewrote existing solutions and applied rewritten DEC and IDEC for current task. *See [my DEC/IDEC_mnist repo](https://github.com/grokhi/pytorch_DEC_IDEC_2022) for details*

# Results
- Improved Deep Embedded Clustering (IDEC) trained on augmented dataset (backtranslation augmentation using 'bert_multilingual_uncased') showed **93%** cluster accuracy (b/line kmeans **58%**). Extracted features were used for catboost training
- Unfortunately, default catboost model (**.65**) showed less ROC-AUC score than in base model (**.67**). Need to tune it
- Tuned catboost results: #TODO.

What's next? Obtained results from base_model and enhanced_model will be validated using scheduling A/B tests on a local server. #abtests #airflow #docker #postgresql *See airflow/dags folder for details. To run dag, simply build docker image in the folder and make docker-compose*
TODO: make dags refactoring
