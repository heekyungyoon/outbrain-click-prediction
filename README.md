# outbrain-click-prediction

## Data
Kaggle Outbrain click prediction challenge: [https://www.kaggle.com/c/outbrain-click-prediction/data](https://www.kaggle.com/c/outbrain-click-prediction/data)  

The problem is about predicting which display ads are going to be clicked. 
There are two major challenges. First, how to handle the massive data sets? page_views.csv alone is 80G with 2bil rows. Second, there are 9 data sets without clear explanation on relationship. Figuring out how to connect dots between those data sets is another major challenge.  

## Field Aware Factorization Machine(FFM)  
A state-of-the-art technology in recommendation was used. To be more specific, [libffm](https://github.com/guestwalk/libffm) was customized to include linear weights in order to account for features that interaction with other features is not so meaningful.  

FFM is a recommendation system that incorporates contextual data along with user and item data. In basic matrix factorization, only a single set of latent factors exist per feature, however there exists multiple sets of latent factors in FFM. One set per field. (Features are grouped by fields they belong to.) 

## Structure

Generate interaction feature  
- pick_topk_ad_characteristics.py  
- main.cpp  
- normalize_interaction.py  

Transform data into ffm format  
- gen_ffm_format.py  

Train & Predict  
- libffm (customized to incorporate linear weights)

Make submission file
- make_submission.py  


## Quick-start
```$ g++ -std=c++11 main.cpp data.h -o main -lboost_iostreams -lpthread```

Move to libffm directory:  
```$ make```  

Come back to parent directory:  
```$ python run.py```
