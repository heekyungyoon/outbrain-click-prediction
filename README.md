# outbrain-click-prediction

## Structure

Generate interaction feature  
- pick_topk_ad_characteristics.py  
- main.cpp  
- normalize_interaction.py  

Transform data into ffm format  
- gen_ffm_format.py  

Train & Predict  
- libffm

Make submission file
- make_submission.py  


## Quick-start
```$ g++ -std=c++11 main.cpp data.h -o main -lboost_iostreams -lpthread```

Move to libffm directory:  
```$ make```  

Come back to parent directory:  
```$ python run.py```
