#!/usr/bin/env python3

import subprocess
import time

data_version = 1
model_version = 2

start = time.time()

# Generate interaction feature
cmd = "python pick_topk_ad_characteristics.py"
subprocess.call(cmd, shell=True)

cmd = "./main"
subprocess.call(cmd, shell=True)

cmd = "python normalize_interaction.py"
subprocess.call(cmd, shell=True)

# Transform data into ffm format
cmd = "python gen_ffm_format.py"
subprocess.call(cmd, shell=True)



# Train
cmd = 'cd libffm/ && \
./ffm-train -p ../output/va{data_version}.ffm --auto-stop ../output/tr{data_version}.ffm ../model/model{data_version}_{model_version}'.format(data_version=data_version, model_version=model_version)
subprocess.call(cmd, shell=True)

# Predict
cmd = './ffm-train ../output/test{data_version}.ffm ../model/model{data_version}_{model_version} ../pred/pred{data_version}_{model_version}'.format(data_version=data_version, model_version=model_version)
subprocess.call(cmd, shell=True)



# Make submission file
cmd = 'python make_submission.py {data_version} {model_version}'.format(data_version=data_version, model_version=model_version)

# Finish
print('Total time taken = {0:.0f}'.format(time.time()-start))
