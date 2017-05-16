#!/usr/bin/python3
# -*- coding:utf-8 -*-
from os import path
import pandas as pd
import subprocess

version = "1_2"

input_path = "../input"
pred_path = "pred/"
submit_path = "submit/"

testid = pd.read_csv(path.join(input_path, 'clicks_test.csv.gz'), compression="gzip")
pred = pd.read_csv(path.join(pred_path, 'pred{}'.format(version)), header=None)
pred.columns = ["pred"]

result = pd.concat([testid, pred], axis=1)
result = result.sort_values(['display_id','pred'], ascending=[True, False])

def process_ad_id(table):
    cutoff = 0.0
    return ' '.join(str(v) if v >= cutoff else '' for v in table.loc[:,'ad_id'].iloc[:12]).strip()


submission = result.groupby('display_id').apply(lambda t: process_ad_id(t))
submission.to_csv(path.join(submit_path, 'submission{}.csv'.format(version)), index=True, index_label='display_id', header=['ad_id'])

cmd = 'gsutil cp {} gs://yhk00323/outbrain/submission/'.format(path.join(submit_path, 'submission{}.csv'.format(version)))
subprocess.call(cmd, shell=True)