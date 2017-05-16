# -*-coding:utf-8 -*-
import pandas as pd
from os import path

dir = "cache/"
ad_files = {"ad_type": ["advertiser", "campaign"],
            "doc_type": ["topic", "entity", "category"]}

if __name__ == "__main__":
    for tr_te in ["train", "test"]:
        for ad_type in ad_files['ad_type']:
            for doc_type in ad_files['doc_type']:
                infile = path.join(dir, "clicks_{}_user_{}_interaction_on_{}.csv.gz".format(tr_te, ad_type, doc_type))
                outfile = path.join(dir, "clicks_{}_user_{}_interaction_on_{}_normalized.csv.gz".format(tr_te, ad_type, doc_type))
                print("Processing {}".format(infile))
                data = pd.read_csv(infile, compression="gzip")
                mx = float(data.weight.max())
                print("max value: {}".format(mx))
                data.loc[:, "weight"] = data.loc[:, "weight"].apply(lambda w: w/mx)
                data.to_csv(outfile, index=False, compression="gzip")
                del data
                print("Finished. Result file: {}".format(outfile))
