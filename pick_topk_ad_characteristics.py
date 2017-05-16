# -*-coding:utf-8 -*-
import pandas as pd
from os import path

input_dir = "../input"
output_dir = "cache/"
promo_filename = "promoted_content.csv.gz"
ad_types = ["campaign", "advertiser"]
doc_files = {"topic": "documents_topics.csv.gz",
            "entity": "documents_entities.csv.gz",
            "category": "documents_categories.csv.gz"}
top_k = 5


if __name__ == "__main__":
    pr = pd.read_csv(path.join(input_dir, promo_filename), compression="gzip")
    df_dict = {}
    for doc_type in doc_files:
        df_dict[doc_type] = pd.read_csv(path.join(input_dir, doc_files[doc_type]),
            compression="gzip")
        tmp = pd.merge(pr, df_dict[doc_type], on='document_id')

        for ad_type in ad_types:
            print("Generating {}_top{}_{}.csv.gz".format(ad_type, top_k, doc_type))
            tmp2 = tmp.loc[:, ("{}_id".format(ad_type), "{}_id".format(doc_type), "confidence_level")].sort_values(
                ["{}_id".format(ad_type),"confidence_level"], ascending=[True, False])
            tmp2 = tmp2.loc[~tmp2.loc[:, ("{}_id".format(ad_type), "{}_id".format(doc_type))].duplicated()]
            tmp2.groupby("{}_id".format(ad_type)).apply(
                lambda tbl: tbl.sort_values("confidence_level", ascending=False
                ).iloc[:top_k, :2]).to_csv(
                path.join(output_dir, "{}_top{}_{}.csv.gz".format(ad_type, top_k, doc_type)),
                compression="gzip", index=False)
