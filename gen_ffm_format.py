# -*-coding:utf-8 -*-
from os import path
import gzip
import csv
from datetime import datetime
from util import *


input_path = "../input"
cache_path = "cache/"
output_path = "output/"

#input_path = "../outbrain_data"
#cache_path = "cache/"


def convert_timestamp(timestamp_ms_relative):
    TIMESTAMP_DELTA = 1465876799998
    return datetime.fromtimestamp((int(timestamp_ms_relative) + TIMESTAMP_DELTA) // 1000)


def extract_platform(platform):
    if platform.isdigit():
        return int(platform)
    else:
        return 0


def extract_country(geo_loc):
    if geo_loc:
        return geo_loc[:2]
    else:
        return None


def extract_us_state(geo_loc):
    if geo_loc and geo_loc[:2] == 'US' and len(geo_loc) > 2:
        return geo_loc[3:5]
    else:
        return None


def gen_event_dict():
    #event_dict = {disply_id: [document_id, day, weekday, hour, platform, country, us_state]}
    event_dict = {}
    filename = "events.csv.gz"
    reader = CSVReader(path.join(input_path, filename))
    print("Header: {}".format(reader.get_header()))
    i = 0
    for row in reader.get_iter():
        timestamp = convert_timestamp(row[3])
        country = extract_country(row[5])
        us_state = extract_us_state(row[5])
        platform = extract_platform(row[4])
        event_dict[int(row[0])] = [int(row[2]), timestamp.day, timestamp.weekday(), timestamp.hour,
                                   platform, country, us_state]
        if i % 1000000 == 0:
            print("{}M...".format(i/1000000))
        i += 1
    reader.close()
    return event_dict


def ffm_format(version, tr_te, click_file, ad_files, event_dict, single_dicts, ad_map, doc_meta_map):
    t1 = datetime.utcnow()
    outfile_name = path.join(output_path, "{}{}.ffm".format(tr_te, version))
    outfile = open(outfile_name, 'w')

    click_reader = CSVReader(path.join(input_path, click_file))
    files = []
    for ad_type in ad_files['ad_type']:
        for doc_type in ad_files['doc_type']:
            files.append(
                CSVReader(path.join(
                    cache_path,
                    "clicks_{}_user_{}_interaction_on_{}_normalized.csv.gz".format(tr_te, ad_type, doc_type)
                ))
            )

    i = 0
    for row in zip(click_reader.get_iter(), files[0].get_iter(), files[1].get_iter(), files[2].get_iter(),
                   files[3].get_iter(), files[4].get_iter(), files[5].get_iter()):
        feature = list()

        display_id = int(row[0][0])
        ad_id = int(row[0][1])
        event = event_dict[display_id]
        document_id = int(event[0])

        ## display
        #feature.append([1, uuid, 1])
        feature.append([0, document_id, 1])    # document id
        feature.append([1, event[1], 1])    # day
        feature.append([2, event[2], 1])    # weekday
        feature.append([3, event[3], 1])    # hour
        feature.append([4, event[4], 1])    # platform
        feature.append([5, event[5], 1])    # country
        feature.append([6, event[6], 1])    # us state

        # documents
        feature.append([7, doc_meta_map[document_id][0], 1])    # source_id
        feature.append([8, doc_meta_map[document_id][1], 1])    # publisher_id

        tmp_id = single_dicts[0].get(document_id)
        if tmp_id is not None:
            feature.append([9, tmp_id, 1])    # single topic

        tmp_id = single_dicts[1].get(document_id)
        if tmp_id is not None:
            feature.append([10, tmp_id, 1])   # single entity

        tmp_id = single_dicts[2].get(document_id)
        if tmp_id is not None:
            feature.append([11, tmp_id, 1])    # single category

        ## ad
        feature.append([12, ad_map[ad_id][0], 1])   # campaign_id
        feature.append([13, ad_map[ad_id][1], 1])   # advertiser_id

        feature.append([14, 0, row[1][0]])
        feature.append([14, 1, row[2][0]])
        feature.append([14, 2, row[3][0]])
        feature.append([14, 3, row[4][0]])
        feature.append([14, 4, row[5][0]])
        feature.append([14, 5, row[6][0]])

        clicked = 0
        if tr_te == 'train':
            clicked = row[0][2]
        outfile.write(transform_to_ffm(clicked, feature))
        if i % 1000000 == 0:
            print("{}M...".format(i/1000000))
        i += 1

    outfile.close()
    click_reader.close()
    for f in files:
        f.close()
    print('row count: {}'.format(i))
    print('time taken: {}'.format(datetime.utcnow()-t1))



if __name__ == "__main__":
    version = 1
    #click_files = {"train": "clicks_train.csv.gz",
    #               "test": "clicks_test.csv.gz"}
    click_files = {"test": "clicks_test.csv.gz"}
    ad_files = {"ad_type": ["advertiser", "campaign"],
                "doc_type": ["topic","entity","category"]}

    event_dict = gen_event_dict()
    doc_single_dicts = list()
    for doc_type in ad_files["doc_type"]:
        is_entity = False
        if doc_type == "entity":
            is_entity = True

        doc_single_dicts.append(
            gen_doc_single_char(
                "document_single_{}.csv.gz".format(doc_type),
                is_entity))

    ad_map = gen_ad_map()
    doc_meta_map = gen_doc_meta_map()

    for click_f in click_files:
        ffm_format(version, click_f, click_files[click_f], ad_files, event_dict, doc_single_dicts, ad_map, doc_meta_map)