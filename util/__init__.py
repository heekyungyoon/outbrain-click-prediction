from os import path
import gzip
import csv
import hashlib

input_path = "../input"
cache_path = "cache/"


def hashindex(field, index, nr_bins=int(1e+6)):
    string = "{}{}{}".format(field, "_", index)
    return int(hashlib.md5(str(string).encode('utf8')).hexdigest(), 16)%(nr_bins-1)+1


class CSVReader(object):
    def __init__(self, filename):
        self.file = gzip.open(filename, "rt", newline="")
        self.reader = csv.reader(self.file)
        self.header = next(self.reader)

    def get_header(self):
        return self.header

    def get_iter(self):
        return self.reader

    def close(self):
        self.file.close()


def gen_doc_single_char(filename, is_entity):
    single_dict = {}
    reader = CSVReader(path.join(cache_path, filename))
    print("Header: {}".format(reader.get_header()))
    if is_entity is False:
        for row in reader.get_iter():
            single_dict[int(row[0])] = int(row[1])
    else:
        for row in reader.get_iter():
            single_dict[int(row[0])] = int(hashlib.md5(str(row[1]).encode('utf8')).hexdigest(), 16)
    reader.close()
    return single_dict


def gen_ad_map():
    # ad_id: (campaign_id, advertiser_id)
    ad_dict = {}
    filename = "promoted_content.csv.gz"
    reader = CSVReader(path.join(input_path, filename))
    print("Header: {}".format(reader.get_header()))
    for row in reader.get_iter():
        ad_dict[int(row[0])] = (int(row[2]), int(row[3]))
    reader.close()
    return ad_dict


def extract_meta(id):
    if id.isdigit():
        return int(id)
    else:
        return 0


def gen_doc_meta_map():
    # document_id: (source_id, publisher_id)
    doc_meta_map = {}
    filename = "documents_meta.csv.gz"
    reader = CSVReader(path.join(input_path, filename))
    print("Header: {}".format(reader.get_header()))
    for row in reader.get_iter():
        doc_meta_map[int(row[0])] = (extract_meta(row[1]), extract_meta(row[2]))
    reader.close()
    return doc_meta_map


def transform_to_ffm(clicked, feature):
    items = [str(clicked)]
    for ft in feature:
        items.append('{0}:{1}:{2}'.format(ft[0], hashindex(ft[0], ft[1]), ft[2]))
    return ' '.join(items) + '\n'
