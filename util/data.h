#ifndef DATA_H
#define DATA_H

#include <map>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include "helpers.h"


class IdMap {
public:
    std::unordered_map<std::string, int> map;
    int get_id(std::string &uuid);
    std::unordered_map<std::string, int>* data();
};


struct ad {
    int document_id;
    int campaign_id;
    int advertiser_id;
};


int get_camp_id(ad ad);
int get_adv_id(ad ad);

// <ad_id, characteristic_id>    cf. characteristic_id = {topic_id, entity_id, category_id}
typedef std::unordered_map<int, std::vector<int>> ad_characteristic_map;
// <<uuid, characteristic_id>, sum_confidence_level>
typedef std::unordered_set<std::pair<int, int>, pairhash> user_characteristic_set;
// <<uuid, characteristic_id>, sum_confidence_level>
typedef std::unordered_map<std::pair<int, int>, float, pairhash> user_characteristic_map;
// <document_id, <topic_id, confidence_level>>
typedef std::unordered_map<int, std::vector<std::pair<int, float>>> document_topic_map;
// <display id, <uuid, document_id>>
typedef std::unordered_map<int, std::pair<int, int>> display_map;
// <ad_id, ad>
typedef std::unordered_map<int, ad> ad_map;


display_map gen_display_map(IdMap &uuid_map);
document_topic_map gen_doc_topic_map(std::string filename, bool is_entity, IdMap &entity_map);
user_characteristic_set gen_user_topic_ref(display_map *display_map, document_topic_map *doc_topic_map);
ad_map gen_ad_map();

#endif

