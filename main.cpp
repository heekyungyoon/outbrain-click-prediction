#include <fstream>
#include <iostream>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <map>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <thread>
#include <future>
#include "util/data.h"


typedef std::unordered_map<int, std::vector<int>> ad_characterstic_map;
typedef std::unordered_map<std::pair<int, int>, float, pairhash> user_topic_map;

ad_characterstic_map gen_ad_characteristic_map(
        std::string filename,
        bool is_entity
);
std::map<std::string, ad_characterstic_map> gen_ad_characteristic_map_set(
        std::string doc_construct
);
void gen_user_topic_map(
        int tid,
        user_topic_map *user_topic_map,
        std::string filename,
        int start_row,
        int end_row,
        std::unordered_map<int, std::vector<std::pair<int, float>>> *doc_topic_map,
        user_topic_map *user_topic_ref);
std::vector<user_topic_map> gen_user_topic_map_set(
        std::string doc_filename,
std::unordered_map<int, std::pair<int, int>> *display_map,
bool is_entity
);
void write_user_ad_interaction_on_topic(
        std::string doc_type,
        std::string doc_file,
        std::unordered_map<int, std::pair<int, int>> *display_map,
        std::unordered_map<int, ad> *ad_map
)


ad_characterstic_map gen_ad_characteristic_map(
        std::string filename,
        bool is_entity
)
{
    //read promoted_content
    ad_characterstic_map ad_characteristic_map;
    std::string id1;
    std::string id2;
    std::string others;

    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << "Start processing " << filename << std::endl;

    std::ifstream file(filename, std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    std::istream instream(&inbuf);
    std::getline(instream, others);
    std::cout << "  Headers: " << others << std::endl;

    //generate ad topic map set
    int row_count = 0; //processed rows
    if (is_entity == false) {
        while (std::getline(instream, id1, ',')) {
            std::getline(instream, id2);

            auto id1_value = ad_characteristic_map.find(stoi(id1));
            if (id1_value != ad_characteristic_map.end()) {
                id1_value->second.push_back(stoi(id2));
            } else {
                std::vector<int> v;
                v.push_back(stoi(id2));
                ad_characteristic_map.insert({stoi(id1), v});
            }
            ++row_count;
        }
    } else {
        while (std::getline(instream, id1, ',')) {
            std::getline(instream, id2);
            int entity_id = get_entity_id(id2);
            auto id1_value = ad_characteristic_map.find(stoi(id1));
            if (id1_value != ad_characteristic_map.end()) {
                id1_value->second.push_back(entity_id);
            } else {
                std::vector<int> v;
                v.push_back(entity_id);
                ad_characteristic_map.insert({stoi(id1), v});
            }
            ++row_count;
        }
    }
    //Cleanup

    std::cout << "\nrow_count = " << row_count
              << "\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;

    return ad_characteristic_map;
};


std::map<std::string, ad_characterstic_map> gen_ad_characteristic_map_set(
        std::string doc_construct
) {
    std::string top_k = "5";
    bool is_entity = false;
    std::map<std::string, ad_characterstic_map> ad_characteristic_map_set;
    std::string ad_constructs[] = {"campaign", "advertiser"};
    //std::string ad_constructs[] = {"advertiser"};
    if (doc_construct == "entity") {
        is_entity = true;
    }
    for (std::string &ad_c: ad_constructs) {
        std::cout << "Generating map for " << ad_c + doc_construct << std::endl;
        ad_characteristic_map_set[ad_c + doc_construct] = gen_ad_characteristic_map(
                "cache/" + ad_c + "_top" + top_k + "_" + doc_construct + ".csv.gz", is_entity);
    }
    return ad_characteristic_map_set;
};


void gen_user_topic_map(
        int tid,
        user_topic_map *user_topic_map,
        std::string filename,
        int start_row,
        int end_row,
        std::unordered_map<int, std::vector<std::pair<int, float>>> *doc_topic_map,
        user_topic_map *user_topic_ref)
{
    std::string uuid;
    std::string document_id;
    std::string others;

    // I. calculate user-topic interaction based on page_views
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << tid << "Start processing " << filename << std::endl;

    std::ifstream file(filename, std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    std::istream instream(&inbuf);

    // transform to unordered map
    std::getline(instream, others);
    std::cout << "  Headers: " << others << std::endl;

    // skip rows until start row
    int i = 0; //all rows
    while(i < start_row - 1) {
        std::getline(instream, others);
        ++i;
    }
    // start processing
    int row_count = 0; //processed rows
    while(std::getline(instream, uuid, ',') && i < end_row) {
        std::getline(instream, document_id, ',');
        std::getline(instream, others);

        auto user = uuid_map.find(uuid);  // convert string uuid to int uid to save memory
        // if the document has topics associated with it
        auto document = (*doc_topic_map).find(stoi(document_id));
        if (user != uuid_map.end() && document != (*doc_topic_map).end()) {
            for (auto &t: document->second) {
                //if user topic exists in the reference
                auto user_topic = (*user_topic_ref).find(std::make_pair(user->second, t.first));
                if (user_topic != (*user_topic_ref).end()) {
                    auto user_topic2 = (*user_topic_map).find(std::make_pair(user->second, t.first));
                    if (user_topic2 != (*user_topic_map).end()) {
                        // if user topic exists in the map
                        user_topic2->second += t.second;
                    } else {
                        // if not
                        (*user_topic_map).insert({std::make_pair(user->second, t.first), t.second});
                    }

                }
            }
        }
        if (i % 10000000 == 0) {
            std::cout << "[" <<start_row << "]" << i/10000000 << "0M...";
            std::cout.flush();
        }
        ++row_count;
        ++i;
    }
    //Cleanup
    file.close();

    std::cout << "\nrow_count = " << row_count <<" (" << start_row << " - " << end_row << ")"
              << "\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;
}



std::vector<user_topic_map> gen_user_topic_map_set(
        std::string doc_filename,
        std::unordered_map<int, std::pair<int, int>> *display_map,
        bool is_entity
)
{
    // 1. generate document topic map
    // <document_id, <topic_id, confidence_level>>
    std::unordered_map<int, std::vector<std::pair<int, float>>> doc_topic_map = gen_doc_topic_map(doc_filename, is_entity);

    // 2. generate user topic reference map
    // <display_id, <uuid, document_id>>
    user_topic_map user_topic_ref = gen_user_topic_ref(
            display_map, &doc_topic_map);

    // 3. user topic map set
    std::vector<user_topic_map> user_topic_map_set;
    std::string filename = "../input/page_views.csv.gz";
    //std::string filename = "../input/page_views_sample.csv.gz";

    unsigned int num_thread = 5;
    int num_row = 2034275448/num_thread + 1; //406,855,090

    //init user_topic_map
    for (int i = 0; i < num_thread; ++i) {
        user_topic_map user_topic_map;
        user_topic_map_set.push_back(user_topic_map);
    }

    //start thread
    std::vector<std::thread> thread_list;
    for (int i = 0; i < num_thread; ++i) {
    thread_list.push_back(std::thread(gen_user_topic_map,
                                      i,
                                      &user_topic_map_set[i],
                                      filename,
                                      (i * num_row + 1),
                                      ((1 + i) * num_row),
                                      &doc_topic_map,
                                      &user_topic_ref));
    }

    //finish thread
    for (auto &t: thread_list) {
        t.join();
    }
    return user_topic_map_set;
}


int calc_user_ad_interaction_topic(
        std::string doc_type,
        std::string ad_type,
        std::string click_file,
        int (*get_key) (ad),
        std::map<std::string, ad_characterstic_map> *ad_topic_map_set,
        std::vector<user_topic_map> *user_topic_map_set,
        std::unordered_map<int, ad> *ad_map,
        std::unordered_map<int, std::pair<int, int>> *display_map
)
{
    std::string outfile_name = "cache/clicks_" + click_file + "_user_"+ ad_type +"_interaction_on_" + doc_type + ".csv.gz";
    // read clicks_train
    std::string filename = "../input/clicks_" + click_file + ".csv.gz";
    std::string display_id;
    std::string ad_id;
    std::string others;
    ad_characterstic_map vec = (*ad_topic_map_set)[ad_type+doc_type];
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    std::cout << "Start generating " << outfile_name << std::endl;

    std::ifstream test_file(filename, std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> test_inbuf;
    test_inbuf.push(boost::iostreams::gzip_decompressor());
    test_inbuf.push(test_file);
    std::istream test_instream(&test_inbuf);

    // transform to unordered map
    std::getline(test_instream, others);
    std::cout << "  Headers: " << others << std::endl;

    // write interaction weights
    std::ofstream outfile(outfile_name, std::ios_base::out | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::output> out;
    out.push(boost::iostreams::gzip_compressor());
    out.push(outfile);
    std::ostream outstream(&out);

    outstream << "weight\n";

    // for row
    // read clicks_train row
    // save interaction to separate file
    int i = 0;
    while(std::getline(test_instream, display_id, ',')) {
        std::getline(test_instream, ad_id);
        //calculate weight
        float weight = 0.0;
        // if uuid and document id related to the display_id exists
        auto display = (*display_map).find(stoi(display_id));
        if (display != (*display_map).end()) {
            // if ad_id related exists
            auto ad = (*ad_map).find(stoi(ad_id));//
            if (ad != (*ad_map).end()) {
                // if topic id related to the document id exists
                auto ad_topic= vec.find((*get_key)(ad->second));
                if (ad_topic != vec.end()) {
                    for (auto &t: ad_topic->second) {
                        // if topic id related to the user id exists
                        for (auto &ut_map: (*user_topic_map_set)) {
                            auto user_topic = ut_map.find(std::make_pair(display->second.first, t));
                            if (user_topic != ut_map.end()) {
                                weight += user_topic->second;
                                //std::cout << "+ " << user_topic->second << " => " << adv_weight << std::endl;
                            }
                        }
                    }
                }
            }

        }
        outstream << weight <<"\n";

        if (i % 1000000 == 0) {
            std::cout << i / 1000000 << "M...";
            std::cout.flush();
        }
        ++i;
    }

    test_file.close();

    std::cout << "\ni = " << i <<"\nTime taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds>
                      (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;
    return 0;
}


void write_user_ad_interaction_on_topic(
        std::string doc_type,
        std::string doc_file,
        std::unordered_map<int, std::pair<int, int>> *display_map,
        std::unordered_map<int, ad> *ad_map
){
    bool is_entity = false;
    if (doc_type == "entity") {
        is_entity = true;
    }
    // I. Read file
    // <advertiser_id, <topic_id, confidence_level>>
    std::map<std::string, ad_characterstic_map> ad_char_set = gen_ad_characteristic_map_set(
            doc_type);

    // <<uuid, topic_id>, sum_confidence_level>
    std::vector<user_topic_map> user_topic_map_set = gen_user_topic_map_set(
            doc_file, display_map, is_entity);

    // II. calculate user-document interaction in terms of topic
    calc_user_ad_interaction_topic(doc_type, "advertiser", "train", get_adv_id, &ad_char_set, &user_topic_map_set, ad_map, display_map);
    calc_user_ad_interaction_topic(doc_type, "campaign", "train", get_camp_id, &ad_char_set, &user_topic_map_set, ad_map, display_map);
    calc_user_ad_interaction_topic(doc_type, "advertiser", "test", get_adv_id, &ad_char_set, &user_topic_map_set, ad_map, display_map);
    calc_user_ad_interaction_topic(doc_type, "campaign", "test", get_camp_id, &ad_char_set, &user_topic_map_set, ad_map, display_map);
}


int main() {
    std::map<std::string, std::string> doc_files = {
            {"topic", "../input/documents_topics.csv.gz"},
            {"entity", "../input/documents_entities.csv.gz"},
            {"category", "../input/documents_categories.csv.gz"}};

    // <display id, <uuid, document_id>>
    std::unordered_map<int, std::pair<int, int>> display_map = gen_display_map();
    // <ad_id, ad>
    std::unordered_map<int, ad> ad_map = gen_ad_map();

    ////user ad interaction on topic
    write_user_ad_interaction_on_topic("topic", doc_files["topic"], &display_map, &ad_map);
    ////user ad interaction on entity
    write_user_ad_interaction_on_topic("entity", doc_files["entity"], &display_map, &ad_map);
    ////user ad interaction on category
    write_user_ad_interaction_on_topic("category", doc_files["category"], &display_map, &ad_map);

    return 0;
}