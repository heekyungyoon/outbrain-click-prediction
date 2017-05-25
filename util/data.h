#ifndef DATA_H
#define DATA_H

#include <fstream>
#include <iostream>
#include <unordered_map>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <map>
#include <unordered_map>
#include <vector>
#include <chrono>


struct pairhash {
public:
    template <typename T, typename U>
    std::size_t operator()(const std::pair<T, U> &x) const
    {
        return std::hash<T>()(x.first) ^ std::hash<U>()(x.second);
    }
};


struct ad {
    int document_id;
    int campaign_id;
    int advertiser_id;
};


int get_camp_id(ad ad) {
    return ad.campaign_id;
}


int get_adv_id(ad ad) {
    return ad.advertiser_id;
}


typedef std::unordered_map<int, std::vector<int>> ad_characterstic_map;
typedef std::unordered_map<std::pair<int, int>, float, pairhash> user_characteristic_map;
typedef std::unordered_map<int, std::vector<std::pair<int, float>>> document_topic_map;
typedef std::unordered_map<int, std::pair<int, int>> display_map;
typedef std::unordered_map<int, ad> ad_map;


class Timer {
private:
    std::chrono::steady_clock::time_point begin;
public:
    Timer() {
        begin = std::chrono::steady_clock::now();
    }
    void finish() {
        std::cout << "Time taken (sec): "
                  << std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now() - begin).count()
                  << "\n"
                  << std::endl;
    }
};


class IdMap {
public:
    std::unordered_map<std::string, int> map;
    int get_id(std::string &uuid) {
        int uid;
        auto pair = map.find(uuid);
        if (pair != map.end()) {
            uid = pair->second;
        } else {
            uid = map.size();
            map.insert(make_pair(uuid, uid));
        }
        return uid;
    }

    std::unordered_map<std::string, int>* data() {
        return &map;
    };

};


display_map gen_display_map(IdMap &uuid_map)
{
    // read events to get uuid and document id from clicks_train
    display_map display_map;
    std::string filename = "../input/events.csv.gz";
    std::string display_id;
    std::string uuid;
    std::string document_id;
    std::string others;

    Timer tmr;
    std::cout << "Start processing " << filename << std::endl;

    std::ifstream file(filename, std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    std::istream instream(&inbuf);

    std::getline(instream, others);
    std::cout << "  Headers: " << others << std::endl;

    int i = 0; //rows
    while(std::getline(instream, display_id, ',')) {
        std::getline(instream, uuid, ',');
        std::getline(instream, document_id, ',');
        std::getline(instream, others);
        int uid = uuid_map.get_id(uuid);

        //insert all display ids to display map
        display_map.insert({stoi(display_id), std::make_pair(uid, stoi(document_id))});

        if (i % 1000000 == 0)
            std::cout << i/1000000 << "M...";
        std::cout.flush();
        ++i;
    }

    file.close();

    std::cout << "\ni = " << i << std::endl;
    tmr.finish();

    return display_map;
}


document_topic_map gen_doc_topic_map(
        std::string filename,
        bool is_entity,
        IdMap &entity_map
)
{
    document_topic_map doc_topic;
    std::string document_id;
    std::string topic_id;
    std::string confidence_level;
    std::string others;

    Timer tmr;
    std::cout << "Start processing " << filename << std::endl;

    std::ifstream topic_file(filename, std::ios_base::in | std::ios_base::binary);
    boost::iostreams::filtering_streambuf<boost::iostreams::input> topic_inbuf;
    topic_inbuf.push(boost::iostreams::gzip_decompressor());
    topic_inbuf.push(topic_file);
    std::istream topic_instream(&topic_inbuf);

    std::getline(topic_instream, others);
    std::cout << "  Headers: " << others << std::endl;

    // transform to unordered map
    int i = 0;
    if (is_entity == false) {
        while(std::getline(topic_instream, document_id, ',')) {
            std::getline(topic_instream, topic_id, ',');
            std::getline(topic_instream, confidence_level);

            auto item = doc_topic.find(stoi(document_id));
            if (item != doc_topic.end()) {
                item->second.push_back(std::make_pair(stoi(topic_id), stof(confidence_level)));
            } else {
                std::vector<std::pair<int, float>> v;
                v.push_back(std::make_pair(stoi(topic_id), stof(confidence_level)));
                doc_topic.insert({stoi(document_id), v});
            }
            ++i;
        }
    } else {
        while(std::getline(topic_instream, document_id, ',')) {
            std::getline(topic_instream, topic_id, ',');
            std::getline(topic_instream, confidence_level);
            int entity_id = entity_map.get_id(topic_id);

            auto item = doc_topic.find(stoi(document_id));
            if (item != doc_topic.end()) {
                item->second.push_back(std::make_pair(entity_id, stof(confidence_level)));
            } else {
                std::vector<std::pair<int, float>> v;
                v.push_back(std::make_pair(entity_id, stof(confidence_level)));
                doc_topic.insert({stoi(document_id), v});
            }
            ++i;
        }
    }


    topic_file.close();

    std::cout << "\ni = " << i << std::endl;
    tmr.finish();

    return doc_topic;
};


user_characteristic_map gen_user_topic_ref(
        display_map *display_map,
        document_topic_map *doc_topic_map)
{
    // read events to get uuid and document id from clicks_train
    user_characteristic_map user_topic_ref;

    Timer tmr;
    std::cout << "Start generating user topic reference map " << std::endl;

    int i = 0; //rows
    for(auto &display: *display_map) {
        //save all user-topic pair associated with display ids in events.csv (afterwards, won't process what's not in it)
        //if the document has topics associated with it
        auto document = (*doc_topic_map).find(display.second.second);
        if (document != (*doc_topic_map).end()) {
            for (auto &t: document->second) {
                //if user topic doesn't exist
                auto user_topic = user_topic_ref.find(std::make_pair(display.second.first, t.first));
                if (user_topic == user_topic_ref.end()) {
                    user_topic_ref.insert({std::make_pair(display.second.first, t.first), 0});
                }
            }
        }

        if (i % 1000000 == 0)
            std::cout << i/1000000 << "M...";
        std::cout.flush();
        ++i;
    }

    std::cout << "\ni = " << i << std::endl;
    tmr.finish();

    return user_topic_ref;
}



ad_map gen_ad_map()
{
    //read promoted_content
    ad_map ad_map;

    std::string filename = "../input/promoted_content.csv.gz";
    std::string ad_id;
    std::string campaign_id;
    std::string advertiser_id;
    std::string document_id;
    std::string others;

    Timer tmr;
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
    while(std::getline(instream, ad_id, ',')) {
        std::getline(instream, document_id, ',');
        std::getline(instream, campaign_id, ',');
        std::getline(instream, advertiser_id);

        ad promoted_ad;
        promoted_ad.document_id = stoi(document_id);
        promoted_ad.campaign_id = stoi(campaign_id);
        promoted_ad.advertiser_id = stoi(advertiser_id);
        ad_map.insert({stoi(ad_id), promoted_ad});
        ++row_count;
    }

    //Cleanup
    file.close();

    std::cout << "\nrow_count = " << row_count << std::endl;
    tmr.finish();

    return ad_map;
};

#endif

