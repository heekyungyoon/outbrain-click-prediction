#ifndef HELPERS_H
#define HELPERS_H

#include <chrono>
#include <iostream>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file.hpp>

struct pairhash {
public:
    template <typename T, typename U>
    std::size_t operator()(const std::pair<T, U> &x) const
    {
        return std::hash<T>()(x.first) ^ std::hash<U>()(x.second);
    }
};


class Timer {
private:
    std::chrono::steady_clock::time_point begin;
public:
    Timer();
    void finish();
};


class CsvGzReader {
public:
    boost::iostreams::filtering_istream inbuf;
    std::string header;

    CsvGzReader(std::string filename);
    bool getline(std::string *line, char delimeter = '\n');
    operator bool();
};

#endif //HELPERS_H
