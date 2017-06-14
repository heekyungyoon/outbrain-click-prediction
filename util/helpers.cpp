#include "helpers.h"

Timer::Timer() {
    begin = std::chrono::steady_clock::now();
}

void Timer::finish() {
    std::cout << "Time taken (sec): "
              << std::chrono::duration_cast<std::chrono::seconds> (std::chrono::steady_clock::now() - begin).count()
              << "\n"
              << std::endl;
}




CsvGzReader::CsvGzReader(std::string filename) {
    boost::iostreams::file_source file(filename, std::ios_base::in | std::ios_base::binary);
    inbuf.push(boost::iostreams::gzip_decompressor());
    inbuf.push(file);
    std::getline(inbuf, header);
    std::cout << "HEADER: "<< header << std::endl;
}

bool CsvGzReader::getline(std::string *line, char delimeter) {
    if (std::getline(inbuf, *line, delimeter)) {
        return true;
    } else {
        return false;
    }
}

CsvGzReader::operator bool() {
    return !inbuf.eof();
}

