#ifndef HELPERS_H
#define HELPERS_H

#include <chrono>
#include <iostream>

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

#endif //HELPERS_H
