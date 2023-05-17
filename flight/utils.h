#include <fstream>


void WriteToFile(std::string data, std::string path, bool append) {
    std::ofstream file;
    if (append) {
        file.open(path, std::ios_base::app);
    } else {
        file.open(path);
    }
    file << data;
    file.close();
}


std::pair<std::string, std::string> SplitRequest(std::string request) {
    std::string delimiter = "@";
    std::string path = request.substr(0, request.find(delimiter));
    std::string query = request.substr(request.find(delimiter) + 1, request.length());
    return std::make_pair(path, query);
}
