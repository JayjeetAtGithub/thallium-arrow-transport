#include "server.h"

int main(int argc, char **argv) {
    if (argc < 3) {
        std::cout << "./ts [selectivity] [backend]" << std::endl;
        exit(1);
    }

    std::string selectivity = argv[1];
    std::string backend = argv[2];

    auto server = new ThalliumTransportService(backend, selectivity);
    server->Init();
    std::cout << "Listening on uri " << server->uri() << std::endl;
    server->Serve();
}
