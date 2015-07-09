#include "test_helper.hpp"

int main(int argc, int argv) {
    MVDE* mvde = mapv_data_entry_rand(128, 128, 1, 10);
    mapv_data_entry_free(mvde, 1);

    return 0;
}
