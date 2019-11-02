#include "mvcc_storage.h"
#include <iostream>


using namespace std;




int main(){

    MVCCStorage* storage = new MVCCStorage();
    storage->InitStorage();
    Value v;
    for (int i = 0; i < 10000;i++) {
        if(storage->Read(i, &v, 0)){
            cout << v << endl;
        }

    }
    return 0;
}