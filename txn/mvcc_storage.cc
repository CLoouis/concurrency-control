

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
  
  // bool found = false;
  // int largest_timestamp;
  // deque<Version*>::iterator it;
  // deque<Version*> *data = mvcc_data_[key];
  
  // // Iterate
  // for (it = data->begin(); it != data->end();it++){

  //   // find the largest write timestamp less than or equal to txn_unique_id
  //   if ((*it)->version_id_ <= txn_unique_id){

  //     if(found==false){
  //       largest_timestamp = (*it)->version_id_;
  //       *result = (*it)->value_;
  //       found = true;

  //     }

  //     if(largest_timestamp < (*it)->version_id_){
  //       *result = (*it)->value_;
  //       largest_timestamp = (*it)->version_id_;

  //     }

  //   } 
  // }

  // if(found) {

  //   (*it)->max_read_id_ = txn_unique_id;

  // }
  
  // return found;
  return true;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  //
  // Implement this method!

  // deque<Version*>* data = mvcc_data_[key];
  // deque<Version*>:: iterator it;

  // it = data->end();
  // if(((*it)->version_id_ < txn_unique_id) && ((*it)->max_read_id_ < txn_unique_id)){
    
  //   return true;

  // } else {

  //   return false;
  // }
  return true;
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  // Note that the performance would be much better if you organize the versions in decreasing order.
  // if(!CheckWrite(key,txn_unique_id)){
    

  //   return;

  // }
  // Version versi;
  // versi.value_ = value;
  // versi.max_read_id_ = txn_unique_id;
  // versi.version_id_ = txn_unique_id;
  // mvcc_data_[key]->push_back(&versi);
  
}