// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  if(!lock_table_[key]){
    lock_table_[key] = new deque<LockRequest>();
  }

  LockRequest writelockrequest(EXCLUSIVE, txn);
  bool isEmpty = (lock_table_[key]->size() == 0);
  lock_table_[key]->push_back(writelockrequest);
  if(!isEmpty){
    txn_waits_[txn] += 1;
  }
  return isEmpty;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  if(!lock_table_[key]){
    lock_table_[key] = new deque<LockRequest>();
  }

  for (std::deque<LockRequest>::iterator it = lock_table_[key]->begin(); it != lock_table_[key]->end(); it++){
    if (it->txn_ == txn){
      lock_table_[key]->erase(it);
      if (lock_table_[key]->size() != 0){
        Txn* requestBerikutnya = lock_table_[key]->front().txn_;
        if (txn_waits_[requestBerikutnya] == 1){
          ready_txns_->push_back(requestBerikutnya);
          txn_waits_.erase(requestBerikutnya);
        }
      }
    } else {
      //karena txn ga ngapa-ngapain di key do nothing
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  if(lock_table_[key]->size() == 0){
    return UNLOCKED;
  } else {
    LockRequest pemegangKeySaatIni = lock_table_[key]->front();
    vector<Txn*> pemilikKey;
    pemilikKey.push_back(pemegangKeySaatIni.txn_);
    *owners = pemilikKey;
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  return UNLOCKED;
}

