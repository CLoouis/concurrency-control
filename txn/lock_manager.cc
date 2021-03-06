// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

// LockManager::~LockManager() {
//   // Cleanup lock_table_
//   for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
//     delete it->second;
//   }
// }

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // Kalau kosong buat deque nya dulu
  if(!lock_table_[key]){
    lock_table_[key] = new deque<LockRequest>();
  }
  // buat sebuah exclusive lock untuk txn
  LockRequest writelockrequest(EXCLUSIVE, txn);
  bool isEmpty = (lock_table_[key]->size() == 0);

  // masukin ke daftar request lock
  lock_table_[key]->push_back(writelockrequest);

  // kalau awalnya kosong berarti langsung granted, kalau ngga wait
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
  Txn* TxnLockPertamaSebelumHapus;
  // Kalau kosong buat dequenya dulu
  if(!lock_table_[key]){
    lock_table_[key] = new deque<LockRequest>();
  }

  // cek txn yang lagi pegang resource key
  if(lock_table_[key]->size() != 0){
    TxnLockPertamaSebelumHapus = lock_table_[key]->front().txn_;
  }

  // hapus txn dari antrian request terhadap key
  std::deque<LockRequest>::iterator it;
  for (it = lock_table_[key]->begin(); it != lock_table_[key]->end();++it){
    if (it->txn_ == txn){
      lock_table_[key]->erase(it);
      break;
    }
  }

  // cek setelah dihapus yang lagi pegang nya sama ngga
  // kalau ga sama berarti txn pegang resourcenya
  bool isPertamaTerhapus = (TxnLockPertamaSebelumHapus != lock_table_[key]->front().txn_);

  // setelah txn keapus yang ngantri berikutnya dapet akses 
  // kalau ternyata transaction baru dpt semua resource jadi ready
  if (lock_table_[key]->size() != 0 && isPertamaTerhapus){
    Txn* requestBerikutnya = lock_table_[key]->front().txn_;
    if (txn_waits_[requestBerikutnya] == 1){
      txn_waits_[requestBerikutnya] -= 1;
      ready_txns_->push_back(requestBerikutnya);
      txn_waits_.erase(requestBerikutnya);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // Kalau antrian kosong berarti ga dilock siapapun
  // Kalau ngga kosong berarti ada 1 txn yang lagi pegang dan pasti exclusve
  // masukin ke vector terus return
  
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

