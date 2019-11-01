// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)


#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);
  
  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }
  
  storage_->InitStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);       
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);  
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));
  
}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
    
  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  // selama tp active ambil satu txn
  // baca readset nya, kalau read lakukan read kalau write lakukan write
  // kalau ga granted, jalankan ulang txn
  // kalau semua granted masukin ready_txn
  // cek completed txn, kalau completed_c apply write terus status = commit
  // kalau completed_a status = aborted
  // release semua key yang dipegang
  // masuk ke txn_result
  // terus txn - txn berikutnya dijalanin (?)
  Txn* transaction;
  set<Key> readLockTelahDidapat;
  set<Key> writeLockTelahDidapat;

  while(tp_.Active()){
    if(txn_requests_.Pop(&transaction)){
      bool isGranted = true;
      
      readLockTelahDidapat.clear();
      writeLockTelahDidapat.clear();

      for(Key key : transaction->readset_){
        if(!lm_->ReadLock(transaction, key)){
          isGranted = false;
          readLockTelahDidapat.insert(key);
          if(transaction->readset_.size() + transaction->writeset_.size() > 1){
            for(Key key : readLockTelahDidapat){
              lm_->Release(transaction, key);
            }
          }
          break;
        }
        readLockTelahDidapat.insert(key);
      }

      if (isGranted){
        for (Key key : transaction->writeset_){
          if(!lm_->WriteLock(transaction, key)){
            isGranted = false;
            writeLockTelahDidapat.insert(key);
            if(transaction->readset_.size() + transaction->writeset_.size() > 1){
              for(Key key : readLockTelahDidapat){
                lm_->Release(transaction, key);
              }

              for(Key key : writeLockTelahDidapat){
                lm_->Release(transaction, key);
              }
            }
            break;
          }
          writeLockTelahDidapat.insert(key);
        }
      }

      if(isGranted == true){
        ready_txns_.push_back(transaction);
      } else if (isGranted == false && (transaction->writeset_.size() + transaction->readset_.size() > 1)) {
        transaction->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(transaction);
      }
    }

    while (completed_txns_.Pop(&transaction)){
      if(transaction->Status() == COMPLETED_C){
        ApplyWrites(transaction);
        transaction->status_ = COMMITTED;
      } else if (transaction->Status() == COMPLETED_A){
        transaction->status_ = ABORTED;
      } else {
        DIE("ERROR");
      }

      for(Key key : transaction->readset_){
        lm_->Release(transaction, key);
      }

      for(Key key : transaction->writeset_){
        lm_->Release(transaction, key);
      }

      txn_results_.Push(transaction);
    }

    while(ready_txns_.size()){
      transaction = ready_txns_.front();
      ready_txns_.pop_front();
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            transaction));
    }
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler() {
  // CPSC 438/538:
  
  // Implement this method!
  
  while (tp_.Active()) {
    Txn *next;
    // Get the next new transaction request (if one is pending) and pass it to an execution thread.
    if(txn_requests_.Pop(&next)){
      // Deal with all transactions that have finished running (see below).
      // }
      // In the execution thread (we are providing you this code):
      
      // Record start time
      next->occ_start_time_= GetTime();
      //   Perform "read phase" of transaction:
      //      Read all relevant data from storage
      //      Execute the transaction logic (i.e. call Run() on the transaction)
      Task *task = new Method<TxnProcessor, void, Txn*>(this,&TxnProcessor::ExecuteTxn,next);
      tp_.RunTask(task);
    }

    // Dealing with a finished transaction (you must write this code):
    Txn *transaksiSelesai;
    while(completed_txns_.Pop(&transaksiSelesai)){
      bool valid = true;
      std::set<Key>::iterator it;
      for (it = transaksiSelesai->writeset_.begin(); it !=  transaksiSelesai->writeset_.end(); ++it)
      {
        //if (the record was last updated AFTER this transaction's start time) {
        if(transaksiSelesai->occ_start_time_ < storage_->Timestamp(*it)){
        //Validation fails!
          valid = false;
          break;
        }
      }

      for (it = transaksiSelesai->readset_.begin(); it != transaksiSelesai->readset_.end  (); ++it)
      {
        //if (the record was last updated AFTER this transaction's start time) {
        if(transaksiSelesai->occ_start_time_ < storage_->Timestamp(*it)){
        //Validation fails!
          valid = false;
          break;
        }
      }
      
      
      if (transaksiSelesai->Status() == COMPLETED_A) {
        transaksiSelesai->status_ = ABORTED;
      }else if (transaksiSelesai->Status() == COMPLETED_C){
        // Validation phase:
        // for (each record whose key appears in the txn's read and write sets) {
        //   }
        // }

        // Commit/restart
        // if (validation failed) {
        if (!valid){
          // Cleanup txn
          // Completely restart the transaction.

          // Cleanup txn:
          transaksiSelesai->reads_.empty();
          transaksiSelesai->writes_.empty();
          transaksiSelesai->status_ = INCOMPLETE;

          // Restart txn:
          mutex_.Lock();
          transaksiSelesai->unique_id_ = next_unique_id_;
          next_unique_id_++;
          txn_requests_.Push(transaksiSelesai);
          mutex_.Unlock();

          continue;
        }else{
          // Apply all writes
          ApplyWrites(transaksiSelesai);
          // Mark transaction as committed
          transaksiSelesai->status_ = COMMITTED;
        }       
      }else{
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << transaksiSelesai->Status());
      }
      txn_results_.Push(transaksiSelesai);
    }
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  }
  // RunSerialScheduler();
}

void TxnProcessor::RunOCCParallelScheduler() {
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // while (tp_.Active()) {
  //   Txn *txn;
  //   MVCCStorage str;
  //   str.InitStorage();
  //   if (txn_requests_.Pop(&txn)) {

  //     tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
  //                 this,
  //                 &TxnProcessor::ExecuteTxn,
  //                 txn));
      
      
  //     set <Key> :: iterator it;
  //     for(it = txn->writeset_.begin() ; it!= txn->writeset_.end() ; ++it){
  //       (*lm_).WriteLock(txn,(*it));
  //       if(str.CheckWrite(*it, txn->unique_id_)){
  //         str.Write(*it, , )
  //       }

  //     }
  //   }
  // }

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute. 
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn. 
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  //RunSerialScheduler();

  // Code according to pseudocode
  Txn* txn;
  while (tp_.Active()) {

    // Get next txn request.

    if (txn_requests_.Pop(&txn)) {
      
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
        this,
        &TxnProcessor::MVCCExecuteTxn,
        txn));
    }
  }
}



void TxnProcessor::MVCCExecuteTxn(Txn* txn){
  
  // This is the function for the thread running the transaction
  
  // First, read all necessary data for this transaction
  // Read the data from read_set_keys

  set<Key>::iterator it;
  Value value;
  for (it = txn->readset_.begin(); it!= txn->readset_.end(); it++){

    // Lock
    storage_->Lock(*it);

    // Read
    if(storage_->Read(*it, &value,txn->unique_id_)){
      // if successful
      txn->reads_[*it] = value;

    }

    // Release the lock
    storage_->Unlock(*it);
  }

  // Execute the transaction logic (i.e. call Run() on the transaction)
  txn->Run();
    
  // Acquire all locks for keys in the write_set_
  for (it = txn->writeset_.begin(); it!= txn->writeset_.end(); it++){
    storage_->Lock(*it);

  }

  //Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  if(MVCCCheckWrites(txn)){

    // Apply the writes
    ApplyWrites(txn);
    // Release locks in writeset
    for(it = txn->writeset_.begin(); it!= txn->writeset_.end(); it++){
      storage_->Unlock(*it);
    }

    // Commit
    txn->status_ = COMMITTED;

    // Return ownership to processor thread
    txn_results_.Push(txn);


  } else {

    // A fail occured !
    for(it = txn->writeset_.begin(); it!= txn->writeset_.end(); it++){
      storage_->Unlock(*it);
    }

    // Cleanup
    txn->reads_.empty();
    txn->writes_.empty();
    txn->status_ = INCOMPLETE;

    // Restart
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock(); 

  }
}
  
bool TxnProcessor::MVCCCheckWrites(Txn* txn){

  // Check if all write can be done with the transaction
  // Check from txn's write_set, iterate
  set<Key>::iterator it;

  // iterate
  for(it = txn->writeset_.begin(); it!= txn->writeset_.end(); it++){
    if(!storage_->CheckWrite(*it,txn->unique_id_)){

      // Write cannot be done
      // immediately return false
      return false;

    }
  }

  // All has been checked
  // Write can be done, hooray !
  return true;
}

void TxnProcessor::MVCCLockWriteKeys(Txn* txn){

  // Assume that write can be done (e.g. validated)
  // Lock all keys in txn's writeset
  set<Key>::iterator it;
  
  //iterate
  for(it = txn->writeset_.begin(); it != txn->writeset_.end(); it++){
    storage_->Lock(*it);
  }

}

void TxnProcessor::MVCCUnlockWriteKeys(Txn* txn){

  // Release all mutex locks on txn's writeset keys
  set<Key>::iterator it;
  for(it = txn->writeset_.begin(); it != txn->writeset_.end(); it++){
    storage_->Unlock(*it);
  }
}

