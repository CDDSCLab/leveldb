#include <stdio.h>
#include "leveldb/db.h"

int main() {
  // open db
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::WriteOptions writeoptions;
  leveldb::ReadOptions readoptions;
  leveldb::Status status;
  std::string name = "db";
  status = leveldb::DB::Open(options, name, &db);
  printf("open: %s\n", status.ToString().data());
  assert(status.ok());

  // put keys
  for (int i = 20; i < 50; i++) {
    std::string k1 = "KEY";
    k1.append(std::to_string(i));
    std::string v1 = "VALUE";
    v1.append(std::to_string(i));
    status = db->Put(writeoptions, k1, v1);
    assert(status.ok());
  }

  std::string oldfname = "old_range.dump";
  std::string newfname = "new_range.dump";

  // dump range
  status = db->DumpRange(oldfname, "KEY20", "KEY80");
  printf("dump old: %s\n", status.ToString().data());
  assert(status.ok());

  // 
  for (int i = 40; i < 50; i++) {
    std::string k1 = "KEY";
    k1.append(std::to_string(i));
    status = db->Delete(writeoptions, k1);
    assert(status.ok());
  }

  for (int i = 70; i < 80; i++) {
    std::string k1 = "KEY";
    k1.append(std::to_string(i));
    std::string v1 = "VALUE";
    v1.append(std::to_string(i));
    status = db->Put(writeoptions, k1, v1);
    assert(status.ok());
  }

  // dump range
  status = db->DumpRange(newfname, "KEY20", "KEY80");
  printf("dump new: %s\n", status.ToString().data());
  assert(status.ok());

  // Get a new db
  leveldb::DB* newdb;
  std::string newname = "newdb";
  status = leveldb::DB::Open(options, newname, &newdb);
  printf("open newdb: %s\n", status.ToString().data());
  assert(status.ok());

  for (int i = 20; i < 50; i++) {
    std::string k1 = "KEY";
    k1.append(std::to_string(i));
    std::string v1 = "VALUE";
    v1.append(std::to_string(i));
    status = newdb->Put(writeoptions, k1, v1);
    assert(status.ok());
  }

  status = newdb->IngestRanges(oldfname, newfname);
  printf("IngestRange: %s\n", status.ToString().data());
  assert(status.ok());

  leveldb::Iterator* iter = newdb->NewIterator(leveldb::ReadOptions());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    printf("Key: [%s]  Value: [%s] \n", iter->key().ToString().c_str(),
           iter->value().ToString().c_str());
  }
  return 0;
}
