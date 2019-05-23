#include <stdio.h>
#include "leveldb/db.h"

int main()
{
    // open db
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::WriteOptions writeoptions;
    leveldb::ReadOptions readoptions;
    leveldb::Status status;
    std::string name = "db";
    status = leveldb::DB::Open(options, name, &db);
    printf("open: %s\n",status.ToString().data());
    assert(status.ok());
    
    // put keys
    for(int i=0;i<1000;i++){
        std::string k1="KEY";
        k1.append(std::to_string(i));
        std::string v1="VALUE";
        v1.append(std::to_string(i));
        status = db->Put(writeoptions, k1, v1);
        assert(status.ok());
    }
   
    std::string rfname = "range.dump";

    // dump table
    status = db->DumpRange(rfname, "KEY10", "KEY11");
    printf("dump: %s\n",status.ToString().data());
    assert(status.ok());

    // load table
    std::string start;
    std::string end;
    status = db->LoadRange("range.dump", &start, &end);
    printf("load: %s. Key range: [%s]-[%s]\n",status.ToString().data(), start.c_str(), end.c_str() );

    assert(status.ok());
    
    return 0;
}
