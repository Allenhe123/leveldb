#include <cassert>
#include <iostream>
#include <string>
#include <cstdlib>

#include "../include/leveldb/db.h"
#include "../include/leveldb/write_batch.h"

int main()
{
    leveldb::DB* db = nullptr;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status;

    std::string key1{"key1"};
    std::string key2{"key2"};
    std::string val1 = "leveldb1", val;
    status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    if (!status.ok())  std::cout << status.ToString() << std::endl;

    status = db->Put(leveldb::WriteOptions(), key1, val1);
    if (!status.ok())  std::cout << status.ToString() << std::endl;

    status = db->Get(leveldb::ReadOptions(), key1, &val);
    if (!status.ok())  std::cout << status.ToString() << std::endl;
    else std::cout << "Get val: " << val << std::endl;

//WriteBatch是一系列对数据库的更新操作，并且这些批量操作之间有一定的顺序性。
//撇开writebatch带来的原子性优势，writebatch也能通过把多个更新放在一个批量操里面来加速操作。
    leveldb::WriteBatch batch;
    batch.Delete(key1);
    batch.Put(key2, val);
    status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) std::cout << status.ToString() << std::endl;

    status = db->Delete(leveldb::WriteOptions(), key1);
    if (!status.ok())  std::cout << status.ToString() << std::endl;

    status = db->Get(leveldb::ReadOptions(), key1, &val);
    if (!status.ok())  std::cout << status.ToString() << std::endl;
    else std::cout << "Get val: " << val << std::endl;

/*
通常情况下，所有的leveldb写操作都是异步的：当leveldb把写操作交个操作系统之后就返回。
从操作系统内存到硬盘等持久性存储是异步的。如果在写的时候打开同步写选项，那么只有当数据持久化到硬盘之后才会返回。
(On Posix systems, this is implemented by calling either fsync(...) or fdatasync(...) or 
msync(..., MS_SYNC) before the write operation returns.)
异步写通常比同步写快1000倍以上。异步写的不足就是当机器宕机时会丢失最后更新的数据。写进程的异常退出并不会造成数据的丢失。
通常情况下异步写能够被妥善的处理。例如，当你在网数据库写大量的数据时，在机器宕机之后能通过重新写一次数据来修复。
混合使用同步和异步也是可以的。例如每N次写做一次同步。当机器宕机的时候，只需要重新写最后一次同步写之后的数据。
同步写一个新增一个标记来记录上一次同步写的位置。
WriteBatch是一个异步写。一个WriteBatch内部的多个更新操作放在一起也可以使用同步写操作，(i.e., write_options.sync 
is set to true). 可以通过批量操作降低同步写的消耗。

https://blog.csdn.net/doc_sgl/article/details/52824426
*/
    leveldb::WriteOptions write_options;
    write_options.sync = true;
    db->Put(write_options, "key3", "leveldb3");
    status = db->Get(leveldb::ReadOptions(), "key3", &val);
    if (!status.ok()) std::cout << status.ToString() << std::endl;
    else std::cout << val << std::endl;

    delete db;
    db = nullptr;

    return 0;
}
