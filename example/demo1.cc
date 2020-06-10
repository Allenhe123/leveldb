#include <cassert>
#include <iostream>
#include <string>
#include <cstdlib>

#include "../include/leveldb/db.h"
#include "../include/leveldb/write_batch.h"
#include "../include/leveldb/comparator.h"

class TwoPartComparator : public leveldb::Comparator {
   public:
    // Three-way comparison function:
    //   if a < b: negative result
    //   if a > b: positive result
    //   else: zero result
    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
      int a1, a2, b1, b2;
      ParseKey(a, a1, a2);
      ParseKey(b, b1, b2);
      if (a1 < b1) return -1;
      if (a1 > b1) return +1;
      if (a2 < b2) return -1;
      if (a2 > b2) return +1;
      return 0;
    }

    // Ignore the following methods for now:
    const char* Name() const { return "TwoPartComparator"; }
    void FindShortestSeparator(std::string*, const leveldb::Slice&) const { }
    void FindShortSuccessor(std::string*) const { }
    void ParseKey(const leveldb::Slice& a, int x, int y) const {
        auto str = a.ToString();
        assert(str.size() >= 2);
        assert(isdigit(str[0]));
        assert(isdigit(str[1]));
        x = str[0] - '0';
        y = str[1] - '0';
    }
  };

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

    leveldb::WriteOptions write_options;
    write_options.sync = true;
    db->Put(write_options, "key3", "leveldb3");
    status = db->Get(leveldb::ReadOptions(), "key3", &val);
    if (!status.ok()) std::cout << status.ToString() << std::endl;
    else std::cout << val << std::endl;

    status = db->Put(leveldb::WriteOptions(), "key0", "leveldb0");
    assert(status.ok());
    status = db->Put(leveldb::WriteOptions(), "key8", "leveldb8");
    assert(status.ok());
    status = db->Put(leveldb::WriteOptions(), "key6", "leveldb6");
    assert(status.ok());

    // 输出数据库的所有key-value对
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << it->key().ToString() << ":" << it->value().ToString() << std::endl;
    }
    assert(it->status().ok());

    // 处理[start,limit)范围内的key
    leveldb::Slice start("key2");
    std::string limit("key6");
    for (it->Seek(start); it->Valid() && it->key().ToString() <= limit; it->Next()) {
        std::cout <<"### "<< it->key().ToString() << ":" << it->value().ToString() << std::endl;
    }

    // 逆序处理：（逆序会比顺序慢一些）
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
        std::cout<< "reverse  " << it->key().ToString() << ":" << it->value().ToString() << std::endl;
    }
    delete it;

    //Snapshots快照
    leveldb::ReadOptions readOptions;
    readOptions.snapshot = db->GetSnapshot();
    assert(readOptions.snapshot != nullptr);
    status = db->Put(leveldb::WriteOptions(), "key10", "leveldb10");
    assert(status.ok());
    leveldb::Iterator* iter = db->NewIterator(readOptions);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::cout << "*** " << iter->key().ToString() << ":" << iter->value().ToString() << std::endl;
    }
    assert(iter->status().ok());
    delete iter;
    db->ReleaseSnapshot(readOptions.snapshot);

    delete db;
    db = nullptr;

    TwoPartComparator cmp;
    leveldb::DB* db1;
    leveldb::Options options1;
    options1.create_if_missing = true;
    options1.comparator = &cmp;
    status = leveldb::DB::Open(options1, "/tmp/testdb11", &db1);
    assert(status.ok());
    status = db1->Put(leveldb::WriteOptions(), "123", "123"); assert(status.ok());
    status = db1->Put(leveldb::WriteOptions(), "133", "133"); assert(status.ok());
    status = db1->Put(leveldb::WriteOptions(), "223", "223"); assert(status.ok());
    status = db1->Put(leveldb::WriteOptions(), "323", "323"); assert(status.ok());
    leveldb::Iterator* it1 = db1->NewIterator(leveldb::ReadOptions());
    for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
        std::cout << it1->key().ToString() << ":" << it1->value().ToString() << std::endl;
    }
    assert(it1->status().ok());
    delete it1;



    return 0;
}



//WriteBatch是一系列对数据库的更新操作，并且这些批量操作之间有一定的顺序性。
//撇开writebatch带来的原子性优势，writebatch也能通过把多个更新放在一个批量操里面来加速操作。
/*
    通常情况下，所有的leveldb写操作都是异步的：当leveldb把写操作交给 操作系统之后就返回。
    从操作系统内存到硬盘等持久性存储是异步的。如果在写的时候打开同步写选项，那么只有当数据持久化到硬盘之后才会返回。
    (On Posix systems, this is implemented by calling either fsync(...) or fdatasync(...) or 
    msync(..., MS_SYNC) before the write operation returns.)
    异步写通常比同步写快1000倍以上。异步写的不足就是当机器宕机时会丢失最后更新的数据。写进程的异常退出并不会造成数据的丢失。
    通常情况下异步写能够被妥善的处理。例如，当你在往数据库写大量的数据时，在机器宕机之后能通过重新写一次数据来修复。
    混合使用同步和异步也是可以的。例如每N次写做一次同步。当机器宕机的时候，只需要重新写最后一次同步写之后的数据。
    同步写一个新增一个标记来记录上一次同步写的位置。
    WriteBatch是一个异步写。一个WriteBatch内部的多个更新操作放在一起也可以使用同步写操作，(i.e., write_options.sync 
    is set to true). 可以通过批量操作降低同步写的消耗。

    https://blog.csdn.net/doc_sgl/article/details/52824426

    一个数据库每次只能被一个进程打开。leveldb为了防止误操作需要一个lock。在一个进程内部，
    同一个leveldb::DB对象可以在这个进程的多个并发线程之间安全的共享。 例如，不同的线程可以写，获取指针，
    或者读取相同的数据库，而不需要额外的同步操作，因为leveldb自动做了请求的同步。然而，其他的对象，
    例如迭代器或者WriteBatch，需要外部的同步操作。如果两个线程共享同一个这样的对象，那么他们必须用自己的lock protocal
    对数据库操作进行保护。

    快照在整个key-value存储状态上提供了一个持久性的只读视图。非空的ReadOptions::snapshot提供了一个针对db特定状态的只读视图。
    如果ReadOptions::snapshot是NULL，那么读操作是在对当前数据库状态的隐式视图上的进行的。
    使用DB::GetSnapshot()方法创建Snapshots. 如果快照不再需要了，应该使用DB::ReleaseSnapshot接口来释放，
    这会消除为了维持快照的状态多与操作。

    level-db不返回以null结尾的c类型的字符串，是因为leveldb允许key和value中包含'\0'字符。

    默认的排序函数，也就是字典序。另外，我们也在打开数据库的时候也可以指定一个排序比较函数。



    */
