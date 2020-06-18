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
    status = db1->Put(leveldb::WriteOptions(), "123", "113"); assert(status.ok());
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

    快照代表了数据库的所有键值数据在某一时间点的状态，快照是只读的且保持一致。
    ReadOptions::snapshot为非空时，表示读操作应该在一个特殊版本的数据库状态上进行操作。
    如果ReadOptions::snapshot为空，读操作将隐式的在数据库的当前状态上进行操作。
    使用DB::GetSnapshot()方法创建Snapshots. 如果快照不再需要了，应该使用DB::ReleaseSnapshot接口来释放，
    这会消除为了维持快照的状态多与操作。

    level-db不返回以null结尾的c类型的字符串，是因为leveldb允许key和value中包含'\0'字符。

    默认的排序函数，也就是字典序。另外，我们也在打开数据库的时候也可以指定一个排序比较函数。

    #向后兼容(Backwards compatibility)
    数据库被创建时，指定的比较器(leveldb::Comparator)的Name方法的返回值将会被保存起来；之后每一次打开数据库时，
    都会检查该值是否与本次打开指定的比较器的Name方法的返回值匹配。如果名字变了，那么 leveldb::DB::Open方法就会返回失败。
    因此，只有在新的key格式和比较器无法与现有的数据库兼容是，可以使用新的名称；同时，现在有的数据库的所有数据都讲被丢弃。
    然而，通过提前制定计划也可以实现key格式的逐渐改变。例如，在每个key中保存一个版本号，当需要修改key的格式的时，
    可以在不修改比较器名称的前提下，增加版本号的值，然后修改比较器的比较函数，使其通过区分key中的版本号，来做不同的处理。

    #性能
    通过修改一些参数可以调整leveldb的性能，可以在include/leveldb/options.h中查看定义。
    块尺寸(Block size)
    leveldb将相邻的keys聚集在一起放进同一个块中，然后将块作为写入或者从持久存储中读取的单元。默认的块大小大约为4096个未压缩字节。
    主要对数据库内容做批量扫描的应用不妨增加块的大小。若应用有很多读取小数据的地方，不妨在配合性能测试的条件下，选择一个更小的块大小。
    当块尺寸小于1K bytes或者1Mbytes时，性能将不会显著提升。注意更大的块尺寸可以让压缩有更好的效果。

    压缩(Compression)
    每个块在被写入持久存储前都会被压缩。leveldb默认是允许压缩的，因为默认的压缩方法是很快的。对不可压缩数据的将自动关闭压缩功能。
    极少数情况下，应用程序可能想要完全禁止压缩，但是除非检测表明禁止之后性能得到提升，否则不应该完全禁止。禁止方法如下：
    leveldb::Options options;
    options.compression = leveldb::kNoCompression;
    leveldb::DB::Open(options, name, ...) ....

    #缓存(Cache)
    leveldb的数据是以一些列文件的形式存放在文件系统中的，每个文件中存放了一系列经过压缩的块。如果options.cache非空，
    那么他将被用来存放频繁使用的未压缩的块数据。
    #include "leveldb/cache.h"
    leveldb::Options options;
    options.cache = leveldb::NewLRUCache(100 * 1048576);  // 100MB cache
    leveldb::DB* db;
    leveldb::DB::Open(options, name, &db);
    ... use the db ...
    delete db
    delete options.cache;

    必须要注意的是缓存中存放的是未压缩的数据，因此应该根据应用程序的数据来确定其大小，而不应该把压缩带来的数据尺寸变小考虑在内。
    (缓存压缩过的块数据是由操作系统负责，或者客户端定制Env来实现)
    当执行批量读操作时，应用程序可能希望禁止缓存功能以防止批量读操作破坏cache中已经缓存的内容。可以通过设置迭代器的来达到该目的：
    leveldb::ReadOptions options;
    options.fill_cache = false;
    leveldb::Iterator* it = db->NewIterator(options);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ...
    }

    #键布局(Key Layout)
    注意磁盘传输和缓存的单位是块。相邻的键(根据数据库的排序顺序)通常被放在同一个块中。
    因此应用程序可以把那些需要同时存取的键放在相邻的位置，不常用的键合拢单独放在一个位置，以此来提高性能。
    例如，假设我们以leveldb为基础，实现一个文件系统。存储的条目类型设置以下格式：
    filename -> permission-bits, length, list of file_block_ids
    file_block_id -> data
    我们可能需要在filename前加一个字母(例如’/’)作为前缀，在file_block_id前加一个不同的字母(例如’0’)，
    这样扫描只需要检查元数据而不需要强制我们读取和缓存笨重的文件内容。(此处翻译有疑问)
    
    #过滤器(Filters)
    由于leveldb的数据在磁盘上的组织方式，一个Get()方法可能导致多次从磁盘读取数据。可选的FilterPolicy机制可以用来减少读磁盘的次数。
   leveldb::Options options;
   options.filter_policy = NewBloomFilterPolicy(10);
   leveldb::DB* db;
   leveldb::DB::Open(options, "/tmp/testdb", &db);
   ... use the database ...
   delete db;
   delete options.filter_policy;

   上述代码将一个基于Bloom_filter算法的过滤策略与数据库联系起来。基于Bloom_filter算法的过滤策略为每个键保存
   若干个bit的数据在内存中(根据传给NewBloomFilterPolicy的参数，该例中将为每个key保存10个bit的数据)。
   该过滤器会将Get()方法需要的不必要磁盘读操作数量降低大约100倍。增加保存的bit数量会大幅的减少磁盘读操作，但是也会占用更多的内存。
   我们建议工作集不适合在内存中或者做大量随机读操作的应用程序设置一个过滤策略。
   如果使用一个定制的比较器，那么应该保证正在使用的过滤策略和比较器是互相兼容的。例如，假设一个比较器在比较key时忽略尾随空格，
   那么NewBloomFilterPolicy不能和这样的比较器一起使用。此时应用程序应该提供一个忽略尾随空格的过滤策略与该比较器一起使用。
   例如：
   class CustomFilterPolicy : public leveldb::FilterPolicy {
   private:
    FilterPolicy* builtin_policy_;
   public:
    CustomFilterPolicy() : builtin_policy_(NewBloomFilterPolicy(10)) { }
    ~CustomFilterPolicy() { delete builtin_policy_; }

    const char* Name() const { return "IgnoreTrailingSpacesFilter"; }

    void CreateFilter(const Slice* keys, int n, std::string* dst) const {
      // Use builtin bloom filter code after removing trailing spaces
      std::vector<Slice> trimmed(n);
      for (int i = 0; i < n; i++) {
        trimmed[i] = RemoveTrailingSpaces(keys[i]);
      }
      return builtin_policy_->CreateFilter(&trimmed[i], n, dst);
    }

    bool KeyMayMatch(const Slice& key, const Slice& filter) const {
      // Use builtin bloom filter code after removing trailing spaces
      return builtin_policy_->KeyMayMatch(RemoveTrailingSpaces(key), filter);
    }
  };
    高级应用可以提供一个筛选策略，它不使用一个布鲁姆过滤器，而是使用其他一些机制来概括一组键。细节参考leveldb/filter_policy.h。

    #校验(Checksums)
    leveldb对所有它存放在文件系统的数据计算校验和。leveldb提供两个独立的选项来控制数据校验的严格程度。
    ReadOptions::verify_checksums设置为true，则对所有从文件系统中读取的数据进行校验和检查。默认不会进行该检查。
    若打开数据库时，设置Options::paranoid_checks为true，那么leveldb检测到内部数据损坏时会抛出一个错误。
    根据数据库中的已损坏部分，当数据库被打开或由有一个数据库操作时，可能会抛出错误。默认情况下该选项是关闭的，
    以便数据库可以在部分已经损坏的情况下继续使用。
    如果数据库已经被损坏(或许无法再Options::paranoid_checks为true时被打开)，leveldb::RepairDB方法可以用来尽可能的恢复数据。

    #估计大小(Approximate)
    使用GetApproximateSizes方法可以估计一个或多个划定的键范围被保存在文件系统中所需占用的空间大小。例如：
   leveldb::Range ranges[2];
   ranges[0] = leveldb::Range("a", "c");
   ranges[1] = leveldb::Range("x", "z");
   uint64_t sizes[2];
   leveldb::Status s = db->GetApproximateSizes(ranges, 2, sizes);
   执行上述代码后，sizes[0]将保存[a..c)范围内所有key保存在文件系统中估计需占用的空间大小，sizes[1]将保存[x..z)范围内
   所有key保存在文件系统中估计需要占用的空间大小。

   #环境(Environment)
    leveldb发起的所有操作(和其他由操作系统调用的)，都需要通过一个leveldb::Env对象路由。有经验的客户端不妨提供自己
    的Env实现以取得更好的控制。例如，一个应用程序可以(in the file IO paths什么意思？？？)引入人为的延迟限制LevelDB对
    系统中的其他活动的影响。
    class SlowEnv : public leveldb::Env {
        .. implementation of the Env interface ...
    };

    SlowEnv env;
    leveldb::Options options;
    options.env = &env;
    Status s = leveldb::DB::Open(options, ...);
    
    #可移植性(Porting)
    通过实现leveldb/port/port.h中的方法的平台相关版本，可以将leveldb移植到平台上。更多细节参见leveldb/port/port_example.h。
    另外，移植一个新的平台后，或许需要实现一个新的默认leveldb::Env类型。示例见文件leveldb/util/env_posix.h
    */

