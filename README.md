# CMU15-445

2021实验在2021分支上，2022在2022分支上，这里是2021的实验笔记

卡内基梅隆大学2021秋数据库实验

## lab1

实验1是手撸一个内存池。如果有os基础的话，相信应该非常容易上手。只需看完CMU数据库视频buffer pool相关章节即可开始。实验被分成三个子任务。


### LRU REPLACEMENT POLICY

这个子任务是写一个lru算法替代类，一开始我觉得这个任务估计是最简单的，后面才发现玄学多着。


#### PARAM OUT

cpp的这个注释我一开始是不知道的，输出参数。即虽然这个参数是传入的形参，但它是个指针变量，起到输出的作用，你需要将要输出的参数值写入此指针所指向的地址，是为输出参数。如
victim函数。起初我很疑惑，为啥这个函数删除的一个frameid，但是不返回呢？这样调用者怎么获得你删除的那个frameid呢，后面一看注释，才知道这个知识点。妙啊


#### 哈希表与链表结合

为了实现lru算法，我使用链表按到来的先后顺序存储frameid，使用哈希表存储frameid和其在链表中的指针。这样我需要对链表中指定数据的删除时可以直接做到o1时间复杂度，而我要找到
victim也只需pop链表头，从哈希表中删除即可，时间复杂度也为o1，需要添加framid也只需要将其push到链表和加入哈希表（重复则不加），时间复杂度也为o1。这样可以大大减少时间开销。一
开始我是单纯使用哈希表存储frameid加时间戳，这样寻找victim的时间复杂度是on，线上测试内存这块会超时。这大大减少了时间复杂度，从on变为o1，只能说妙啊，不愧是cmu的数据库实
验！！！


#### UNPIN

这个方法需要注意，当然你看本地的测试文件时也会得出这个结论。就是unpin多次同一个frameid时，时间应该以第一次unpin时为准，而不是最后一次。因为第一次已经unpin了，就不在这个lru
队列中，后面的unpin是无效的。


#### 多线程安全

最后为了使lru是多线程安全的，我们在victim、pin、unpin三个需要修改lru相关数据结构的方法最开始加上lock_gaurd保护相关数据结构读写。



### BUFFER POOL MANAGER INSTANCE

这个算这个实验的核心与重点吧，下面分别介绍一些实现的函数思路与坑点

#### NewPgImp

- 首先我们遍历page_数组，寻找没有被pin的页来承载。如果没有找到，我们直接返回空指针

- 然后我们需要调用分配pageid相关方法分配新的pageid，作为我们new生成的pageid，这在os中的虚拟内存管理中类似于虚拟内存，或者说虚拟页号

- 而接下来我们需要找到可以映射的物理内存，或者说物理页号。这分两步，首先在freelist中寻找，不为空我们直接popfirst即可。

- 找不到，我们调用上面lrureplacer的victim方法寻找受害者。这里就需要特别注意了。此处的victim受害者有可能在page_table_存在和pageid有相关映射关系，类比虚拟内存，相当于在页表
  中这块物理页面和虚拟页面存在映射关系，此处我们必须要删除这映射关系！！！同时，在pages_数组中承载这块frameid，所以若其为脏，我们必须写回磁盘，这样相当于将其安全地从内存写
  回磁盘，它在内存的一切活动已经结束，最后标记为不脏，为啥标记为不脏？因为这接下来这frameid要映射的是我们的新建的pageid，而我们新建的pageid目前肯定是不脏的。故接下来我们可
  以放心的使用这块frameid来和我们新建的pageid进行映射。这里我们要说一下为啥我们这里可以确定在freelist和vimtim中成功找到frameid呢？我们第一步就确定了哦

- 接下来我们在page_table_中建立映射关系，相当于虚拟内存中页表的map方法。同时pages_数组中frmeid的page承载的内容也要发生变化，修改pageid，修改pincount，关键一点是要调用
  lrureplacer的pin方法pin住，这是一开始我不知道的，后面通过测试得出结论。最后返回之。


#### FetchPgImp

- 首先会在page_table_中寻找，找到则pin，然后返回。

- 找不到，则和上面的new方法及其相似，除了不会分配新的pageid。在内容上，最后需要从磁盘读入相关内容到承载的page上。


#### DeletePgImp

- 一开始DeallocatePage(page_id)

- 然后在page_table_寻找映射关系，有可能是不存在映射关系的，这种情况我们直接返回true即可，这步不能少，如果你直接find，不判断这个find值是否是end（若是end表示不存在映射关系，
  你接下来使用这end会出大错，我就是UnpinPgImp忽视这一步，导致后面的PARALLEL BUFFER POOL MANAGER的NewPgImp奇奇怪怪的错误。
  
- 若存在映射关系，我们检查pincount，不为0我们是删除不了的，这种情况返回false

- 然后将frameid代表的page所有字段清0，pageid标记为INVALID_PAGE_ID，从page_table_删除映射关系

- 最后加入freelist中，返回true


#### UnpinPgImp

- 首先在page_table_寻找映射关系，有可能是不存在映射关系的，这种情况我们直接返回true即可，这步不能少，和上面一样

- 然后通过传入的脏标志判断是否需要标记page为脏

- 接下来判断pincount是否已经为0，为0则不能unpin，返回false

- 否则将pincount减1，判断是否为0，为0则调用lrureplacer的unpin加入lru相关数据结构


#### 线程安全的BUFFER POOL MANAGER INSTANCE

上面四个方法均对相关数据结构进行修改读写，所以，在方法最开始加上lock_gaurd保护相关数据结构读写。


### PARALLEL BUFFER POOL MANAGER

首先这个模块的设立是减少竞争，通过打破锁的粒度来达到，在xv6中的lock实验也很好的涉及到了。这个子任务估计是最简单的，只需要按照mod的映射关系，调用相关instance的相关接口即
可。提一下NewPgImp，这个方法注意你starting_index加的时机，这里注释没有说明白，不是说你一轮寻找完了才对starting_index加1，而是每找一个instance，都对其进行加1，直到成功，
成功后再加1。

#### 构造方法与析构函数

需要通过num_instances来判断新建instance的个数，并且保存之，可以使用数组或者链表。注意，如果使用new则记住在析构函数中delete之

#### 映射关系

按照取余数的映射关系，调用对应instance接口即可

#### NewPgImp

这里的a round robin manner算法得重点提一下

- 首先从start开始新建，start被初始化为0，若新建失败，start需要自增1到了num_instances则退回0，这里可以通过取余数来实现

- 直到新建成功后，start继续自增1，然后返回即可

### 相关数据结构在bustub数据库中的位置层级

实验1主要是实现内存缓存池的管理，下面一块很好的说明了bustub对内存缓冲区管理的实现
              
                        继承                                                                                                        
                      ------> BufferPoolManager <                                                                                   
                   /                           |                                                                                    
                  /                            |
      ParallelBufferPoolManager                |                                                                                              
                |                              |  继承                                
                |                              |                                  
      BufferPoolManagerInstance[]              |                                 
                |                              |                                  
                ------------------->     BufferPoolManagerInstance
                                               |
                                     /         |             \                    
                         
               Page[]    DiskManager    LogManager    unordered_map<page_id_t, frame_id_t>    Replacer   list<frame_id_t>
                                                                                                  |
                                                                                                  |
                                                                                                 / \
                                                                                         继承   /    \   继承
                                                                                               /      \
                                                                                    lru_replacer     clock_replacer
               

## lab2

实验二是实现extendible哈希表，其实我更想写b+树的，可惜这个好像是2020年的，以后有机会写一下。

三个子任务，一开始是高估的第一个子任务的难度，其实看测试文件，只需要实现一点点即可。


### PAGE LAYOUTS

#### DIRECTORY

首先hashtabledirectorypage保存哈希表的根page，是整个哈希表最基本的数据结构，整个哈希表能够通过该数据结构获取整个哈希表在磁盘中的分布。由于必须要能够存储在一块磁盘下，所以
对整个数据结构大小有限制，下面是相关字段

- page_id_t page_id_  指示该hashtabledirectorypage存储在哪个pageid中

- lsn_t lsn_          

- uint32_t global_depth_  全局深度，为2的整数次幂，初始值为0

- uint8_t local_depths_[DIRECTORY_ARRAY_SIZE]  记录局部bucket深度，最大为DIRECTORY_ARRAY_SIZE，防止超出一块磁盘大小

- page_id_t bucket_page_ids_[DIRECTORY_ARRAY_SIZE]   记录局部bucket的pageid号，最大为DIRECTORY_ARRAY_SIZE，防止超出一块磁盘大小

##### IncrGlobalDepth 

先是简单的深度+1，然后复制前一半的pageid和localdepth到后一半，去HASH TABLE IMPLEMENTATION去实现这个机理也可。这里我们发现，我们并不需要数组扩容，其实，我们的globaldepth已
经是控制好了我们能够访问的数组下标范围，为[0, 2^globaldepth - 1]。插入的数据容量不超过约20w，应该是不会造成数组溢出的，其实为了健壮性考虑，我在上层实现了溢出检测，在写实验
的时候没有实现溢出检测，导致出现了非常奇怪的错误。

##### GetSplitImageIndex
这里的SplitImage概念指导书说自己会明白，定义：

- 真镜像：a为xxxxx1000，b为xxxxx0000，a的真镜像是b，x的个数是globaldepth-localdepth，代表0或者1，二者所有x代表的位值应该相同而数字的位数则是localdepth。所以获取的真镜像
  是bucket_idx ^ (1 <<(local_depth_[bucket_idx] - 1))，注意^符号是异或。称a的真镜像是b

- 镜像：a为xxxxx1000，b位yyyyy0000，x和y不一定相等，x和y的个数均是globaldepth-localdepth，称b是a的镜像之一

- 镜像族：所有的b就是a的镜像族

镜像是镜像族的一个，真镜像是只有第localdepth位不同，其他都相同，而镜像族则是所有localdepth-1位都相同，但是第localdepth位不同的所有index集合，(最高位限制到globaldepth位)，
在分裂和合并时镜像族的概念非常重要！！！

##### CanIncr

判断是否能增长，条件为(1 << (global_depth_ + 1)) <= DIRECTORY_ARRAY_SIZE，很容易理解

##### CanShrink

判断是否能收缩，只需要所有localdepth小于globaldepth即可



#### BUCKET

hashtablebucketpage存储了bucket的内容，大小也是限制在一块磁盘大小，相关字段如下：

- char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1]  位图，每一个比特位表示该槽是否被占据

- char readable_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1]  位图，每一个比特位表示该槽是否可读，这里解释下占据和可读的关系，可读表示有数据，并且该数据合法（没有被删除），
                                                   被占据表示有数据，但是合法性不保证（可能被删除，插入数据可以直接写入被占据但是不可读的槽）

- MappingType array_[1]  每一个数组位代表一个槽，存储插入哈希表的数据，这是个0长数组或者1长数组，可自行google，又在cmu学到一个新技巧

##### GetFirstNoOcpBkt

辅助方法之一，获取第一个没有被occupy的位，算法就是简单遍历occupied_数组，之所以没有设置相关字段保存第一个没有被occupy的位，是因为整个数据结构大小已经刚好存入一整块磁盘，
所以没有添加字段。这几个要存入整块磁盘的数据结构我都没有添加字段

##### GetFirstNoReadBkt

辅助方法之一，获取第一个不可读的位，算法同上

##### Insert

插入，首先遍历哈希表看是否有要插入的kv，若有则直接返回false，没有则找到第一个不可读的位写入要插入的kv（上面提到了），如果找第一个没有被占据的位，这样会大大浪费空间，会导致
后续10w级别的测试插入后删除，再插入时可能根本插入不进去，因为删除时只会将可读位置为不可读，但是occupied位是不会被修改的

##### Remove

删除，和插入相反，是位删除，只需简单标记位不可读即可，不能修改occupied位


### HASH TABLE IMPLEMENTATION

大boss来了。首先说明下extendiblehashtable相关字段，需要说明这个数据结构是不需要存入磁盘的。

- page_id_t directory_page_id_                记录存储hashtabledirectory的pageid，是整个哈希表最关键的字段

- BufferPoolManager *buffer_pool_manager_     内存缓冲池管理器，实验一我们已经拿捏

- KeyComparator comparator_                   比较器，用于比较键值对

- ReaderWriterLatch table_latch_              读写锁，用于保护整个哈希表

- HashFunction<KeyType> hash_fn_              哈希函数，传入的值通过此函数计算出哈希值，哈希值取低globaldepth位即为要被存入的bucketid


接下来介绍哈希表的最主要的一些方法
  
#### 构造方法
  
- 首先需要新建directory_page的page，调用实验一写的new方法即可，然后unpin之
- 接下来新建一个bucket页，同时再获取directory_page，将新建的bucketid写入数组下标为0的位置即可，这样我们的哈希表成功新建了directory_page，同时我们还新建了一个bucketpage，
  用于装填数据。pageid已经写入directorypage中。完成哈希表初始化。
  
- 最后在这里我要说明下带page和不带page的数据结构区别，如hashtabledirectorypage和hashtabledirectory，前者主要是侧重该数据结构存储在磁盘，后者侧重数据结构。当从磁盘读
  出这块数据结构时，是一段字节序列，我们使用cpp自带的强转reinterpret_cast转化为相关数据结构。应该是很好理解。下面的辅助方法会提到
  
#### 辅助方法
  
- Hash 计算哈希值，哈希值为bucketid号
  
- KeyToDirectoryIndex  计算出哈希值，哈希值取低globaldepth位即为要被存入的bucketid
  
- FetchDirectoryPage 从内存缓冲区管理器通过directoryid号获取存储hashtabledirectorypage的page，将其中的字节序列强转为hashtabledirectorypage返回
  
- FetchBucketPage 同上
  
#### GetValue
  
- 首先FetchDirectoryPage
  
- 再计算哈希值，获取bucket_idx号，通过DirectoryPage获取数组下标为bucket_idx，得到bucketpageid。
  
- 再FetchBucketPage。这一系列操作后面的方法一开始基本会用到，这就是从内存缓冲池管理器那里获取DirectoryPage，再通过DirectoryPage的bucket_page_ids_获取bucketpageid，
  通过这个从内存缓冲池管理器那里获取对应bucketpage。后面的方法我就简称初始化。
  
- 接下来调用其GetValue即可，最后unpin初始化阶段fetch两个page，否则缓冲区会爆，而且test的检查会很严格，一般对于此哈希表最少只需要3个page即可支持整个哈希表的内存运
  作，这个我会在SplitInsert详细讨论。
 
- 对于线程安全的GetValue，我们只需一开始对整个哈希表加读锁，而对于bucket页，我们也是请求读锁，这样可以减少锁的竞争。
  
#### Insert
  
- 初始化
  
- 调用对应bucketpage的insert方法
  
- 开始检查，是否因为bucket满了而插入失败，判断条件是!res && bucket_page->IsFull()，因为也有可能存在相同键值对而插入失败。
  
- 如果是，我们unpin两个page，开始调用SplitInsert，否则我们我们unpin两个page，结束插入
  
- 对于unpin脏位的判断，我一开始是认认真真写，但是后面开始出现数据不一致性问题，故除了GetValue之外，我全部都将传入的脏标志改为true
  
- 对于线程安全的Insert，我们一开始对整个哈希表加读锁，而对于bucket页，我们请求写锁，这样可以减少锁的竞争。而万一我们需要修改directory，即bucket满了，我们需要分裂插入，
  我们将读锁升级为写锁，释放bucket的写锁，因为我们已经对整个哈希表加写锁了。
  
#### SplitInsert

整个实验最关键最难的两个方法之一，要插入的这个bucket满了。我们需要分裂。
  
- 初始化，然后进行判断，局部深度和全局深度是否一致
 
- 一致则看是能否扩展哈希表，即调用CanIncr()
  
- 如果不能，我们在unpin两个page之后exit退出程序。如果可以的话我们调用IncrGlobalDepth()
 
- 扩展哈希表之前，将所有原来属于同一个bucketid的bucketidx集合，成为B，深度通通加一
  
- 然后我们再新建一个page，注意上面提到至少三个page就可以满足我们哈希表在内存的运行，ag测试程序也是这样测试，这里说一下哪三个呢，directory，满的bucket，和新建的bucket，一共
  三个
  
- 下面获取原先满bucketidx的镜像族C，其个数刚好为B的二分之一，将其bucketid改为新建的bucketid。这样就成功完成了分裂
  
- 取出原先满bucket中所有数据，并且清空之，将它们重新计算哈希分入新建的bucket和原先满（现在被清空）的bucket。有一个很极端的情况，就是分裂后所有数据还是集中在一个bucket中， 
  此时我们需要unpin另一个空的bucket，继续循环上述操作进行分裂。
  
- 若极端情况没有出现，我们获取待插入的键值对要插入的page，另一个不需要插入的page我们unpin掉，执行插入，最后unpin两个page，结束。
  


#### Remove
  
- 初始化
  
- 调用对应bucketpage的remove方法
  
- 开始检查，和insert很相似。如果出现res && bucket_page->IsEmpty()，即本次的确删除了一个键值对并且导致了bucket空，如果单纯判断bucket_page->IsEmpty()有可能之前已经空了，
  这样如果调用合并是浪费成本，因为之前已经操作过了，本次删除是啥也没有干。那我们就unpin两个page，调用Merge方法合并。
  
- 如果上述条件不满足，我们unpin两个page结束

- 线程安全的Remove方法，我们采取的策略和Insert一模一样
  
#### Merge
  
整个实验最关键最难的两个方法之一，删除后的这个bucket为空。我们检查是否能够合并。
  
- 初始化
 
- 获取空镜像B的真镜像C，进行条件判断：B为空并且B和C的局部深度相等，第二个条件很重要，因为有可能B和C局部深度不相等，C大于B，这样的话，相当于B统一了但是C自己内部还没有统一
  
- 满足条件，开始循环
  
- 将所有是B的bucketidx全部改为C，然后将所有是C的bucketidx局部深度减1，相当于SplitInsert中相关的逆操作

- 更新全局遍历，开始新一轮循环，具体为unpin掉B，求C的真镜像D，将C赋值给B，将D赋值给C。
  
- 循环结束
  
- 收缩到不能收缩为止，unpin两个page，结束


### 相关数据结构在bustub数据库中的位置层级

实验二实现的是哈希索引，其实不论是哈希索引还是b+树索引，它们的数据结构和存储的数据都是分成一块块地存入磁盘，而相关的机制则是由实验一实现的内存缓冲池实现。
  

  
## lab3
  
实验三是实现火山模型，将上层的sql查询（可以看成经过SQL解析后的结果）转换为底层对数据库的操作，在实验三，一共分成九大模块，我将不仅仅说明这九大sql操作的实现，还会深入底层
分析这些操作在底层是如何进行的。首先我将对需要阅读的所有类进行分析，下面是这些类的层次结构
  
### ExecutorContext       
+ Transaction *transaction_
+ Catalog *catalog_  
+ BufferPoolManager *bpm_
+ TransactionManager *txn_mgr_
+ LockManager *lock_mgr_
  
### Catalog *catalog_ 
+ BufferPoolManager *bpm_  
+ LockManager *lock_manager_  
+ LogManager *log_manager_    
+ unordered_map<table_oid_t, unique_ptr<TableInfo>> tables_  存储tableid和table的映射关系
+ unordered_map<string, table_oid_t> table_names_  存储tablename和tableid的映射关系      
+ atomic<table_oid_t> next_table_oid_  原子类型，生成tableid  
+ unordered_map<index_oid_t,unique_ptr<IndexInfo>> indexes_  存储indexid和index的关系
+ unordered_map<string, unordered_map<string, index_oid_t>> index_names_  存储tablename indexname和indexid的关系
+ atomic<index_oid_t> next_index_oid_  原子类型，生成indexid
  
### TableInfo（table）
+ Schema schema_  相当于表结构
+ string name_  表名字
+ unique_ptr<TableHeap> table_  按表结构存储表的数据，组织形式为tuple，是一个指针
+ table_oid_t oid_  表id  
  
### Schema（存储表项）
+ uint32_t length_  一个tuple的长度
+ vector<Column> columns_  所有的列
+ bool tuple_is_inlined_  是否所有的列都是inlined
+ vector<uint32_t> uninlined_columns_  所有uninlined的列

### Column（存储每一个表项内容）
+ string column_name_  列名      
+ TypeId column_type_  列类型
+ uint32_t fixed_length_  
+ uint32_t variable_length_  列变量长度  
+ uint32_t column_offset_  该列在tuple中的偏移量
+ AbstractExpression *expr_  用于创建该列的表达式，通过调用表达式的evaluate方法，传入tuple和schema，可以获取对应列的值value，而tuple则可以通过value数组构造
           
### TableHeap（存储tuple，是数据库存储数据的数据结构）
+ BufferPoolManager *buffer_pool_manager_
+ LockManager *lock_manager_
+ LogManager *log_manager_
+ page_id_t first_page_id_ 存储tuple的第一个pageid，其中记录了pageid链的信息，sql查询的底层就是对这些进行操作
    
### tuple（数据库中数据载体）
+ bool allocated_ 是否被分配
+ RID rid_  
+ uint32_t size_  大小
+ char *data_  数据

### IndexInfo（index）
+ Schema key_schema_  相当于表结构
+ string name_  index名字
+ unique_ptr<Index> index_ 存储索引值
+ index_oid_t index_oid_ indexid
+ string table_name_ 对应的table名字
+ const size_t key_size_ 索引大小

### Index
+ unique_ptr<IndexMetadata> metadata_  index存储的数据

### ExtendibleHashTableIndex entend Index
+ KeyComparator comparator_  比较器
+ ExtendibleHashTable<KeyType, ValueType, KeyComparator> container_  哈希表，存储index

下面分析一下底层table存储tuple的构造，即分析TablePage类的字段，和文件系统十分相似其实。

这是整体的结构：

---------------------------------------------------------
| HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
---------------------------------------------------------

这是header的结构：

----------------------------------------------------------------------------
| PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
----------------------------------------------------------------------------
----------------------------------------------------------------
| TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
----------------------------------------------------------------

接下来介绍查询执行需要涉及到的类及其继承关系

### AbstractExpression
表达式类，计算相关比较结果和取值等，关键方法：evaluate
+ AggregateValueExpression
+ ColumnValueExpression
+ ComparisonExpression
+ ConstantValueExpression
  
### AbstractExecutor
执行类，执行相关查询操作，也是我们本次实验需要补充的，关键方法：init、next
+ SeqScanExecutor
+ InsertExecutor
+ DeleteExecutor
+ UpdateExecutor
...

### AbstractPlanNode
计划类，存储相关查询对应的信息
+ SeqScanPlanNode
+ InsertPlanNode
+ DeletePlanNode
+ UpdatePlanNode
...


而我们需要写的executer则是在上面表示的数据库中进行操作，对于每个execute，我们实现其init和next方法
  
### SEQUENTIAL SCAN
等价于：select * from table where
table存储在传入的ExecutorContext，而where存储在SeqScanPlanNode，通过GetPredicate()获取
  
#### init

- 通过exec_ctx_获取底层table，再获取这个table的迭代器，保存此迭代器
  
#### next

- 首先通过exec_ctx_获取要输入的scheme格式

- 再通过plan_->OutputSchema()获取输出tuple的格式

- 然后进入while循环，当迭代器没有消费完则持续循环

- 通过plan_->GetPredicate()获取select条件predict，是一个ComparisonExpression

- 若predict为空或者predict调用evaluate为true，依次调用输出scheme的所有column的evaluate方法计算value存入value数组，通过value数组构造tuple，返回之。注意是一次性返回一个

- 否则即不满足条件，我们开启新一轮循环

- 若循环结束，表示没有tuple可消费，table已经走到底，返回false
  
### INSERT
等价于：insert into table(field1,field2) values(value1,value2)
table存储在传入的exec_ctx_，value1存储有两种情况，如果是rawinsert，则存储在plan_中，如果不是则调用child_executor_的next方法获取。插入更新删除特别相似
  
#### init

- 若child_executor_不为空，调用child_executor_的init

#### next

- 通过exec_ctx_获取要执行插入的table

- 通过plan_调用IsRawInsert()进行判断，若是则从plan处调用RawValues()获取待插入值

- 不是则从child_executor_的Next方法获取插入值。

- 调用TableHeap的InsertTuple插入值，

- 调用exec_ctx_->GetCatalog()->GetTableIndexes获取索引数组，更新索引

- 注意必须一口气插入所有value，然后返回false即可

  
### UPDATE
等价于：update table set field1=value1 where
table存储在传入的exec_ctx_
  
#### init

- 若child_executor_不为空，调用child_executor_的init
  
#### next

- 待修改的原tuple全部通过child_executor的Next方法获取，child_executor_的next方法作为循环判断条件

- 循环体中调用下面提供的GenerateUpdatedTuple进行更新，注意索引也要同步更新

- 调用table的UpdateTuple方法完成底层更新

- 完成所有更新之后返回false

### DELETE
等价于：delete from table where 
table存储在传入的exec_ctx_
  
#### init

- 若child_executor_不为空，调用child_executor_的init
  
#### next

- 待修改的原tuple全部通过child_executor的Next方法获取，child_executor_的next方法作为循环判断条件

- 循环体中调用table的MarkDelete进行删除，注意索引也要同步删除

- 完成所有更新之后返回false

  
### NESTED LOOP JOIN
  
#### init

- 对left_executor_和right_executor_执行init，各自调用GetOutputSchema()获取二者输出格式

- 调用NestedLoopJoinPlanNode的Predicate()获取是否进行连接条件

- left_executor_的next为外循环，right_executor_的next为内循环，一定要这样，因为ag会测试磁盘花费。注意内循环一开始right_executor_调用Init初始化迭代器

- 通过是否连接条件判断连接。连接后将连接的tuple存储在数组ret_中即可，这样留给next使用，为啥要在init中干好这些事呢，因为连接是pipeline breaker，所以下一个也是和这个相似
  
#### next

- 使用ret_的数组迭代器，每次返回一个即可
  
### HASH JOIN
首先在头文件中加入哈希表相关代码.其次添加hash_map_字段存储key->vector<tuple>

#### init

- 对left_executor_和right_executor_执行init

- 计算所有left_executor_的tuple的key，加入hash_map_

- 计算所有right_executor_的tuple的key，在哈希表中查找，将key相同的全部连接之。保存于tuple数组供next使用。
  
#### next

- 使用ret_的数组迭代器，每次返回一个即可

  
### AGGREGATION
groupby和having，实现MIN 最小值，MAX 最大值，SUM 求和，AVG 求平均，COUNT 计数。

#### init

- 首先对children调用init

- 其次存储所有结果，因为这也是pipelinebreaker，在children的next循环中

- MakeAggregateKey获取groupby的key数组，调用MakeAggregateValue获取value数组，或者说计算value，这个value就是上面诸如MAX，MIN，然后将二者作为键值对插入aht_

#### next

- 若aht_迭代器指向末尾，返回false

- 获取当前迭代器指向的key和value

- 若plan_->GetHaving()为空或者plan_->GetHaving()->EvaluateAggregate满足条件，我们返回输出，否则我们接着下一轮迭代
  
### LIMIT
限制执行次数，只需设立相关字段保存执行次数，每执行一次加1即可
  
#### init
- 初始化执行次数time，调用child_executor_的Init

#### next
- 调用LimitPlanNode的GetLimit()获取最大执行次数

- 当执行次数和child_executor_的Next执行结果为真，我们则返回true，并且将执行次数加1，否则返回false
  
### DISTINCT

#### init

- 调用GetDistinctKey和GetDistinctValue计算child_executor_->Next获取的tuple，保存在哈希表中

#### next

- 使用init新建的哈希表迭代器，一次返回一对键值对即可
  
## lab4
  
实验四是实现事务机制，实现线程安全的事务机制，其实和os类似，这里可以把事务类比成os的线程或进程，同时实现四大隔离机制。
  
首先说明事务的四个状态
  
+ GROWING     此阶段可以获取锁，但是不能释放锁
  
+ SHRINKING   一旦GROWING开始释放锁，将转换为此阶段
  
+ COMMITTED   事务成功提交
  
+ ABORTED     事务失败
  
事务四大特征acid：
  
+ 原子性： 事务作为一个整体被执行，包含在其中的对数据库的操作要么全部都执行，要么都不执行
  
+ 一致性： 指在事务开始之前和事务结束以后，数据不会被破坏，假如A账户给B账户转10块钱，不管成功与否，A和B的总金额是不变的
  
+ 隔离性： 多个事务并发访问时，事务之间是相互隔离的，一个事务不应该被其他事务干扰，多个并发事务之间要相互隔离
  
+ 持久性： 表示事务完成提交后，该事务对数据库所作的操作更改，将持久地保存在数据库之中
  
四大隔离机制：
  
### LOCK MANAGER and DEADLOCK PREVENTION
  
+ 读未提交（Read Uncommitted）  可能出现脏读、不可重复读、幻读
  
+ 读已提交（Read Committed）  不会出现脏读
  
+ 可重复读（Repeatable Read）  不会出现脏读、不可重复读
  
+ 串行化（Serializable）  最高级别，啥都没有
  
### 死锁预防（Would-Wait）
  
+ 年轻事务如果和老事务有上锁冲突, 年轻事务需要等老事务解锁
+ 老事务如果和年轻事务有上锁冲突, 老事务直接将年轻的事务统统杀掉(回滚), 非常的野蛮hh
  
这样破环了循环等待条件
  
下面是本次实验涉及到的数据结构
  
#### LockManager
+ mutex latch_
+ unordered_map<RID, LockRequestQueue> lock_table_  存储锁队列的哈希表
  
#### LockRequestQueue  
+ list<LockRequest> request_queue_
+ condition_variable cv_  条件变量，保护request_queue_
+ txn_id_t upgrading_
  
#### LockRequest
+ txn_id_t txn_id_  事务id
+ LockMode lock_mode_  s锁还是x锁（相当于读锁与写锁）
+ bool granted_
  

  
四大隔离机制均会在本次实验底层详细实现（其实串行化没有实现，因为这里没有实现给index加锁）
  
#### LockShared
  
获取s锁
  
+ 若事务状态为ABORTED，返回false
  
+ 若事务状态为SHRINKING，此时不能获取锁，返回false
  
+ 若隔离级别为读未提交，此时没有s锁，也返回false（读未提交要对数据库进行读操作，直接读，不需上锁，这样才可能会出现脏读，也就是在读的过程中，其他事务对数据进行了修改）
  
+ 如果该事务对该rid已经上了s锁或者x锁，返回true
  
+ 下面获取条件变量的锁
  
+ 将锁请求添加到相关锁队列
  
+ 新建判断函数
  
+ 进入循环，若判断函数为假，或者事务状态为ABORTED，表面在等待过程被杀，我们直接退出循环，返回false。
  
+ 否则我们调用对应锁队列的条件变量的wait函数，即释放锁，等待唤醒，唤醒时抢锁
  
+ 最后获取s锁，我们在事务中记录，并且返回true
  
+ 接下来，我们需要重点说一下判断函数的过程我们遍历锁队列，通过txn_id_来判断年轻与年老，将所有在自己之前并且比自己年轻的，并且请求锁类型为x锁的事务通通杀掉，即相当于将状态改
  为ABORTED，并且将其在所队列中删除，将自己的相关字段删去锁。记住，三个条件，在自己之前、请求x锁、比自己年轻。三个条件缺一不可。若自己之前出现了比自己年老的x锁请求，我们只
  能返回false。否则返回true。这里注意cpp的深拷贝与浅拷贝。我就是图方便直接复制锁队列，导致根本没有修改锁队列。最后要返回时，若自己杀了事务，必须调用条件变量的notify_all唤
  醒所有事务，因为有可能前面的事务由于被杀导致部分事务已经可以获取锁，不手动唤醒可能会导致永远没有锁获取，因为原来作为唤醒者的事务现在被杀
  
  
#### LockExclusive

获取x锁
  
+ 若事务状态为ABORTED，返回false

+ 如果该事务对该rid已经上了s锁，返回false
  
+ 若事务状态为SHRINKING，此时不能获取锁，返回false 
  
+ 如果该事务对该rid已经上了x锁，返回false
  
+ 下面获取条件变量的锁
  
+ 将锁请求添加到相关锁队列
  
+ 新建判断函数
  
+ 进入循环，若判断函数为假，或者事务状态为ABORTED，表面在等待过程被杀，我们直接退出循环，返回false。
  
+ 否则我们调用对应锁队列的条件变量的wait函数，即释放锁，等待唤醒，唤醒时抢锁
  
+ 最后获取x锁，我们在事务中记录，并且返回true
  
+ 上述过程和获取s锁差不多，这里说一下判断函数和上面也差不多，不同地方在于杀事务时，前面所有比自己年轻的事务通通杀死，前面是比自己年轻的x锁请求杀死。杀完后若自己前面还有锁，
  则等待，而前者是只要杀完后前面没有x锁即可获取

#### LockUpgrade

锁升级，可以看成先删除s锁，再获取x锁，删除s锁时注意notify_all()即可，可以参照上面两个和下面一个函数
  
#### Unlock
  
释放锁
  
+ 首先若队列中没有自己的锁请求，返回false（不能虚假释放嘻嘻）
 
+ 否则我们将该锁从队列删除，调用notify_all()，自己相关获取锁字段也要擦除
  
+ 下面就是对状态的修改，若目前状态是GROWING，因为不可能我们也就是SHRINKING或者是ABORTING了我们还修改状态为SHRINKING
  
+ 若当前要释放的是s锁并且隔离级别为可重复读，或者是x锁，我们修改状态为SHRINKING。这里详细解释一下：因为RC是可以重复的加S锁/解S锁的, 这样他才可以在一个事务周期中读到不同的
  数据



### CONCURRENT QUERY EXECUTION

并行查询，这里我们修改四个地方：SEQUENTIAL_SCAN、INSERT、UPDATE、DELETE

#### SEQUENTIAL_SCAN

+ 在next方法中，一开始我们需要获取s锁，若不是读未提交，上面讲过了，我们调用LockShared获取s锁

+ 读完后，如果隔离级别是读已提交RC，我们需要释放s锁，因为RC可以随意加s锁释放s锁而不进入SHRINKING状态，而若是可重复读则不需要

#### INSERT

+ 我们需要获取x锁，但是插入之前它不存在怎么办呢，我们先插入，然后再获取x锁即可这样RU可以读到，但是其他的读不到哦

+ 注意最后我们需要更新索引，table更新已经帮我们实现了

#### DELETE

+ 获取x锁，但是我们是不能释放的，一旦释放我们就不能获取锁了

+ 同样注意索引更新哦

#### UPDATE

+ 同上
  
















