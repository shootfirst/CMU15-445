# CMU15-445
卡内基梅隆大学2021秋数据库实验

lab0直接跳过，因为自认为有一点cpp基础，加之前面两个实验(cs144和cs143)也是cpp写的，并且并没有发现写实验需要对cpp掌握很深。

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
 
- 获取空镜像B的真镜像C，进行条件判断：B为空并且B和C的局部深度相等，第二个条件很重要，因为有可能B和C局部深度不相等，C大于B，这样的话，是绝对不合适的，以图例
  
  idx                id
  000--------------|  100
  001--------------|    101
  010--------------|  100
  011--------------|   102
  100--------------|  100
  101--------------|    101
  110--------------|  100
  111--------------|   102
  

结尾为0的bucketidx假如为空，是绝对不能和结尾为1的bucketidx合并，因为结尾为1的idx自己还没有统一。

- 满足条件，开始循环
  
- 将所有是B的bucketidx全部改为C，然后将所有是C的bucketidx局部深度减1，相当于SplitInsert中相关的逆操作

- 更新全局遍历，开始新一轮循环，具体为unpin掉B，求C的真镜像D，将C赋值给B，将D赋值给C。
  
- 循环结束
  
- 收缩到不能收缩为止，unpin两个page，结束


### 相关数据结构在bustub数据库中的位置层级

实验二实现的是哈希索引，其实不论是哈希索引还是b+树索引，它们的数据结构和存储的数据都是分成一块块地存入磁盘，而相关的机制则是由实验一实现的内存缓冲池实现。




















