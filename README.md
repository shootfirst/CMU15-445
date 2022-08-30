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

第一个就是directory的IncrGlobalDepth()，不仅仅是简单的加深度即可，还要注意复制前一半的pageid和深度到后一半，当然你去上层去实现这个机理也可。第二个就是GetSplitImageIndex，
这里的SplitImage概念指导书说自己自然而然会明白，我解释一下，举个栗子，xxxxx1000的SplitImage是xxxxx0000，x的位数是globaldepth-localdepth，代表0或者1，而数字的位数则是
localdepth。所以获取SplitImage是bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1))。这里获取的是其中一个镜像，这里注意深度为0时调用此方法会报错！！！这里还得区分一下镜像
(SplitImage)和镜像族，镜像是镜像族的一个，我这里获取的镜像是只有第localdepth位不同，其他都相同，而镜像族则是所有localdepth-1位都相同，但是第localdepth位不同的所有index集合
(最高位限制到globaldepth位)，在分裂和合并时镜像族的概念非常重要！！！

#### BUCKET

bucket的删除，删除是位删除，只需要将readable设为0即可，occupied不需置位。最后就是0(1)长数组，这个自行google。


### HASH TABLE IMPLEMENTATION

这个是重点。我提一下fetch和new的调用时机，还有就是flite和merge的思路机理


#### FETCH AND NEW

directory_page_id_在构造方法中需要调用new方法新建一个page，将directory的内容写入其中。bucket的page在构造方法中也需要首先先new一个，将pageid号写入directory中。还有就是在分
裂flite时需要生成新的bucket，此时也需要调用new方法，其他的任何时机，只需要调用fetch方法！！！如果不这样，会导致你的数据不一致，还有很多奇奇怪怪的错误！！！还有就是每一个
fetch或者new必须要即时unpin，否则会内存溢出。

#### FLITER



#### MERGE





















