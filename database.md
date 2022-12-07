# database 

## storage

diskmanager 负责数据库的数据在易失性存储和非易失性存储的移动

#### disk manager替换os来管理的意义

#### 如何使用磁盘文件来表示数据库的数据

#### OLTP和OLAP

#### data storage model

+ NSM

+ DSM

#### bufferpool

+ pagetable 哈希表

+ 预取

+ lru链表

+ flush链表

#### 替换算法

+ LRU

+ clock

+ LRU-K

## transaction

#### ACID

原子性、一致性、隔离性、持久性

#### Serial

+ Serial Schedule

+ Serializable Schedule

#### 2PL

+ lock manager

+ lock type

+ 2PL

+ Rigorous 2PL

+ 死锁检测 检测wait_for_map

+ 死锁预防 wait-die wound-wait

+ hierarchical locking 防止锁过多

#### timestamp ordering 




