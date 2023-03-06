# 数据库


## STORAGE

#### Disk Manager

#### Bufferpool

+ pagetable 哈希表

+ 预取

+ lru链表

+ flush链表

#### 使用磁盘文件来表示数据库的数据

#### Data Storage Model

+ NSM

+ DSM


## INDEX索引

#### B+树

##### 数据结构

##### 并发控制

#### 动态哈希表

##### 数据结构

##### 并发控制



## SQL

#### SQL解析

#### SQL预处理

#### SQL优化

#### SQL执行

##### Processing Model

+ Iterator Model

+ Materialization Model

+ Vectorization Model

##### Join Algorithms

+ Nested Loop Join

+ Sort Merge Join

+ Hash Join



## TRANSACTION事务

#### ACID

原子性、一致性、隔离性、持久性

#### 四大隔离级别

+ 读未提交

+ 读已提交

+ 可重复读

+ 串行化

#### MVCC

#### RECOVERY

##### REDU

##### UNDO

#### 锁

##### 间隙锁

##### 表锁

##### 行锁

#### 并发控制实现

##### 2PL

+ lock manager

+ lock type

+ 2PL

+ Rigorous 2PL

+ 死锁检测 检测wait_for_map

+ 死锁预防 wait-die wound-wait

+ hierarchical locking 防止锁过多










## Redis

### 数据结构

##### string

sds

+ int

+ raw

+ embstr

##### list

quicklist

ziplist

##### hash

##### set

##### zset








