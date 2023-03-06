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

#### 分类

+ 只读事务

+ 读写事务

#### 事务id分配时机

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




## 日志

#### redo log

记录数据修改后的状态

##### redo log格式

+ 在某个偏移量修改len长度字节

+ 插入、删除记录，创建页面的redo log


##### redo log组

##### mini trasaction

##### redo log buffer

刷盘时机

##### log buffer

##### redo log 日志文件组

##### lsn flush_lsn checkpoint

##### redo log写入过程

##### 崩溃时如何恢复




#### undo log

记录数据修改前的状态

##### 日志指针存储位置row pointer

##### insert的undo log

##### delete的undo log

中间态

##### update的undo log

+ 更新主键

+ 不更新主键

##### 增删改二级索引的步骤

##### undo页

+ 两种主要类型

##### undo链表

+ 重用原则

##### 回滚（rollback）段

+ slot

+ cache

+ history

##### undo日志分配、写入过程

+ 分配回滚段

+ 寻找是否有对应缓存slot

+ 没有则寻找一个可用slot

+ 如果非cache，则分配undo seg，将firstpage填入slot

+ 开始使用！

##### undo log buffer

刷盘时机

##### 崩溃时如何恢复



















