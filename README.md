## bitcask

实现：https://github.com/Qraffa/bitcask

bitcask k-v 数据库

### 实现功能

- put，添加或更新 k-v
- get，查询 k
- del，删除 k
- merge，整理合并数据文件 datafile，并生成 hintfile 用于重建内存模型
- rebuild，根据存在的 datafile 或 hintfile，重建内存模型

### 实现思路

#### 存储结构及数据结构

1. 数据文件

数据文件分为 datafile 和 hintfile，datafile 用于保存 k-v 键值对信息，hintfile 用于在 merge 时保存 key 在 datafile 中的 offset 等信息

datafile

```te
 crc   ks     vs    m   k     v
+----+----+--------+-+-----+-----+
|    |    |        | |     |     |
+----+----+--------+-+-----+-----+
```

datafile中保存完整k-v信息，包括crc校验，keysize，valuesize，mark，key，value

hintfile

```tex
  ks     of      k
+----+--------+-----+
|    |        |     |
+----+--------+-----+
```

hintfile中保存和对应的datafile中的k-v信息，包括keysize，offset，key

2. 内存

在内存中使用hash表，保存 key 到 datafile 的映射

```go
type item struct {
   fileID      int64 // 位于哪个datafile
   entryOffset int64 // 位于datafile中的offset
}

type Bitcask struct {
   index     map[string]*item
}
```

#### CRUD 实现

- get

  通过key从index映射中定位到该key最新的datafile位置以及offset，然后从datafile文件中读取完整的entry

- put、del、update

  这类操作在datafile中的表现都是，append追加一条日志，update相当于追加一个新的k-v记录覆盖原来，del相当于追加一个空val记录，并且mark标记为del

#### merge 实现

为了解决datafile增大，且许多key被覆盖或删除遗留的无用信息，使用merge合并datafile，减小磁盘占用

1. 首先开启一个新的临时mdb
2. 对当前db加锁，将index同步到mdb中，并且db开启新的datafile进行写入，然后当前db解锁。表示同步时间点为当前，在该时间点之后进行的db操作，将会写入到新的datafile文件中
3. 遍历临时mdb中的index，对于每一个key
    1. 从对应datafile中读取entry
    2. 将该entry在mdb中回放put操作
    3. 写入hintfile
4. 对当前db加锁，更新index中k-v的entry信息，指向新的datafile
5. 将临时datafile移动到当前db的目录中，并且删除掉不再使用的datafile
6. 当前db开启新的datafile进行写入，当前db解锁
7. merge完成

#### 重建

重新打开db，需要能从db的目录中读取datafile或hintfile重建index

1. 首先读取目录中的所有datafile和hintfile，datafile和hintfile的fileid是一一对应的
2. 优先从hintfile中重建，hintfile不存在再从datafile中重建
3. 对于hintfile，hintfile中保存的就是key和offset，因此直接读出然后写入到index中
4. 对于datafile，从datafile中读取完整的entry，然后构建新的item，写入到index中，如果读取到的key的mark标记为del，则表示该key被删除，因此在index中删除

### 参考

https://riak.com/assets/bitcask-intro.pdf

https://medium.com/@arpitbhayani/bitcask-a-log-structured-fast-kv-store-c6c728a9536b

https://github.com/roseduan/minidb

https://git.mills.io/prologic/bitcask