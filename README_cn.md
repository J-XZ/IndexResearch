# FB+-tree
本质上，FB+-tree 与内存型 B+-tree 相同，区别主要在于内部节点和叶子节点的布局方式。
类似于在 trie 中使用 bit 或 byte 进行分支操作，FB+-tree 在每一层内部节点中会逐步考虑公共前缀之后的若干字节，
这些字节被称为特征（features）。通过引入特征，FB+-tree 模糊了 B+-tree 与 trie 之间的边界，使其能够从前缀偏斜
（prefix skewness）中获益。在最理想的情况下，FB+-tree 几乎会退化为一棵 trie；而在最坏情况下，它仍然表现为一棵
B+-tree。在大多数情况下，分支操作都可以通过特征比较完成，从而提升缓存友好性。
此外，特征比较通过简单 `for` 循环中的 SIMD 指令实现；与二分查找相比，这种方式减轻了指令间依赖，使 FB+-tree 能够更好地利用计算并行性
（指令级并行和数据级并行）以及内存级并行，例如动态硬件调度、超标量、动态分支预测、推测执行以及一系列乱序执行技术。

在并发环境下，借助特征比较，FB+-tree 能有效缓解二分查找带来的细粒度、随机且不规则的内存访问，因此显著提升内存带宽和
Ultra Path Interconnect（UPI）带宽的利用率。最终，即使在只读负载下，FB+-tree 也比典型 B+-tree 具有更好的多核可扩展性。
与典型 B+-tree 将锚键（anchor keys）复制到内部节点不同，FB+-tree 将锚键的实际内容保存在叶子节点中
（即 `high_key`，上界），而内部节点只维护指向 `high_key` 的指针，这使得 FB+-tree 在空间上更加高效。
由于 `high_key` 仅表示叶子节点的上界，因此可以使用可区分前缀（discriminative prefixes）来构造它，以进一步优化性能和空间占用。

更多细节请参阅我们的 [VLDB 论文](https://www.vldb.org/pvldb/vol18/p1579-li.pdf)：
`````
Yuan Chen, Ao Li, Wenhai Li, and Lingfeng Deng:
FB+-tree: A Memory-Optimized B+-tree with Latch-Free Update. PVLDB 18(6): 1579-1592, 2025
`````

# Synchronization Protocol
FB+-tree 为并发索引访问采用了一种高度优化的乐观同步协议。
其特点包括：
* 无锁存（latch-free）的索引遍历，并且在大多数情况下无需承担与 `high_key` 比较的开销
* **高度可扩展的无锁存更新（通过精细的原子操作与乐观锁协同实现）**
* 基于叶子节点链表的并发线性化范围扫描，以及惰性重排

# Index Structures
每个索引目录中都包含一个示例。
* [ARTOLC](https://github.com/wangziqi2016/index-microbench.git)
* BLinkTree：基于锁的 B-link-tree，按照 YAO 等人的论文实现，仅为演示版本，可能存在一些问题
* [B+-treeOLC](https://github.com/wangziqi2016/index-microbench.git)
* [FAST](https://github.com/RyanMarcus/fast64.git)：一个使用 Rust 编写的 FAST 简化实现，仅支持 `bulk_load`
* FB+-tree
* [GoogleBtree](https://code.google.com/archive/p/cpp-btree/)
* [HOT](https://github.com/speedskater/hot.git)
* [Masstree](https://github.com/kohler/masstree-beta.git)
* [ARTOptiQL](https://github.com/sfu-dis/optiql)
* [STX B+-tree](https://github.com/tlx/tlx.git)
* [Wormhole](https://github.com/wuxb45/wormhole.git)

# Requirements
* 支持 SSE2、AVX2 或 AVX512 指令集的 x86-64 CPU
* Intel Threading Building Blocks（TBB）`apt install libtbb-dev`
* jemalloc `apt install libjemalloc-dev`
* google-glog `apt install libgoogle-glog-dev`
* 支持 C++17 的编译器

# API
```
KVPair* lookup(KeyType key)

KVPair* update(KVPair* kv)

KVPair* upsert(KVPair* kv)

KVPair* remove(KeyType key)

iterator begin()

iterator lower_bound(KeyType key)

iterator upper_bound(KeyType key)
```

# Get Started
1. 克隆本仓库并初始化子模块
```
git clone <repository name>
cd <repository name>
git submodule init
git submodule update
```
2. 创建新目录 `build`：`mkdir build && cd build`
3. 构建项目：`cmake -DCMAKE_BUILD_TYPE=Release .. && make -j`
4. 运行示例：`./FBTree/FBTreeExample 10000000 1 1`

# Notes
* 当前尚未实现单线程版本。后续我们会实现一个带有更多优化的单线程版本，例如将键值对嵌入叶子节点、使用更大的叶子节点（128），以及更合理的分裂策略。
* 若要评估并发 `remove` 的性能/可扩展性，请禁用 `free` 接口，以减轻跨线程内存释放的开销（例如 jemalloc 中对 arena 加锁）
* 开发过程中的早期实现： https://gitee.com/spearNeil/blinktree.git 和 https://gitee.com/spearNeil/tree-research.git
