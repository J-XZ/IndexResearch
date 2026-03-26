/**
 * BTreeOLC_child_layout.h - This file contains a modified version that
 *                           uses the key-value pair layout
 *
 * We use this to test whether child node layout will affect performance
 *
 * 这个文件包含一个修改版本的 B+ 树实现，
 * 使用了键值对的布局。我们用它来测试子节点布局是否会影响性能。
 */

#pragma once

#include <sched.h>

#include <atomic>
#include <cassert>
#include <cstring>
// std::pair
#include <utility>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

namespace btreeolc {

/**
 * @brief 轻量级自旋等待指令。
 *
 * 在并发冲突时，不会立刻陷入重量级阻塞，而是先执行一个非常短的
 * “放松 CPU” 指令，降低忙等对流水线和功耗的影响。
 *
 * 不同架构使用不同实现：
 * - ARM64: `yield`
 * - x86: `_mm_pause()`
 * - 其他平台：退化为编译器层面的栅栏
 */
inline void cpu_relax() {
#if defined(__aarch64__) || defined(__arm64__)
  __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

/// 节点类型：内部节点或叶子节点。
enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

/// 这里假设每个节点按 4KB 页大小组织，类似数据库/索引常见做法。
static const uint64_t pageSize = 4 * 1024;

/**
 * @brief B+ 树里每个节点自带的“乐观锁 + 版本号”组件。
它不提供传统互斥锁语义，而是给节点维护一个 64 位状态字
typeVersionLockObsolete，让读线程先乐观读取，最后再校验版本是否变化；
  写线程则通过 CAS 抢占写锁。

  这个状态字的位语义从代码可以直接看出来：

  - bit 0：obsolete 位
    - 0 表示正常，1 表示这个节点已经废弃了，不应该再被访问了（逻辑删除标记）
  - bit 1：locked 位
  - 更高位：版本号，每次解锁或废弃节点时递增，供读线程检测
      “我读的这份内容期间有没有被修改过”

  各个方法的作用是：

  readLockOrRestart()
  它不是加真正的读锁，只是读一次版本字：

  - 如果节点当前被写锁持有，或者已经废弃，就把 needRestart=true
  - 否则返回当前版本，调用方继续读节点内容
    后面读完后再用这个版本做一致性校验。

  upgradeToWriteLockOrRestart()
  这是核心写锁获取逻辑：

  - 传入调用方之前读到的版本号
  - 用 compare_exchange_strong 把状态从“旧版本”改成“旧版本 + 0b10”
  - +0b10 的效果是把 locked 位设为 1
  - 如果 CAS 失败，说明期间有人改过这个节点，当前线程要重试

  writeLockOrRestart()
  这是个便捷封装：

  - 先做一次 readLockOrRestart()
  - 再尝试升级到写锁

  writeUnlock()
  通过 fetch_add(0b10) 解锁：

  - locked 位从 1 变回 0
  - 同时版本号也前进一次
    这样之前乐观读到旧版本的线程，在最后校验时会发现版本变化并重试。

  readUnlockOrRestart()
  读线程“完成读取后”的校验步骤：

  - 比较开始读时的版本 startRead
  - 和当前状态字是否完全相等
  - 不相等就说明节点在读取期间被改过，必须重启

  checkOrRestart()
  只是 readUnlockOrRestart() 的别名，调用点更贴近“做一次检查”的语义。

  writeUnlockObsolete()
  fetch_add(0b11) 同时：

  - 清掉写锁
  - 标记节点为 obsolete
  - 推进版本
    通常用于节点被替换、退休后不应再被访问的场景。这个文件里目前基本没真正用上.

  它在树里的工作方式大概是：

  1. 读线程先对当前节点调用 readLockOrRestart()
  2. 乐观读取 keys/children/data
  3. 下探前或返回前调用 checkOrRestart() / readUnlockOrRestart()
  4. 如果版本变了就整次操作重试

  写线程则是：

  1. 先乐观读到节点版本
  2. 需要修改时调用 upgradeToWriteLockOrRestart()
  3. 修改节点内容
  4. 调用 writeUnlock() 发布修改

  所以它本质上不是“阻塞式读写锁”，而是 OLC，optimistic lock coupling：

  - 读路径尽量不加真正锁，只做版本校验
  - 写路径只在必要节点上短暂持有写锁
  - 冲突时靠 restart 重试
*/
struct OptLock {
  /**
   * 这个 64 位状态字同时承载三类信息：
   * - bit0: obsolete，节点是否已失效
   * - bit1: locked，节点是否正被写线程持有
   * - 高位: version，节点版本号
   *
   * 初始值 0b100 的含义是：
   * - locked = 0
   * - obsolete = 0
   * - version 从一个非零值开始
   */
  std::atomic<uint64_t> typeVersionLockObsolete{0b100};

  /**
   * @brief 检查节点版本号的第二低位是否为 1，表示节点当前被加锁
   *
   * @param version 节点的版本号
   * @return true 节点被加锁
   * @return false 节点未被加锁
   */
  bool isLocked(uint64_t version) { return ((version & 0b10) == 0b10); }

  /**
   * @brief 乐观读入口。
   *
   * 它并不真正“加读锁”，而是读一次状态字：
   * - 如果节点当前正被写线程修改，或者已经 obsolete，则要求上层重试
   * - 否则返回当前版本号，调用方在读取节点内容后再做一次版本校验
   */
  uint64_t readLockOrRestart(bool &needRestart) {
    // 读取节点的版本号值
    uint64_t version;
    version = typeVersionLockObsolete.load();

    // 如果节点当前被写线程持有，或者已经废弃了，
    // 就说明除了调用方之外还有其他线程正在修改这个节点，当前线程的读取结果可能不一致，必须重试。
    // 这里调用方线程会通过cpu_relax()先短暂等待一下，降低忙等对 CPU 的影响，
    // 然后把 needRestart 标记为 true，通知调用方重试。
    if (isLocked(version) || isObsolete(version)) {
      cpu_relax();
      needRestart = true;
    }

    return version;
  }

  void writeLockOrRestart(bool &needRestart) {
    uint64_t version;
    version = readLockOrRestart(needRestart);
    if (needRestart) return;

    upgradeToWriteLockOrRestart(version, needRestart);
    if (needRestart) return;
  }

  /**
   * @brief 将“我刚才读到的那个版本”升级为写锁。
   *
   * 这里依赖 CAS：
   * - 若状态仍是我先前看到的 version，说明期间没人改过它，可以安全加写锁
   * - 若 CAS 失败，说明发生并发竞争，调用方必须整次操作重试
   */
  void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart) {
    if (typeVersionLockObsolete.compare_exchange_strong(version,
                                                        version + 0b10)) {
      version = version + 0b10;
    } else {
      cpu_relax();
      needRestart = true;
    }
  }

  void writeUnlock() { typeVersionLockObsolete.fetch_add(0b10); }

  bool isObsolete(uint64_t version) { return (version & 1) == 1; }

  /// 语义化包装：本质上就是“读完后校验版本是否没变”。
  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  /**
   * @brief 读路径的收尾检查。
   *
   * 如果开始读节点时拿到的版本号，与当前版本不同，
   * 就说明本次读取期间该节点被并发修改过，当前结果不能信任，必须重试。
   */
  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    needRestart = (startRead != typeVersionLockObsolete.load());
  }

  /// 解除写锁并顺便把节点标记为
  /// obsolete。当前文件里预留了这个能力，但几乎没实际用到。
  void writeUnlockObsolete() { typeVersionLockObsolete.fetch_add(0b11); }
};

/**
 * @brief NodeBase 包含：

 一个乐观锁（std::atomic<uint64_t>）

 一个 PageType（BTreeInner 或 BTreeLeaf）（uint8_t）

 一个 count 对内部节点表示分隔键的数量，对于叶子节点表示元素数量（uint16_t）
 */
struct NodeBase : public OptLock {
  PageType type;
  uint16_t count;
};

/**
 * @brief 叶节点：
 * 在NodeBase的基础上定义了一个静态常量
 * typeMarker，表示这是一个叶子节点。
 */
struct BTreeLeafBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeLeaf;
};

/**
 * @brief 内部节点：
 * 在NodeBase的基础上定义了一个静态常量
 * typeMarker，表示这是一个内部节点。
 */
struct BTreeInnerBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeInner;
};

/**
 * @brief BTreeLeaf 继承了 BTreeLeafBase，
 并且添加了一个静态常量 maxEntries，表示每个叶子节点最多能存储多少个键值对。
 还有一个数组 data，存储了实际的键值对。
 每个键值对是一个 std::pair<Key, Payload>。
 *
 * @tparam Key
 * @tparam Payload
 */
template <class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
  // 这是叶子节点存储的元素类型：一个键值对
  using KeyValueType = std::pair<Key, Payload>;

  // 叶子节点能容纳多少元素，取决于页大小减去公共头部之后剩余的空间。
  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase)) / (sizeof(KeyValueType));

  // 这是我们进行搜索的数组，存储了实际的键值对
  // 这个实现没有做键值分离，所以键和值的布局是交错的，存储在同一个数组里
  KeyValueType data[maxEntries];

  BTreeLeaf() {
    count = 0;  // 一个新的叶子节点开始时没有任何键值对，所以 count 初始化为 0
    type = typeMarker;
  }

  bool isFull() { return count == maxEntries; };

  /**
   * @brief 在有序叶子数组中做 lower_bound。
   *
   * 返回值语义与 STL 的 lower_bound 一致：
   * - 若找到相等 key，返回其位置
   * - 否则返回“第一个大于 k 的位置”
   *
   * 这个位置既可用于 lookup，也可直接用于 insert 时决定新元素插入点。
   */
  unsigned lowerBound(Key k) {
    unsigned lower = 0;
    unsigned upper = count;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      // 这是我们进行比较的基准键，也就是当前二分搜索的中间位置的键
      const Key &middle_key = data[mid].first;

      if (k < middle_key) {
        upper = mid;
      } else if (k > middle_key) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  /**
   * @brief 向叶子节点插入一个键值对。
   *
   * 这里的语义是 upsert：
   * - 若 key 已存在，则覆盖 value
   * - 若 key 不存在，则保持有序地插入新元素
   *
   * 由于数据布局是 `pair<Key, Payload>` 数组，所以一次 `memmove`
   * 即可整体挪动键和值。
   */
  void insert(Key k, Payload p) {
    assert(count < maxEntries);
    if (count) {  // 如果当前叶子非空，找到要插入的位置，搬运数组中的元素，然后插入
      unsigned pos = lowerBound(k);
      if ((pos < count) && (data[pos].first == k)) {
        // Upsert
        data[pos].second = p;
        return;
      }
      memmove(data + pos + 1, data + pos, sizeof(KeyValueType) * (count - pos));
      // memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
      data[pos].first = k;
      data[pos].second = p;
    } else {  // 否则直接插入一个元素
      data[0].first = k;
      data[0].second = p;
    }
    count++;
  }

  /**
   * @brief 分裂叶子节点。
   *
   * 分裂策略：
   * - 新建一个右侧叶子 `newLeaf`
   * - 将后半部分元素拷贝到新叶子
   * - 当前叶子保留前半部分
   * - `sep` 输出为左叶子最后一个 key，用作插入父节点的分隔键
   *
   * 这是 B+ 树插入时的核心步骤之一。
   */
  BTreeLeaf *split(Key &sep) {
    BTreeLeaf *newLeaf = new BTreeLeaf();
    newLeaf->count = count - (count / 2);
    count = count - newLeaf->count;
    memcpy(newLeaf->data, data + count, sizeof(KeyValueType) * newLeaf->count);
    // memcpy(newLeaf->payloads, payloads+count,
    // sizeof(Payload)*newLeaf->count);
    // 叶子节点分裂以后产生1个分隔键，是分裂后左侧节点中的最大键
    sep = data[count - 1].first;
    return newLeaf;
  }
};

/**
 * @brief 继承 BTreeInnerBase，
 * 并且添加了一个静态常量maxEntries，表示每个内部节点最多能存储多少个分隔键。
 * 另外包含一个 children 数组，存储指向子节点的指针，
 * 以及一个 keys 数组，存储分隔键。
 *
 * 备注：children 数组的大小是 maxEntries + 1，
 * 因为一个内部节点有 count 个分隔键，对应 count + 1 个子节点指针。
 *
 * @tparam Key
 */
template <class Key>
struct BTreeInner : public BTreeInnerBase {
  // 内部节点中有 count 个 key，对应 count + 1 个 child pointer。
  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(NodeBase *));

  NodeBase *children[maxEntries];
  Key keys[maxEntries];

  BTreeInner() {
    count = 0;
    type = typeMarker;
  }

  bool isFull() { return count == (maxEntries - 1); };

  /**
   * @brief 一个分支预测更友好的 lower_bound 变体。
   *
   * 当前实现里没有实际使用它，保留在这里只是提供另一种节点内搜索写法。
   */
  unsigned lowerBoundBF(Key k) {
    auto base = keys;
    unsigned n = count;
    while (n > 1) {
      const unsigned half = n / 2;
      base = (base[half] < k) ? (base + half) : base;
      n -= half;
    }
    return (*base < k) + base - keys;
  }

  /**
   * @brief 在内部节点的分隔键数组上做二分搜索。
   *
   * 返回的位置既可用于：
   * - 查找下一跳 child
   * - 向内部节点插入新的分隔键
   */
  unsigned lowerBound(Key k) {
    unsigned lower = 0;
    unsigned upper = count;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (k < keys[mid]) {
        upper = mid;
      } else if (k > keys[mid]) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  /**
   * @brief 分裂内部节点。
   *
   * 与叶子分裂不同，内部节点分裂时：
   * - 中间那个 key 会“上推”给父节点，保存在 `sep`
   * - 左右两个内部节点都不再保留这个被上推的 key
   */
  BTreeInner *split(Key &sep) {
    BTreeInner *newInner = new BTreeInner();
    newInner->count = count - (count / 2);
    count = count - newInner->count - 1;
    sep = keys[count];
    memcpy(newInner->keys, keys + count + 1,
           sizeof(Key) * (newInner->count + 1));
    memcpy(newInner->children, children + count + 1,
           sizeof(NodeBase *) * (newInner->count + 1));
    return newInner;
  }

  /**
   * @brief 向内部节点插入一个新的分隔键和右孩子指针。
   *
   * 插入完成后要维护 B+ 树内部节点的经典关系：
   * - keys 有序
   * - children 数量始终比 keys 多 1
   */
  void insert(Key k, NodeBase *child) {
    assert(count < maxEntries - 1);
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
    memmove(children + pos + 1, children + pos,
            sizeof(NodeBase *) * (count - pos + 1));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    count++;
  }
};

template <class Key, class Value>
struct BTree {
  /// root 也做成原子变量，是为了让根分裂时的替换对并发线程可见。
  std::atomic<NodeBase *> root;

  BTree() { root = new BTreeLeaf<Key, Value>(); }

  /**
   * @brief 当根节点分裂时创建一个全新的根。
   *
   * 分裂前树高为 h，分裂后树高增加为 h + 1：
   * - 左孩子指向旧根
   * - 右孩子指向新分裂出的节点
   * - 中间的分隔键放在新根里
   */
  void makeRoot(Key k, NodeBase *leftChild, NodeBase *rightChild) {
    auto inner = new BTreeInner<Key>();
    inner->count = 1;
    inner->keys[0] = k;
    inner->children[0] = leftChild;
    inner->children[1] = rightChild;
    root = inner;
  }

  /**
   * @brief 在适当的时候调用 yield()，让出 CPU 给其他线程，避免长时间占用 CPU
   * 导致饥饿。
   *
   * @param count 当前线程已经重试的次数，根据这个次数来决定是否调用
   * sched_yield() 或者 cpu_relax()。
   */
  void yield(int count) {
    // 如果重试次数超过 3 次，就调用 sched_yield() 让出 CPU 给其他线程
    if (count > 3) {
      sched_yield();
    } else {
      // 否则调用 cpu_relax() 进行短暂的忙等待，减少自旋对 CPU 的影响
      cpu_relax();
    }
  }

  void insert(Key k, Value v) {
    int restartCount = 0;
    while (true) {
      if (restartCount++) {  // 如果不是第一次尝试插入，
                             // 就调用 yield() 让出 CPU 或者进行短暂的忙等待
                             // 第一次显然不应该等待，因为根本没有冲突，直接尝试插入就好
        yield(restartCount);
      }

      bool needRestart = false;
      bool restart = false;

      // 从根节点开始，准备进行一次完整的“乐观遍历 + 必要时重试”
      NodeBase *node = root;
      uint64_t nodeVersion = node->readLockOrRestart(needRestart);

      // 如果需要重试，或者在读版本时发现根节点已经被替换了，
      // 就要从头开始重试整个插入过程
      if (needRestart || (node != root)) {
        continue;
      }

      // Parent of current node
      BTreeInner<Key> *parent = nullptr;
      uint64_t versionParent = 0;

      // 先在inner节点上一路向下走，直到到达叶子节点。
      while (node->type == PageType::BTreeInner) {
        auto inner = static_cast<BTreeInner<Key> *>(node);

        // 采用“eager split”策略：沿路向下时，若发现内部节点已满，先分裂再继续。
        // 这样到达叶子后，通常就不需要回溯向上调整父节点了。
        if (inner->isFull()) {
          if (parent) {
            parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
            if (needRestart) {
              restart = true;
              break;
            }
          }
          node->upgradeToWriteLockOrRestart(nodeVersion, needRestart);
          if (needRestart) {
            if (parent) {
              parent->writeUnlock();
            }
            restart = true;
            break;
          }
          if (!parent && (node != root)) {  // there's a new parent
            node->writeUnlock();
            restart = true;
            break;
          }

          // 分裂当前内部节点，并将分隔键插入父节点；如果当前就是根，则新建根。
          Key sep;
          BTreeInner<Key> *newInner = inner->split(sep);
          if (parent) {
            parent->insert(sep, newInner);
          } else {
            makeRoot(sep, inner, newInner);
          }
          node->writeUnlock();
          if (parent) {
            parent->writeUnlock();
          }
          restart = true;
          break;
        }

        if (parent) {
          parent->readUnlockOrRestart(versionParent, needRestart);
          if (needRestart) {
            restart = true;
            break;
          }
        }

        parent = inner;
        versionParent = nodeVersion;

        // 根据 key 在内部节点中选择下一跳 child。
        node = inner->children[inner->lowerBound(k)];
        inner->checkOrRestart(nodeVersion, needRestart);
        if (needRestart) {
          restart = true;
          break;
        }
        nodeVersion = node->readLockOrRestart(needRestart);
        if (needRestart) {
          restart = true;
          break;
        }
      }

      // 在内部节点遍历的过程中，如果任何一步需要重试，我们都直接设置 restart
      // 标记并跳出循环， 最后在外层根据 restart 标记决定是否重试整个插入过程。

      // 在遍历内部节点的过程中，会维护最后访问到的叶子节点的父节点指针 parent
      // 和版本号 versionParent

      if (restart) {
        continue;
      }

      // 走到这里说明已经到达叶子节点。
      auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);

      // 叶子满了则先分裂，分裂成功后整次操作重试。
      // 重试后会沿着最新树结构重新找到正确叶子。

      // 如果叶子满了
      if (leaf->count == leaf->maxEntries) {
        if (parent) {  // 如果这个叶子节点有父节点，就先尝试锁住父节点，准备修改它的分隔键和孩子指针
          parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
          if (needRestart) {
            continue;
          }
        }

        // 锁住当前叶子节点，准备分裂它
        node->upgradeToWriteLockOrRestart(nodeVersion, needRestart);

        if (needRestart) {
          // 如果尝试锁住当前叶子节点失败了，说明期间有其他线程修改过它，我们就直接重试整个插入过程。
          // 别忘了如果之前锁住了父节点，要先解锁父节点。
          if (parent) {
            parent->writeUnlock();
          }
          continue;
        }

        // 如果当前线程本来以为自己处理的是根节点，但在它准备修改这个节点时，
        // 发现树的根已经被别的线程换掉了，所以这次操作不能继续，必须释放锁并重试。
        if (!parent && (node != root)) {  // there's a new parent
          node->writeUnlock();
          continue;
        }

        // 排除了上述各种冲突和竞争情况后，当前线程成功锁住了需要分裂的叶子节点（以及它的父节点，如果有的话），
        // 可以安全地进行分裂操作了。

        Key sep;
        BTreeLeaf<Key, Value> *newLeaf = leaf->split(sep);

        // 如果当前要分裂的叶子节点有父节点，就把分隔键和新叶子插入父节点；
        // 如果没有父节点，说明当前叶子就是根，分裂后需要新建一个根。
        if (parent) {
          parent->insert(sep, newLeaf);
        } else {
          // 创建根节点的过程其实就是创建一个内部节点，
          // 这个内部节点有两个指向孩子节点的指针和一个分隔键
          // 分隔键等于左侧孩子节点的最大键。
          // 这里的分割键类似平衡二叉树中内部节点的值
          // 所有的左孩子都小于等于这个值，右孩子都大于这个值
          makeRoot(sep, leaf, newLeaf);
        }

        node->writeUnlock();
        if (parent) {
          parent->writeUnlock();
        }
        continue;
      }

      // 叶子未满时，只需锁住叶子本身即可完成插入，父节点保持乐观读验证。
      node->upgradeToWriteLockOrRestart(nodeVersion, needRestart);
      if (needRestart) {
        continue;
      }
      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) {
          node->writeUnlock();
          continue;
        }
      }

      // 实际在一个叶子中插入kv。
      leaf->insert(k, v);

      // 解锁叶子节点
      node->writeUnlock();

      return;  // success
    }
  }

  bool lookup(Key k, Value &result) {
    int restartCount = 0;
    while (true) {
      if (restartCount++) yield(restartCount);
      bool needRestart = false;
      bool restart = false;

      // lookup 是纯读路径：一路乐观读下去，最后通过版本校验确认中途没被修改。
      NodeBase *node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node != root)) {
        continue;
      }

      // Parent of current node
      BTreeInner<Key> *parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type == PageType::BTreeInner) {
        auto inner = static_cast<BTreeInner<Key> *>(node);

        if (parent) {
          parent->readUnlockOrRestart(versionParent, needRestart);
          if (needRestart) {
            restart = true;
            break;
          }
        }

        parent = inner;
        versionParent = versionNode;

        node = inner->children[inner->lowerBound(k)];
        inner->checkOrRestart(versionNode, needRestart);
        if (needRestart) {
          restart = true;
          break;
        }
        versionNode = node->readLockOrRestart(needRestart);
        if (needRestart) {
          restart = true;
          break;
        }
      }

      if (restart) {
        continue;
      }

      // 到达叶子后，做一次节点内二分查找。
      BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
      unsigned pos = leaf->lowerBound(k);
      bool success = false;
      if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
        success = true;
        result = leaf->data[pos].second;
      }
      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) {
          continue;
        }
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        continue;
      }

      return success;
    }
  }

  uint64_t scan(Key k, int range, Value *output) {
    int restartCount = 0;
    while (true) {
      if (restartCount++) {
        yield(restartCount);
      }
      bool needRestart = false;
      bool restart = false;

      // scan 先像 lookup 一样定位到包含 lower bound 的叶子，
      // 然后在当前实现里只扫描这个叶子内部的连续元素。
      // 注意：这里不是完整跨叶链表的范围扫描，只是单叶扫描。
      NodeBase *node = root;
      uint64_t versionNode = node->readLockOrRestart(needRestart);
      if (needRestart || (node != root)) {
        continue;
      }

      // Parent of current node
      BTreeInner<Key> *parent = nullptr;
      uint64_t versionParent = 0;

      while (node->type == PageType::BTreeInner) {
        auto inner = static_cast<BTreeInner<Key> *>(node);

        if (parent) {
          parent->readUnlockOrRestart(versionParent, needRestart);
          if (needRestart) {
            restart = true;
            break;
          }
        }

        parent = inner;
        versionParent = versionNode;

        node = inner->children[inner->lowerBound(k)];
        inner->checkOrRestart(versionNode, needRestart);
        if (needRestart) {
          restart = true;
          break;
        }
        versionNode = node->readLockOrRestart(needRestart);
        if (needRestart) {
          restart = true;
          break;
        }
      }

      if (restart) {
        continue;
      }

      BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
      unsigned pos = leaf->lowerBound(k);
      int count = 0;
      for (unsigned i = pos; i < leaf->count; i++) {
        if (count == range) {
          break;
        }
        output[count++] = leaf->data[i].second;
      }

      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) {
          continue;
        }
      }
      node->readUnlockOrRestart(versionNode, needRestart);
      if (needRestart) {
        continue;
      }

      return count;
    }
  }
};

}  // namespace btreeolc
