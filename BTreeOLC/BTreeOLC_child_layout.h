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

inline void cpu_relax() {
#if defined(__aarch64__) || defined(__arm64__)
  __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#else
  std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

static const uint64_t pageSize = 4 * 1024;

/*
B+ 树里每个节点自带的“乐观锁 + 版本号”组件。
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
  std::atomic<uint64_t> typeVersionLockObsolete{0b100};

  bool isLocked(uint64_t version) { return ((version & 0b10) == 0b10); }

  uint64_t readLockOrRestart(bool &needRestart) {
    uint64_t version;
    version = typeVersionLockObsolete.load();
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

  void checkOrRestart(uint64_t startRead, bool &needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool &needRestart) const {
    needRestart = (startRead != typeVersionLockObsolete.load());
  }

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
 * @brief 在NodeBase的基础上定义了一个静态常量
 * typeMarker，表示这是一个叶子节点。
 *
 */
struct BTreeLeafBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeLeaf;
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

  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase)) / (sizeof(KeyValueType));

  // 这是我们进行搜索的数组，存储了实际的键值对
  KeyValueType data[maxEntries];

  BTreeLeaf() {
    count = 0;  // 一个新的叶子节点开始时没有任何键值对，所以 count 初始化为 0
    type = typeMarker;
  }

  bool isFull() { return count == maxEntries; };

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

  void insert(Key k, Payload p) {
    assert(count < maxEntries);
    if (count) {
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
    } else {
      data[0].first = k;
      data[0].second = p;
    }
    count++;
  }

  BTreeLeaf *split(Key &sep) {
    BTreeLeaf *newLeaf = new BTreeLeaf();
    newLeaf->count = count - (count / 2);
    count = count - newLeaf->count;
    memcpy(newLeaf->data, data + count, sizeof(KeyValueType) * newLeaf->count);
    // memcpy(newLeaf->payloads, payloads+count,
    // sizeof(Payload)*newLeaf->count);
    sep = data[count - 1].first;
    return newLeaf;
  }
};

struct BTreeInnerBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeInner;
};

template <class Key>
struct BTreeInner : public BTreeInnerBase {
  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(NodeBase *));
  NodeBase *children[maxEntries];
  Key keys[maxEntries];

  BTreeInner() {
    count = 0;
    type = typeMarker;
  }

  bool isFull() { return count == (maxEntries - 1); };

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
  std::atomic<NodeBase *> root;

  BTree() { root = new BTreeLeaf<Key, Value>(); }

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

      // Current node
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

        // Split eagerly if full
        if (inner->isFull()) {
          if (parent) {
            parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
            if (needRestart) {
              restart = true;
              break;
            }
          }
          node->upgradeToWriteLockOrRestart(versionNode, needRestart);
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

      auto leaf = static_cast<BTreeLeaf<Key, Value> *>(node);

      if (leaf->count == leaf->maxEntries) {
        if (parent) {
          parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
          if (needRestart) {
            continue;
          }
        }
        node->upgradeToWriteLockOrRestart(versionNode, needRestart);
        if (needRestart) {
          if (parent) {
            parent->writeUnlock();
          }
          continue;
        }
        if (!parent && (node != root)) {  // there's a new parent
          node->writeUnlock();
          continue;
        }
        Key sep;
        BTreeLeaf<Key, Value> *newLeaf = leaf->split(sep);
        if (parent) {
          parent->insert(sep, newLeaf);
        } else {
          makeRoot(sep, leaf, newLeaf);
        }
        node->writeUnlock();
        if (parent) {
          parent->writeUnlock();
        }
        continue;
      }

      // only lock leaf node
      node->upgradeToWriteLockOrRestart(versionNode, needRestart);
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
      leaf->insert(k, v);
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
      if (restartCount++) yield(restartCount);
      bool needRestart = false;
      bool restart = false;

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
        if (count == range) break;
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
