// A basic B^e-tree implementation templated on types Key and Value.
// Keys and Values must be serializable (see swap_space.hpp).
// Keys must be comparable (via operator< and operator==).
// Values must be addable (via operator+).
// See test.cpp for example usage.

// 以 Key（键）和 Value（值）为模板参数的基础 Bᵉ 树实现。
// 键和值必须支持序列化（参见 swap_space.hpp）。
// 键必须支持比较运算（通过 operator< 和 operator==）。
// 值必须支持加法运算（通过 operator+）。
// 示例用法参见 test.cpp。

// This implementation represents in-memory nodes as objects with two
// fields:
// - a std::map mapping keys to child pointers
// - a std::map mapping (key, timestamp) pairs to messages
// Nodes are de/serialized to/from an on-disk representation.
// I/O is managed transparently by a swap_space object.

// 本实现将内存中的节点表示为包含两个字段的对象：
// - 一个 std::map，建立键到子节点指针的映射
// - 一个 std::map，建立（键，时间戳）对到消息的映射
// 节点可与磁盘存储格式进行序列化/反序列化转换。
// 输入输出操作由 swap_space 对象透明管理。

// This implementation deviates from a "textbook" implementation in
// that there is not a fixed division of a node's space between pivots
// and buffered messages.

// 该实现与“教科书式”实现有所不同，
// 原因是节点空间并未在 pivot（枢纽/分界点）与缓冲消息之间进行固定划分。

// In a textbook implementation, nodes have size B, B^e space is
// devoted to pivots and child pointers, and B-B^e space is devoted to
// buffering messages.  Whenever a leaf gets too many messages, it
// splits.  Whenever an internal node gets too many messages, it
// performs a flush.  Whenever an internal node gets too many
// children, it splits.  This policy ensures that, whenever the tree
// needs to flush messages from a node to one of its children, it can
// always move a batch of size at least (B-B^e) / B^e = B^(1-e) - 1
// messages.

// 在教科书式实现中，节点大小固定为 B，其中 B^e
// 空间用于存储枢纽（pivot）和子节点指针， B-B^e 空间用于消息缓冲。
// 每当叶子节点的消息数量过多时，就会执行分裂操作；
// 每当内部节点的消息数量过多时，就会执行刷新（flush）操作；
// 每当内部节点的子节点数量过多时，也会执行分裂操作。
// 该策略保证：当树需要将消息从节点刷新到其子节点时，
// 总能批量移动至少 (B-B^e) / B^e = B^(1-e) - 1 条消息。

// In this implementation, nodes have a fixed maximum size.  Whenever
// a leaf exceeds this max size, it splits.  Whenever an internal node
// exceeds this maximum size, it checks to see if it can flush a large
// batch of elements to one of its children.  If it can, it does so.
// If it cannot, then it splits.

// 在本实现中，节点具有固定的最大容量。
// 每当叶子节点超出该最大容量时，就会执行分裂操作。
// 每当内部节点超出该最大容量时，会先检查是否能向其子节点批量刷新大量元素。
// 如果可以，则执行批量刷新；
// 如果不可以，则执行分裂操作。

// In-memory nodes may temporarily exceed the maximum size
// restriction.  During a flush, we move all the incoming messages
// into the destination node.  At that point the node may exceed the
// max size.  The flushing procedure then performs further flushes or
// splits to restore the max-size invariant.  Thus, whenever a flush
// returns, all the nodes in the subtree of that node are guaranteed
// to satisfy the max-size requirement.

// 内存中的节点可能会临时超出最大容量限制。
// 在执行刷新操作时，我们会将所有传入的消息移入目标节点。
// 此时该节点可能会超出最大容量。
// 随后，刷新流程会继续执行后续的刷新或分裂操作，以恢复最大容量不变性。
// 因此，每当一次刷新操作完成返回时，该节点子树中的所有节点都必定满足最大容量要求。

// This implementation also optimizes I/O based on which nodes are
// on-disk, clean in memory, or dirty in memory.  For example,
// inserted items are always immediately flushed as far down the tree
// as they can go without dirtying any new nodes.  This is because
// flushing an item to a node that is already dirty will not require
// any additional I/O, since the node already has to be written back
// anyway.  Furthermore, it will flush smaller batches to clean
// in-memory nodes than to on-disk nodes.  This is because dirtying a
// clean in-memory node only requires a write-back, whereas flushing
// to an on-disk node requires reading it in and writing it out.

// 本实现还会根据节点的状态（磁盘节点、内存干净节点、内存脏节点）对 I/O
// 进行优化。
// 例如：插入的数据项会立即沿着树结构尽可能向下刷新，且不会污染任何新的节点。
// 这是因为将数据项刷新到已为脏节点的节点上，不会产生任何额外 I/O 开销——
// 该节点原本就需要被写回磁盘。
// 此外，相比于磁盘节点，本实现会向内存干净节点刷新更小的批量数据。
// 这是因为将内存干净节点标记为脏节点仅需要一次写回操作，
// 而刷新数据到磁盘节点则需要先将节点读入内存，再写回磁盘。

#include "backing_store.hpp"
#include "swap_space.hpp"
#include <cassert>
#include <cstdint>
#include <map>
#include <vector>

////////////////// Upserts

// Internally, we store data indexed by both the user-specified key
// and a timestamp, so that we can apply upserts in the correct order.
template <class Key> class MessageKey {
public:
  MessageKey(void) : key(), timestamp(0) {}

  MessageKey(const Key &k, uint64_t tstamp) : key(k), timestamp(tstamp) {}

  static MessageKey range_start(const Key &key) { return MessageKey(key, 0); }

  static MessageKey range_end(const Key &key) {
    return MessageKey(key, UINT64_MAX);
  }

  MessageKey range_start(void) const { return range_start(key); }

  MessageKey range_end(void) const { return range_end(key); }

  void _serialize(std::iostream &fs, serialization_context &context) const {
    fs << timestamp << " ";
    serialize(fs, context, key);
  }

  void _deserialize(std::iostream &fs, serialization_context &context) {
    fs >> timestamp;
    deserialize(fs, context, key);
  }

  Key key;
  uint64_t timestamp;
};

template <class Key>
bool operator<(const MessageKey<Key> &mkey1, const MessageKey<Key> &mkey2) {
  return mkey1.key < mkey2.key ||
         (mkey1.key == mkey2.key && mkey1.timestamp < mkey2.timestamp);
}

template <class Key>
bool operator<(const Key &key, const MessageKey<Key> &mkey) {
  return key < mkey.key;
}

template <class Key>
bool operator<(const MessageKey<Key> &mkey, const Key &key) {
  return mkey.key < key;
}

template <class Key>
bool operator==(const MessageKey<Key> &a, const MessageKey<Key> &b) {
  return a.key == b.key && a.timestamp == b.timestamp;
}

// The three types of upsert.  An UPDATE specifies a value, v, that
// will be added (using operator+) to the old value associated to some
// key in the tree.  If there is no old value associated with the key,
// then it will add v to the result of a Value obtained using the
// default zero-argument constructor.
//
// 三种类型的插入更新操作。
// UPDATE（更新操作）会指定一个值 v，
// 该值将通过加法运算符（+）与树中某个键对应的旧值相加。
// 如果该键不存在对应的旧值，
// 则会将 v 与通过默认无参构造函数创建的 Value 类型结果相加。
#define INSERT (0)
#define DELETE (1)
#define UPDATE (2)

template <class Value> class Message {
public:
  Message(void) : opcode(INSERT), val() {}

  Message(int opc, const Value &v) : opcode(opc), val(v) {}

  void _serialize(std::iostream &fs, serialization_context &context) {
    fs << opcode << " ";
    serialize(fs, context, val);
  }

  void _deserialize(std::iostream &fs, serialization_context &context) {
    fs >> opcode;
    deserialize(fs, context, val);
  }

  int opcode;
  Value val;
};

template <class Value>
bool operator==(const Message<Value> &a, const Message<Value> &b) {
  return a.opcode == b.opcode && a.val == b.val;
}

// Measured in messages.
#define DEFAULT_MAX_NODE_SIZE (1ULL << 18)

// The minimum number of messages that we will flush to an out-of-cache node.
// Note: we will flush even a single element to a child that is already dirty.
// Note: we will flush MIN_FLUSH_SIZE/2 items to a clean in-memory child.
#define DEFAULT_MIN_FLUSH_SIZE (DEFAULT_MAX_NODE_SIZE / 16ULL)

template <class Key, class Value> class betree {
private:
  class node;
  // We let a swap_space handle all the I/O.
  typedef typename swap_space::pointer<node> node_pointer;
  class child_info : public serializable {
  public:
    child_info(void) : child(), child_size(0) {}

    child_info(node_pointer child, uint64_t child_size)
        : child(child), child_size(child_size) {}

    void _serialize(std::iostream &fs, serialization_context &context) {
      serialize(fs, context, child);
      fs << " ";
      serialize(fs, context, child_size);
    }

    void _deserialize(std::iostream &fs, serialization_context &context) {
      deserialize(fs, context, child);
      deserialize(fs, context, child_size);
    }

    node_pointer child;
    uint64_t child_size;
  };
  typedef typename std::map<Key, child_info> pivot_map;
  typedef typename std::map<MessageKey<Key>, Message<Value>> message_map;

  class node : public serializable {
  public:
    // Child pointers
    pivot_map pivots;
    message_map elements;

    bool is_leaf(void) const { return pivots.empty(); }

    // Holy frick-a-moly.  We want to write a const function that
    // returns a const_iterator when called from a const function and
    // a non-const function that returns a (non-const_)iterator when
    // called from a non-const function.  And we don't want to
    // duplicate the code.  The following solution is from
    //         http://stackoverflow.com/a/858893
    template <class OUT, class IN> static OUT get_pivot(IN &mp, const Key &k) {
      assert(mp.size() > 0);
      auto it = mp.lower_bound(k);
      if (it == mp.begin() && k < it->first) {
        throw std::out_of_range("Key does not exist "
                                "(it is smaller than any key in DB)");
      }
      if (it == mp.end() || k < it->first) {
        --it;
      }
      return it;
    }

    // Instantiate the above template for const and non-const
    // calls. (template inference doesn't seem to work on this code)
    typename pivot_map::const_iterator get_pivot(const Key &k) const {
      return get_pivot<typename pivot_map::const_iterator, const pivot_map>(
          pivots, k);
    }

    typename pivot_map::iterator get_pivot(const Key &k) {
      return get_pivot<typename pivot_map::iterator, pivot_map>(pivots, k);
    }

    // Return iterator pointing to the first element with mk >= k.
    // (Same const/non-const templating trick as above)
    template <class OUT, class IN>
    static OUT get_element_begin(IN &elts, const Key &k) {
      return elts.lower_bound(MessageKey<Key>::range_start(k));
    }

    typename message_map::iterator get_element_begin(const Key &k) {
      return get_element_begin<typename message_map::iterator, message_map>(
          elements, k);
    }

    typename message_map::const_iterator get_element_begin(const Key &k) const {
      return get_element_begin<typename message_map::const_iterator,
                               const message_map>(elements, k);
    }

    // Return iterator pointing to the first element that goes to
    // child indicated by it
    typename message_map::iterator
    get_element_begin(const typename pivot_map::iterator it) {
      return it == pivots.end() ? elements.end() : get_element_begin(it->first);
    }

    // Apply a message to ourself.
    // be tree 的一个节点调用以下函数，将一个键值对操作应用到自己身上
    void apply(const MessageKey<Key> &mkey, const Message<Value> &elt,
               Value &default_value) {

      // range_start是用户键绑定上时间戳 0
      // range_end是用户键绑定上时间戳 UINT64_MAX
      // 分别表示一个用户键可能存在的最早和最晚的消息
      // 这个 be tree 实现不是 MVCC 的，
      // 因此写入一个键时，会删除该键之前的所有消息（不管时间戳），
      // 然后写入一个新的消息。
      // 新的消息的时间戳是递增的。

      switch (elt.opcode) {
      case INSERT:
        elements.erase(elements.lower_bound(mkey.range_start()),
                       elements.upper_bound(mkey.range_end()));
        elements[mkey] = elt;
        break;

      case DELETE:
        elements.erase(elements.lower_bound(mkey.range_start()),
                       elements.upper_bound(mkey.range_end()));
        if (!is_leaf()) {
          // 如果不是叶节点，删除标记需要一直下传到子节点中去
          elements[mkey] = elt;
        }
        break;

      case UPDATE: {
        // 找到第一个比当前key大的元素
        auto iter = elements.upper_bound(mkey.range_end());

        // 找到第一个小于等于当前key的元素
        if (iter != elements.begin()) {
          iter--;
        }

        // 如果第一个小于等于当前key的元素与当前key不相等
        if (iter == elements.end() || iter->first.key != mkey.key) {
          if (is_leaf()) {
            Value dummy = default_value;
            apply(mkey, Message<Value>(INSERT, dummy + elt.val), default_value);
          } else {
            elements[mkey] = elt;
          }
        } else {
          // 如果第一个小于等于当前key的元素与当前key相等
          assert(iter != elements.end() && iter->first.key == mkey.key);
          // 如果这个元素是一个插入操作，把之前插入的值和当前更新的值拼接，得到一个新的插入操作
          // @todo
          // 为什么要把之前插入的值和当前更新的值拼接起来？这不太符合直觉啊
          if (iter->second.opcode == INSERT) {
            apply(mkey, Message<Value>(INSERT, iter->second.val + elt.val),
                  default_value);
          } else {
            elements[mkey] = elt;
          }
        }
      } break;

      default:
        assert(0);
      }
    }

    // Requires: there are less than MIN_FLUSH_SIZE things in elements
    //           destined for each child in pivots);
    // 要求：在 pivots 中，分配给每个子节点的元素数量
    // 都必须小于 MIN_FLUSH_SIZE
    pivot_map split(betree &bet) {
      // [分裂]
      assert(pivots.size() + elements.size() >= bet.max_node_size);

      // This size split does a good job of causing the resulting
      // nodes to have size between 0.4 * MAX_NODE_SIZE and 0.6 * MAX_NODE_SIZE.
      //
      // 这种大小划分方式效果很好，
      // 能让最终生成的节点大小保持在 0.4 倍最大节点容量 到 0.6 倍最大节点容量
      // 之间。
      // 新节点的数量，保证分裂后每个新节点的大小在 0.4 到 0.6
      // 倍最大节点容量之间。
      // 这样不至于出现过多数量的新节点（如果每个新节点的大小都非常小），
      // 也不至于出现太满的新节点（如果每个新节点的大小都很接近最大节点容量）。
      int num_new_leaves =
          (pivots.size() + elements.size()) / (10 * bet.max_node_size / 24);
      // 每个新节点分配的内容量
      int things_per_new_leaf =
          (pivots.size() + elements.size() + num_new_leaves - 1) /
          num_new_leaves;

      pivot_map result;
      auto pivot_idx = pivots.begin();
      auto elt_idx = elements.begin();
      int things_moved = 0;
      // 在一个循环中完成所有新节点的分配和内容划分
      // 用 pivot_idx 和 elt_idx 两个迭代器分别遍历 pivots 和 elements，
      // 按照 things_per_new_leaf 的数量划分内容，分配给每个新节点。
      // 每一轮循环分配一个新节点
      // 然后先把一个枢轴（pivot）分配给新节点，再把这个枢轴对应的元素分配给新节点，
      // 直到分配的内容数量达到 things_per_new_leaf，或者 pivots 和 elements
      // 都被遍历完了。
      // 最终效果是均匀地把当前节点的数据（包含枢轴和子节点信息）
      // 均匀地划分到 num_new_leaves 个新节点中去。
      for (int i = 0; i < num_new_leaves; i++) {
        if (pivot_idx == pivots.end() && elt_idx == elements.end()) {
          break;
        }

        node_pointer new_node = bet.ss->allocate(new node);
        result[pivot_idx != pivots.end() ? pivot_idx->first
                                         : elt_idx->first.key] =
            child_info(new_node,
                       new_node->elements.size() + new_node->pivots.size());

        while (things_moved < (i + 1) * things_per_new_leaf &&
               (pivot_idx != pivots.end() || elt_idx != elements.end())) {
          if (pivot_idx != pivots.end()) {
            new_node->pivots[pivot_idx->first] = pivot_idx->second;
            ++pivot_idx;
            things_moved++;
            auto elt_end = get_element_begin(pivot_idx);
            while (elt_idx != elt_end) {
              new_node->elements[elt_idx->first] = elt_idx->second;
              ++elt_idx;
              things_moved++;
            }
          } else {
            // Must be a leaf
            assert(pivots.size() == 0);
            new_node->elements[elt_idx->first] = elt_idx->second;
            ++elt_idx;
            things_moved++;
          }
        }
      }

      for (auto it = result.begin(); it != result.end(); ++it) {
        it->second.child_size =
            it->second.child->elements.size() + it->second.child->pivots.size();
      }

      assert(pivot_idx == pivots.end());
      assert(elt_idx == elements.end());
      pivots.clear();
      elements.clear();
      // 返回一个映射表，包含一个索引多个分裂出来的新节点的枢轴键和子节点信息
      return result;
    }

    node_pointer merge(betree &bet, typename pivot_map::iterator begin,
                       typename pivot_map::iterator end) {
      node_pointer new_node = bet.ss->allocate(new node);
      for (auto it = begin; it != end; ++it) {
        new_node->elements.insert(it->second.child->elements.begin(),
                                  it->second.child->elements.end());
        new_node->pivots.insert(it->second.child->pivots.begin(),
                                it->second.child->pivots.end());
      }
      return new_node;
    }

    void merge_small_children(betree &bet) {

      if (is_leaf()) {
        return;
      }

      for (auto beginit = pivots.begin(); beginit != pivots.end(); ++beginit) {
        uint64_t total_size = 0;
        auto endit = beginit;
        while (endit != pivots.end()) {
          if (total_size + beginit->second.child_size >
              6 * bet.max_node_size / 10) {
            break;
          }
          total_size += beginit->second.child_size;
          ++endit;
        }
        if (endit != beginit) {
          node_pointer merged_node = merge(bet, beginit, endit);
          for (auto tmp = beginit; tmp != endit; ++tmp) {
            tmp->second.child->elements.clear();
            tmp->second.child->pivots.clear();
          }
          Key key = beginit->first;
          pivots.erase(beginit, endit);
          pivots[key] =
              child_info(merged_node, merged_node->pivots.size() +
                                          merged_node->elements.size());
          beginit = pivots.lower_bound(key);
        }
      }
    }

    // Receive a collection of new messages and perform recursive
    // flushes or splits as necessary.  If we split, return a
    // map with the new pivot keys pointing to the new nodes.
    // Otherwise return an empty map.
    //
    // 接收一组新消息，并根据需要执行递归式的刷新或拆分操作。
    // 若执行了节点拆分，则返回一个映射表，其中以新的主键作为键、对应指向新节点。
    // 若未执行拆分，则返回一个空的映射表。
    pivot_map flush(betree &bet, message_map &elts) {
      debug(std::cout << "Flushing " << this << std::endl);
      pivot_map result;

      // 如果请求是空的，就直接返回一个空的映射表，不做任何操作
      if (elts.size() == 0) {
        debug(std::cout << "Done (empty input)" << std::endl);
        return result;
      }

      if (is_leaf()) {
        // [数据写入]1. 遍历一组kv操作，将每个操作应用到当前节点上
        // （当且仅当当前节点是叶子节点时）
        for (auto it = elts.begin(); it != elts.end(); ++it) {
          apply(it->first, it->second, bet.default_value);
        }

        // [数据写入]2. 如果当前节点的消息数量超过了最大容量限制，就执行分裂操作
        // elements是一个 map，存储了当前节点的键值对
        //   key 是一个 MessageKey 对象，包含用户键和时间戳
        //   value 是一个 Message 对象，包含操作类型和操作值
        // pivots是一个 map，存储了当前节点的子节点信息
        //   key 是一个用户键，表示子节点的分界点
        //   value 是一个 child_info 对象，包含子节点指针和元数据
        if (elements.size() + pivots.size() >= bet.max_node_size) {
          result = split(bet);
          // split 返回的对象是一个 pivot_map
        }
        return result;
      }

      ////////////// Non-leaf
      // [数据写入]3.0 如果写入记录的目标节点不是叶子节点

      // Update the key of the first child, if necessary
      // [数据写入]3.1 如果当前插入的key比枢轴键中最小的还要小，
      // 就把这个key作为新的枢轴键
      Key oldmin = pivots.begin()->first;
      MessageKey<Key> newmin = elts.begin()->first;
      if (newmin < oldmin) {
        // 替换掉 pivots 中最小的那个键
        pivots[newmin.key] = pivots[oldmin];
        pivots.erase(oldmin);
      }

      // If everything is going to a single dirty child, go ahead
      // and put it there.
      // [数据写入]4. 检查当前要插入的一组key是否都要进入到同一个子节点
      auto first_pivot_idx = get_pivot(elts.begin()->first.key);
      auto last_pivot_idx = get_pivot((--elts.end())->first.key);

      // [数据写入]5. 如果要插入的一组key都要进入到同一个子节点，
      // 并且这个子节点是脏的，就直接把这一组key插入到这个子节点中去
      if (first_pivot_idx == last_pivot_idx &&
          first_pivot_idx->second.child.is_dirty()) {
        // There shouldn't be anything in our buffer for this child,
        // but lets assert that just to be safe.
        {
          auto next_pivot_idx = next(first_pivot_idx);
          auto elt_start = get_element_begin(first_pivot_idx);
          auto elt_end = get_element_begin(next_pivot_idx);
          assert(elt_start == elt_end);
        }

        // [数据写入]6. 把这一组key插入到这个子节点中去
        pivot_map new_children =
            first_pivot_idx->second.child->flush(bet, elts);

        if (!new_children.empty()) {
          pivots.erase(first_pivot_idx);
          pivots.insert(new_children.begin(), new_children.end());
        } else {
          first_pivot_idx->second.child_size =
              first_pivot_idx->second.child->pivots.size() +
              first_pivot_idx->second.child->elements.size();
        }
      } else {
        // [数据写入]7.
        // 如果要插入的一组key要进入到多个子节点，或者要进入的子节点是干净的，
        // 就把这一组key先插入到当前节点的缓冲区中去

        for (auto it = elts.begin(); it != elts.end(); ++it) {
          apply(it->first, it->second, bet.default_value);
        }

        // Now flush to out-of-core or clean children as necessary
        // 到这里数据已经全部写入了当前节点的内存。
        // 接下来根据需要把数据刷新到磁盘节点或内存干净节点中去。

        // [数据写入]8. 现在根据需要将数据刷新到磁盘节点或内存干净节点中去
        // 如果当前节点的消息数量超过了最大容量限制，就执行以下操作：
        while (elements.size() + pivots.size() >= bet.max_node_size) {
          // Find the child with the largest set of messages in our buffer
          // [数据写入]9.
          // 找到当前节点的哪个子节点在当前节点的缓冲区中有最多的消息要插入
          unsigned int max_size = 0;
          auto child_pivot = pivots.begin();
          auto next_pivot = pivots.begin();
          for (auto it = pivots.begin(); it != pivots.end(); ++it) {
            auto it2 = next(it);
            auto elt_it = get_element_begin(it);
            auto elt_it2 = get_element_begin(it2);
            unsigned int dist = distance(elt_it, elt_it2);
            if (dist > max_size) {
              child_pivot = it;
              next_pivot = it2;
              max_size = dist;
            }
          }

          // [数据写入]10.
          // 如果这个子节点在当前节点的缓冲区中有足够多的消息要插入，
          // 或者要插入的消息数量不太少（至少是最大容量限制的一半），并且这个子节点是内存中的干净节点，
          // 就把这一组消息插入到这个子节点中去
          // 否则就需要执行节点分裂操作了，因为当前节点的子节点太多了
          // 下面的条件判断逻辑是为了决定是否要把消息直接插入到子节点中去，还是要执行节点分裂操作
          // 如果break，会跳出while循环，继续执行下面的分裂操作
          if (!(max_size > bet.min_flush_size ||
                (max_size > bet.min_flush_size / 2 &&
                 child_pivot->second.child.is_in_memory()))) {
            break; // We need to split because we have too many pivots
          }

          // [数据写入]11.1. 提取当前节点的缓冲区中要插入到这个子节点中的消息
          auto elt_child_it = get_element_begin(child_pivot);
          auto elt_next_it = get_element_begin(next_pivot);
          message_map child_elts(elt_child_it, elt_next_it);
          // [数据写入]11.2. 把这一组消息插入到这个子节点中去，
          // 如果插入操作导致了子节点的分裂，
          // 分裂的结果会返回到new_children中去
          // 如果new_children不为空，说明子节点发生了分裂，
          // 需要把分裂出来的新节点插入到当前节点的枢轴键中去
          pivot_map new_children =
              child_pivot->second.child->flush(bet, child_elts);
          // [数据写入]11.3. 从当前节点的缓冲区中删除这一组消息
          elements.erase(elt_child_it, elt_next_it);

          if (!new_children.empty()) {
            pivots.erase(child_pivot);
            pivots.insert(new_children.begin(), new_children.end());
          } else {
            first_pivot_idx->second.child_size =
                child_pivot->second.child->pivots.size() +
                child_pivot->second.child->elements.size();
          }
        }

        // We have too many pivots to efficiently flush stuff down, so split
        if (elements.size() + pivots.size() > bet.max_node_size) {
          result = split(bet);
        }
      }

      // merge_small_children(bet);

      debug(std::cout << "Done flushing " << this << std::endl);
      return result;
    }

    Value query(const betree &bet, const Key k) const {
      debug(std::cout << "Querying " << this << std::endl);
      if (is_leaf()) {
        auto it = elements.lower_bound(MessageKey<Key>::range_start(k));
        if (it != elements.end() && it->first.key == k) {
          assert(it->second.opcode == INSERT);
          return it->second.val;
        } else {
          throw std::out_of_range("Key does not exist");
        }
      }

      ///////////// Non-leaf

      auto message_iter = get_element_begin(k);
      Value v = bet.default_value;

      if (message_iter == elements.end() || k < message_iter->first)
        // If we don't have any messages for this key, just search
        // further down the tree.
        v = get_pivot(k)->second.child->query(bet, k);
      else if (message_iter->second.opcode == UPDATE) {
        // We have some updates for this key.  Search down the tree.
        // If it has something, then apply our updates to that.  If it
        // doesn't have anything, then apply our updates to the
        // default initial value.
        try {
          Value t = get_pivot(k)->second.child->query(bet, k);
          v = t;
        } catch (std::out_of_range e) {
        }
      } else if (message_iter->second.opcode == DELETE) {
        // We have a delete message, so we don't need to look further
        // down the tree.  If we don't have any further update or
        // insert messages, then we should return does-not-exist (in
        // this subtree).
        message_iter++;
        if (message_iter == elements.end() || k < message_iter->first)
          throw std::out_of_range("Key does not exist");
      } else if (message_iter->second.opcode == INSERT) {
        // We have an insert message, so we don't need to look further
        // down the tree.  We'll apply any updates to this value.
        v = message_iter->second.val;
        message_iter++;
      }

      // Apply any updates to the value obtained above.
      while (message_iter != elements.end() && message_iter->first.key == k) {
        assert(message_iter->second.opcode == UPDATE);
        v = v + message_iter->second.val;
        message_iter++;
      }

      return v;
    }

    std::pair<MessageKey<Key>, Message<Value>>
    get_next_message_from_children(const MessageKey<Key> *mkey) const {
      if (mkey && *mkey < pivots.begin()->first)
        mkey = NULL;
      auto it = mkey ? get_pivot(mkey->key) : pivots.begin();
      while (it != pivots.end()) {
        try {
          return it->second.child->get_next_message(mkey);
        } catch (std::out_of_range e) {
        }
        ++it;
      }
      throw std::out_of_range("No more messages in any children");
    }

    std::pair<MessageKey<Key>, Message<Value>>
    get_next_message(const MessageKey<Key> *mkey) const {
      auto it = mkey ? elements.upper_bound(*mkey) : elements.begin();

      if (is_leaf()) {
        if (it == elements.end())
          throw std::out_of_range("No more messages in sub-tree");
        return std::make_pair(it->first, it->second);
      }

      if (it == elements.end())
        return get_next_message_from_children(mkey);

      try {
        auto kids = get_next_message_from_children(mkey);
        if (kids.first < it->first)
          return kids;
        else
          return std::make_pair(it->first, it->second);
      } catch (std::out_of_range e) {
        return std::make_pair(it->first, it->second);
      }
    }

    void _serialize(std::iostream &fs, serialization_context &context) {
      fs << "pivots:" << std::endl;
      serialize(fs, context, pivots);
      fs << "elements:" << std::endl;
      serialize(fs, context, elements);
    }

    void _deserialize(std::iostream &fs, serialization_context &context) {
      std::string dummy;
      fs >> dummy;
      deserialize(fs, context, pivots);
      fs >> dummy;
      deserialize(fs, context, elements);
    }
  };

  swap_space *ss;
  uint64_t min_flush_size;
  uint64_t max_node_size;
  uint64_t min_node_size;
  node_pointer root;
  uint64_t next_timestamp = 1; // Nothing has a timestamp of 0
  Value default_value;

public:
  betree(swap_space *sspace, uint64_t maxnodesize = DEFAULT_MAX_NODE_SIZE,
         uint64_t minnodesize = DEFAULT_MAX_NODE_SIZE / 4,
         uint64_t minflushsize = DEFAULT_MIN_FLUSH_SIZE)
      : ss(sspace), min_flush_size(minflushsize), max_node_size(maxnodesize),
        min_node_size(minnodesize) {

    // [静态数据结构]1. 分配一个 node 类型对象，node 内部包含一个节点的
    // 枢轴键到子节点指针的映射（pivot_map），
    // 和一个消息键到消息的映射（message_map）
    // 这里的消息键是用户键和时间戳的组合，消息是操作码和用户值的组合
    //
    // 在代码中搜索 [静态数据结构]，找到 3 层封装的具体实现
    root = ss->allocate(new node);
  }

  // Insert the specified message and handle a split of the root if it
  // occurs.
  // 插入指定的消息，若根节点发生分裂则处理该分裂操作。
  //
  // 插入、更新和删除操作都通过 upsert 函数实现，upsert
  // 函数会将这些操作转化为一个个的消息， 然后将这些消息插入到树中。
  void upsert(int opcode, Key k, Value v) {
    message_map tmp;

    // 拼接每个键 -> 时间戳+原始键
    // 拼接每个值 -> 操作码+原始值
    // 这里和 LSM tree 很像，都是把更新操作转化为一个个的消息，插入到树中
    tmp[MessageKey<Key>(k, next_timestamp++)] = Message<Value>(opcode, v);

    // pivot_map 是一个 std::map，建立键到 child_info 的映射
    // 如果主节点发生了分裂，flush 才会返回一个非空的
    // pivot_map，里面包含了新的主键和对应的新节点
    // 这样调用者可以创建新的主节点，
    // 新的主节点的 pivots 就是 flush 返回的 pivot_map
    pivot_map new_nodes = root->flush(*this, tmp);
    // 在上面一行代码中，root->flush不是简单地调用root中的flush函数
    // 而是重载了->运算符，
    // -> 运算符会生成一个临时对象，
    // 临时对象的具体功能:
    //   递增 root 节点的 pincount
    // 这个临时对象会递归调用 -> 运算符，
    // 临时对象 -> 也被重载，具体功能：
    //   返回 root 节点的真实底层对象指针
    //  （这里的 root 对象实际只包含一个指向 swap_space 的指针 和 一个 id）
    // 最后会调用 node 类中的 flush 函数，flush 函数会执行实际的刷新操作，
    // flush 完成后在上述临时对象的析构函数中会递减 root 节点的 pincount

    if (new_nodes.size() > 0) {
      root = ss->allocate(new node);
      root->pivots = new_nodes;
    }
  }

  void insert(Key k, Value v) { upsert(INSERT, k, v); }

  void update(Key k, Value v) { upsert(UPDATE, k, v); }

  void erase(Key k) { upsert(DELETE, k, default_value); }

  Value query(Key k) {
    Value v = root->query(*this, k);
    return v;
  }

  void dump_messages(void) {
    std::pair<MessageKey<Key>, Message<Value>> current;

    std::cout << "############### BEGIN DUMP ##############" << std::endl;

    try {
      current = root->get_next_message(NULL);
      do {
        std::cout << current.first.key << " " << current.first.timestamp << " "
                  << current.second.opcode << " " << current.second.val
                  << std::endl;
        current = root->get_next_message(&current.first);
      } while (1);
    } catch (std::out_of_range e) {
    }
  }

  class iterator {
  public:
    iterator(const betree &bet)
        : bet(bet), position(), is_valid(false), pos_is_valid(false), first(),
          second() {}

    iterator(const betree &bet, const MessageKey<Key> *mkey)
        : bet(bet), position(), is_valid(false), pos_is_valid(false), first(),
          second() {
      try {
        position = bet.root->get_next_message(mkey);
        pos_is_valid = true;
        setup_next_element();
      } catch (std::out_of_range e) {
      }
    }

    void apply(const MessageKey<Key> &msgkey, const Message<Value> &msg) {
      switch (msg.opcode) {
      case INSERT:
        first = msgkey.key;
        second = msg.val;
        is_valid = true;
        break;
      case UPDATE:
        first = msgkey.key;
        if (is_valid == false)
          second = bet.default_value;
        second = second + msg.val;
        is_valid = true;
        break;
      case DELETE:
        is_valid = false;
        break;
      default:
        abort();
        break;
      }
    }

    void setup_next_element(void) {
      is_valid = false;
      while (pos_is_valid && (!is_valid || position.first.key == first)) {
        apply(position.first, position.second);
        try {
          position = bet.root->get_next_message(&position.first);
        } catch (std::exception e) {
          pos_is_valid = false;
        }
      }
    }

    bool operator==(const iterator &other) {
      return &bet == &other.bet && is_valid == other.is_valid &&
             pos_is_valid == other.pos_is_valid &&
             (!pos_is_valid || position == other.position) &&
             (!is_valid || (first == other.first && second == other.second));
    }

    bool operator!=(const iterator &other) { return !operator==(other); }

    iterator &operator++(void) {
      setup_next_element();
      return *this;
    }

    const betree &bet;
    std::pair<MessageKey<Key>, Message<Value>> position;
    bool is_valid;
    bool pos_is_valid;
    Key first;
    Value second;
  };

  iterator begin(void) const { return iterator(*this, NULL); }

  iterator lower_bound(Key key) const {
    MessageKey<Key> tmp = MessageKey<Key>::range_start(key);
    return iterator(*this, &tmp);
  }

  iterator upper_bound(Key key) const {
    MessageKey<Key> tmp = MessageKey<Key>::range_end(key);
    return iterator(*this, &tmp);
  }

  iterator end(void) const { return iterator(*this); }
};
