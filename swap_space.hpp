// A scheme for transparently swapping data structures in and out of
// memory.

// WARNING: this is very incomplete.  It's just enough functionality
//          for the betree.cpp.  In particular, the current system
//          does not handle cycles in the pointer graph or pointers
//          into the middle of objects (such as into an array).

// The goal of this code is to enable users to write complex in-memory
// data structures and have a separate layer (i.e. this code) manage
// I/O.  Users should be able to define their data structures as they
// see fit (i.e. they can use pointers, etc) but still control the
// granularity at which items are swapped to/from memory.

// Therefore, we define a swap_space::pointer type that represents a
// pointer from one swappable unit to another.  When the swapper elects
// to swap out an object X, it will swap out all the objects that X
// points to through regular C++ pointers.  All these objects will be
// written to a single place on the backing store, so this will be
// I/O-efficient.  The swapper does not traverse swap_space::pointers
// -- they point to separate things that should be swapped out
// independently of the thing pointing to them.

// The betree code provides an example of how this is used.  We want
// each node to be swapped in/out as a single unit, but separate nodes
// in the tree should be able to be swapped in/out independently of
// eachother.  Therefore, nodes use swap_space::pointers to point to
// eachother.  They use regular C++ pointers to point to internal
// items that should be serialized as part of the node.

// The swap_space needs to manage all pointers to swappable objects.
// New swappable objects should be created like this:
//      swap_space ss;
//      swap_space::pointer<T> p = ss.allocate(new T(constructor args));

// You can then use operator-> as normal, e.g.
//      p->some_field
//      p->some_method(args)
// Although no operator* is not defined, it should be straightforward
// to do so.

// Invoking p->some_method() pins the object referred to by p in
// memory.  Thus, during the execution of some_method(), it is safe to
// dereference "this" and any other plain C++ pointers in the object.

// Objects are automatically garbage collected.  The garbage collector
// uses reference counting.

// The current system uses LRU to select items to swap.  The swap
// space has a user-specified in-memory cache size it.  The cache size
// can be adjusted dynamically.

// Don't try to get your hands on an unwrapped pointer to the object
// or anything that is swapped in/out as part of the object.  It can
// only lead to trouble.  Casting is also probably a bad idea.  Just
// write nice, clean, type-safe, well-encapsulated code and everything
// should work just fine.

// Objects managed by this system must be sub-types of class
// serializable.  This basically defines two methods for serializing
// and deserializing the object.  See the betree for examples of
// implementing these methods.  We provide default implementations for
// a few basic types and STL containers.  Feel free to add more and
// submit patches as you need them.

// The current implementation serializes to a textual file format.
// This is just a convenience.  It would be nice to be able to swap in
// different formats.

#ifndef SWAP_SPACE_HPP
#define SWAP_SPACE_HPP

#include "backing_store.hpp"
#include "debug.hpp"
#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

class swap_space;

class serialization_context {
public:
  serialization_context(swap_space &sspace) : ss(sspace), is_leaf(true) {}
  swap_space &ss;
  bool is_leaf;
};

class serializable {
public:
  virtual void _serialize(std::iostream &fs,
                          serialization_context &context) = 0;
  virtual void _deserialize(std::iostream &fs,
                            serialization_context &context) = 0;
  virtual ~serializable(void) {};
};

void serialize(std::iostream &fs, serialization_context &context, uint64_t x);
void deserialize(std::iostream &fs, serialization_context &context,
                 uint64_t &x);

void serialize(std::iostream &fs, serialization_context &context, int64_t x);
void deserialize(std::iostream &fs, serialization_context &context, int64_t &x);

void serialize(std::iostream &fs, serialization_context &context,
               std::string x);
void deserialize(std::iostream &fs, serialization_context &context,
                 std::string &x);

template <class Key, class Value>
void serialize(std::iostream &fs, serialization_context &context,
               std::map<Key, Value> &mp) {
  fs << "map " << mp.size() << " {" << std::endl;
  assert(fs.good());
  for (auto it = mp.begin(); it != mp.end(); ++it) {
    fs << "  ";
    serialize(fs, context, it->first);
    fs << " -> ";
    serialize(fs, context, it->second);
    fs << std::endl;
  }
  fs << "}" << std::endl;
}

template <class Key, class Value>
void deserialize(std::iostream &fs, serialization_context &context,
                 std::map<Key, Value> &mp) {
  std::string dummy;
  int size = 0;
  fs >> dummy >> size >> dummy;
  assert(fs.good());
  for (int i = 0; i < size; i++) {
    Key k;
    Value v;
    deserialize(fs, context, k);
    fs >> dummy;
    deserialize(fs, context, v);
    mp[k] = v;
  }
  fs >> dummy;
}

template <class X>
void serialize(std::iostream &fs, serialization_context &context, X *&x) {
  fs << "pointer ";
  serialize(fs, context, *x);
}

template <class X>
void deserialize(std::iostream &fs, serialization_context &context, X *&x) {
  std::string dummy;
  x = new X;
  fs >> dummy;
  assert(dummy == "pointer");
  deserialize(fs, context, *x);
}

template <class X>
void serialize(std::iostream &fs, serialization_context &context, X &x) {
  x._serialize(fs, context);
}

template <class X>
void deserialize(std::iostream &fs, serialization_context &context, X &x) {
  x._deserialize(fs, context);
}

class swap_space {
public:
  swap_space(backing_store *bs, uint64_t n);

  template <class Referent> class pointer;

  /**
   * @brief
   *
   * @tparam Referent
   * @param tgt 指向在栈上新分配但是没有初始化的 Referent 对象的指针
   * @return pointer<Referent> 返回的是一个在堆上分配的指针对象，指向 swap_space
   * 中管理的 Referent 对象
   */
  template <class Referent> pointer<Referent> allocate(Referent *tgt) {
    // [静态数据结构]3. object 对象被进一步封装成 pointer 对象
    // pointer 对象包含：
    //  一个指向 swap_space 的指针，
    //  一个 uint64_t 类型的 target，
    //     表示这个 pointer 指向的对象在 swap_space 中的 ID
    // pointer 对象在创建时，会先创建一个 object 对象来管理 tgt 指向的数据
    // 然后修改 swap_space 中的对象映射表，添加这个 object 对象，
    // 可以通过查找 swap_space 中的对象映射表,
    // 根据 object 对象的唯一 ID 来访问这个 object 对象，
    // 同时，pointer 对象在创建时还会将这个 object 对象
    //   插入到 swap_space 的 LRU 队列中，
    // LRU 队列维护着最近最久未访问的对象，以便在需要时进行交换（eviction）。
    // 另外，pointer 对象在创建时更新 swap_space 中当前内存对象的数量，
    // 并可能触发交换（eviction）操作，以确保内存使用在设定的限制范围内。
    return pointer<Referent>(this, tgt);
  }

  // This pins an object in memory for the duration of a member
  // access.  It's sort of an instance of the "resource aquisition is
  // initialization" paradigm.
  //
  // 在成员访问期间，将对象固定（锁定）在内存中。
  // 这属于“资源获取即初始化”编程范式的一种应用。
  template <class Referent> class pin {
  public:
    const Referent *operator->(void) const {
      assert(ss->objects.count(target) > 0);
      debug(std::cout << "Accessing (constly) " << target << " ("
                      << ss->objects[target]->target << ")" << std::endl);

      // 在通过封装的指针对象访问目标时
      // 会透明地调用 swap_space 的 access 方法，更新对象的访问时间戳，
      // 从而实现 LRU 交换策略。
      // 并且如果数据不在内存中，access 方法会负责将数据从磁盘加载到内存中。

      access(target, false);
      return (const Referent *)ss->objects[target]->target;
    }

    Referent *operator->(void) {
      assert(ss->objects.count(target) > 0);
      debug(std::cout << "Accessing " << target << " ("
                      << ss->objects[target]->target << ")" << std::endl);
      access(target, true);
      return (Referent *)ss->objects[target]->target;
    }

    pin(const pointer<Referent> *p) : ss(NULL), target(0) {
      dopin(p->ss, p->target);
    }

    pin(void) : ss(NULL), target(0) {}

    ~pin(void) { unpin(); }

    pin &operator=(const pin &other) {
      if (&other != this) {
        unpin();
        dopin(other.ss, other.target);
      }
    }

  private:
    void unpin(void) {
      debug(std::cout << "Unpinning " << target << " ("
                      << ss->objects[target]->target << ")" << std::endl);
      if (target > 0) {
        assert(ss->objects.count(target) > 0);
        ss->objects[target]->pincount--;
        ss->maybe_evict_something();
      }
      ss = NULL;
      target = 0;
    }

    void dopin(swap_space *newss, uint64_t newtarget) {
      assert(ss == NULL && target == 0);
      ss = newss;
      target = newtarget;
      if (target > 0) {
        assert(ss->objects.count(target) > 0);
        debug(std::cout << "Pinning " << target << " ("
                        << ss->objects[target]->target << ")" << std::endl);
        ss->objects[target]->pincount++;
      }
    }

    void access(uint64_t tgt, bool dirty) const {
      // [swap] 当通过 pointer 对象访问目标对象时，access 方法会被调用，
      // 这个方法会更新对象的访问时间戳，以实现 LRU 交换策略。
      // 如果对象不在内存中，access 方法会负责将对象从磁盘加载到内存中。
      assert(ss->objects.count(tgt) > 0);
      object *obj = ss->objects[tgt];
      ss->lru_pqueue.erase(obj);
      obj->last_access = ss->next_access_time++;
      ss->lru_pqueue.insert(obj);
      obj->target_is_dirty |= dirty;
      ss->load<Referent>(tgt);
      ss->maybe_evict_something();
    }

    swap_space *ss;
    uint64_t target;
  };

  template <class Referent> class pointer : public serializable {
    friend class swap_space;
    friend class pin<Referent>;

  public:
    pointer(void) : ss(NULL), target(0) {}

    pointer(const pointer &other) {
      ss = other.ss;
      target = other.target;
      if (target > 0) {
        assert(ss->objects.count(target) > 0);
        ss->objects[target]->refcount++;
      }
    }

    ~pointer(void) { depoint(); }

    void depoint(void) {
      if (target == 0) {
        return;
      }
      assert(ss->objects.count(target) > 0);

      object *obj = ss->objects[target];
      assert(obj->refcount > 0);
      if ((--obj->refcount) == 0) {
        debug(std::cout << "Erasing " << target << std::endl);
        // Load it into memory so we can recursively free stuff
        if (obj->target == NULL) {
          assert(obj->bsid > 0);
          if (!obj->is_leaf) {
            ss->load<Referent>(target);
          } else {
            debug(std::cout << "Skipping load of leaf " << target << std::endl);
          }
        }
        ss->objects.erase(target);
        ss->lru_pqueue.erase(obj);
        if (obj->target) {
          delete obj->target;
        }
        ss->current_in_memory_objects--;
        if (obj->bsid > 0) {
          ss->backstore->deallocate(obj->bsid);
        }
        delete obj;
      }
      target = 0;
    }

    pointer &operator=(const pointer &other) {
      if (&other != this) {
        depoint();
        ss = other.ss;
        target = other.target;
        if (target > 0) {
          assert(ss->objects.count(target) > 0);
          ss->objects[target]->refcount++;
        }
      }
      return *this;
    }

    bool operator==(const pointer &other) const {
      return ss == other.ss && target == other.target;
    }

    bool operator!=(const pointer &other) const { return !operator==(other); }

    // const Referent * operator->(void) const {
    //   ss->access(target, false);
    //   return ss->objects[target].target;
    // }

    const pin<Referent> operator->(void) const { return pin<Referent>(this); }

    pin<Referent> operator->(void) { return pin<Referent>(this); }

    pin<Referent> get_pin(void) { return pin<Referent>(this); }

    const pin<Referent> get_pin(void) const { return pin<Referent>(this); }

    bool is_in_memory(void) const {
      assert(ss->objects.count(target) > 0);
      return target > 0 && ss->objects[target]->target != NULL;
    }

    bool is_dirty(void) const {
      assert(ss->objects.count(target) > 0);
      return target > 0 && ss->objects[target]->target &&
             ss->objects[target]->target_is_dirty;
    }

    void _serialize(std::iostream &fs, serialization_context &context) {
      assert(target > 0);
      assert(context.ss.objects.count(target) > 0);
      fs << target << " ";
      target = 0;
      assert(fs.good());
      context.is_leaf = false;
    }

    void _deserialize(std::iostream &fs, serialization_context &context) {
      assert(target == 0);
      ss = &context.ss;
      fs >> target;
      assert(fs.good());
      assert(context.ss.objects.count(target) > 0);
      // We just created a new reference to this object and
      // invalidated the on-disk reference, so the total refcount
      // stays the same.
    }

  private:
    swap_space *ss;
    uint64_t target;

    // Only callable through swap_space::allocate(...)
    pointer(swap_space *sspace, Referent *tgt) {
      ss = sspace;
      target = sspace->next_id++;

      // [静态数据结构]2. tgt 就是前面分配的 node 对象的指针
      // 这里的 node 对象被封装一个 object 对象
      // object 对象里面包含：
      // 指向 node 对象(tgt) 的指针
      // 一个单调递增的 id,
      //   这个 id 后续会被用来作为键，
      //   查找 swap_space 中的对象映射表，访问这个 object 对象
      //
      //   额外的一层映射的作用是：Be tree相关逻辑可以直接把
      //   所有操作都当成是在内存中进行的
      //   但是 swap_space 可以在后台把这些对象交换到磁盘上，
      //   以节省内存空间。
      //   通过 swap_space对象间接访问这些对象时，
      //   swap_space 可以自动地把它们交换到内存中来，
      // 一个 bsid @todo
      // 一个 is_leaf 标志，表示这个对象是否是叶子节点
      // 一个 refcount，表示有多少指针指向这个对象，初始值为 1
      // 一个 last_access，表示上次访问这个对象的时间戳
      //   last_access 的初始值为 sspace->next_access_time++，
      //   即一个单调递增的时间戳
      // 一个 target_is_dirty 标志，表示这个对象是否被修改过但还没有写回磁盘
      //   初始值为 true，因为刚创建的对象还没有被写回过磁盘
      // 一个 pincount，pincount 不为0 的对象永远不会被交换出去，
      //   因为它们正在被访问着
      object *o = new object(sspace, tgt);
      assert(o != NULL);
      target = o->id;
      assert(ss->objects.count(target) == 0);
      ss->objects[target] = o;
      ss->lru_pqueue.insert(o);
      ss->current_in_memory_objects++;
      ss->maybe_evict_something();
    }
  };

private:
  backing_store *backstore;

  uint64_t next_id = 1;
  uint64_t next_access_time = 0;

  class object {
  public:
    object(swap_space *sspace, serializable *tgt);

    serializable *target;
    uint64_t id;
    uint64_t bsid;
    bool is_leaf;
    uint64_t refcount;
    uint64_t last_access;
    bool target_is_dirty;
    uint64_t pincount;
  };

  static bool cmp_by_last_access(object *a, object *b);

  template <class Referent> void load(uint64_t tgt) {
    assert(objects.count(tgt) > 0);
    if (objects[tgt]->target == NULL) {
      object *obj = objects[tgt];
      debug(std::cout << "Loading " << obj->id << std::endl);
      std::iostream *in = backstore->get(obj->bsid);
      Referent *r = new Referent();
      serialization_context ctxt(*this);
      deserialize(*in, ctxt, *r);
      backstore->put(in);
      obj->target = r;
      current_in_memory_objects++;
    }
  }

  void set_cache_size(uint64_t sz);

  void write_back(object *obj);
  void maybe_evict_something(void);

  uint64_t max_in_memory_objects;
  uint64_t current_in_memory_objects = 0;

  /*
    objects 是一个 unordered_map
    键是 uint64_t 类型，表示对象的 ID。
    值是 object* 类型，指向一个 object 结构体，包含了对象的具体信息。
  */
  std::unordered_map<uint64_t, object *> objects;

  /*
    lru_pqueue 是一个 std::set
    存储的对象类型是 swap_space::object*，
    并且把 last_access 较小的对象排在前面
    可以实现一个类似 LRU 的功能。
  */
  std::set<object *, bool (*)(object *, object *)> lru_pqueue;
};

#endif // SWAP_SPACE_HPP
