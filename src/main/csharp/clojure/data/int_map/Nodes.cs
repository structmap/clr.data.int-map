//  Copyright (c) Zach Tellman, Rich Hickey and contributors. All rights reserved.
//  The use and distribution terms for this software are covered by the
//  Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
//  which can be found in the file epl-v10.html at the root of this distribution.
//  By using this software in any fashion, you are agreeing to be bound by
//  the terms of this license.
//  You must not remove this notice, or any other, from this software.

namespace clojure.data.int_map;

using clojure.lang;
using System.Collections;

public class Nodes {

  class InvertFn : AFn {
    public IFn f;

    public InvertFn(IFn f) {
      this.f = f;
    }

    public override Object invoke(Object x, Object y) {
      return f.invoke(y, x);
    }
  }

  public static IFn invert(IFn f) {
    if (f is InvertFn) {
      return ((InvertFn) f).f;
    }
    return new InvertFn(f);
  }

  // bitwise helper functions

  public static long lowestBit(long n) {
    return n & -n;
  }

  private static byte[] deBruijnIndex =   new byte[]{0, 1, 2, 53, 3, 7, 54, 27, 4, 38, 41, 8, 34, 55, 48, 28,
                                                  62, 5, 39, 46, 44, 42, 22, 9, 24, 35, 59, 56, 49, 18, 29, 11,
                                                  63, 52, 6, 26, 37, 40, 33, 47, 61, 45, 43, 21, 23, 58, 17, 10,
                                                  51, 25, 36, 32, 60, 20, 57, 16, 50, 31, 19, 15, 30, 14, 13, 12};

  public static int bitLog2(long n) {
    return deBruijnIndex[0xFF & (int) ((n * 0x022fdd63cc95386dL) >>> 58)];
  }

  public static int offset(long a, long b) {
    return bitLog2(highestBit(a ^ b, 1)) & ~0x3;
  }

  public static long highestBit(long n, long estimate) {
    long x = n & ~(estimate - 1);
    long m;
    while (true) {
      m = lowestBit(x);
      if (x == m) return m;
      x -= m;
    }
  }

  // 2-way top-level branch
  public class BinaryBranch : INode {

    public INode a, b;

    public BinaryBranch(INode a, INode b) {
      this.a = a;
      this.b = b;
    }

    public long count() {
      return a.count() + b.count();
    }

    public IEnumerator iterator(INode.IterationType type, bool reverse) {
      yield return reverse ? b.iterator(type, reverse) : a.iterator(type, reverse);
      yield return reverse ? a.iterator(type, reverse) : b.iterator(type, reverse);
    }

    public INode range(long min, long max) {
      if (max < 0) {
        return a.range(min, max);
      } else if (min >= 0) {
        return b.range(min, max);
      } else {
        INode aPrime = a.range(min,max);
        INode bPrime = b.range(min, max);

        if (aPrime == null && bPrime == null)
        {
          return Empty.EMPTY;
        } else if (aPrime == null) {
          return bPrime;
        } else if (bPrime == null) {
          return aPrime;
        } else {
          return new BinaryBranch(aPrime, bPrime);
        }
      }
    }

    public INode merge(INode node, long epoch, IFn f) {
      if (node is BinaryBranch) {
        BinaryBranch bin = (BinaryBranch) node;
        return new BinaryBranch(a.merge(bin.a, epoch, f), b.merge(bin.b, epoch, f));
      } else if (node is Branch) {
        Branch branch = (Branch) node;
        return branch.prefix < 0 ? new BinaryBranch(a.merge(node, epoch, f), b) : new BinaryBranch(a, b.merge(node, epoch, f));
      } else {
        return node.merge(this, epoch, invert(f));
      }
    }

    public INode assoc(long k, long epoch, IFn f, Object v) {
      if (k < 0) {
        INode aPrime = a.assoc(k, epoch, f, v);
        return a == aPrime ? this : new BinaryBranch(aPrime, b);
      } else {
        INode bPrime = b.assoc(k, epoch, f, v);
        return b == bPrime ? this : new BinaryBranch(a, bPrime);
      }
    }

    public INode dissoc(long k, long epoch) {
      if (k < 0) {
        INode aPrime = a.dissoc(k, epoch);
        return aPrime == null
                ? b
                : (a == aPrime)
                ? this
                : new BinaryBranch(aPrime, b);
      } else {
        INode bPrime = b.dissoc(k, epoch);
        return bPrime == null
                ? a
                : (b == bPrime)
                ? this
                : new BinaryBranch(a, bPrime);
      }
    }

    public INode update(long k, long epoch, IFn f) {
      if (k < 0) {
        INode aPrime = a.update(k, epoch, f);
        return a == aPrime ? this : new BinaryBranch(aPrime, b);
      } else {
        INode bPrime = b.update(k, epoch, f);
        return b == bPrime ? this : new BinaryBranch(a, bPrime);
      }
    }

    public Object get(long k, Object defaultVal) {
      return k < 0 ? a.get(k, defaultVal) : b.get(k, defaultVal);
    }

    public Object kvreduce(IFn f, Object init) {
      init = a.kvreduce(f, init);
      if (RT.isReduced(init)) return init;
      return b.kvreduce(f, init);
    }

    public Object reduce(IFn f, Object init) {
      init = a.reduce(f, init);
      if (RT.isReduced(init)) return init;
      return b.reduce(f, init);
    }

    // public Object fold(long n, final IFn combiner, final IFn reducer, final IFn fjtask, final IFn fjfork, final IFn fjjoin) {
    //   if (count() > n) {
    //     Object forked = new Callable() {
    //       public Object call() throws Exception {
    //         return b.fold(n, combiner, reducer, fjtask, fjfork, fjjoin);
    //       }
    //     };
    //     return combiner.invoke(a.fold(n, combiner, reducer, fjtask, fjfork, fjjoin), fjjoin.invoke(fjfork.invoke(fjtask.invoke(forked))));
    //   } else {
    //     return kvreduce(reducer, combiner.invoke());
    //   }
    // }
  }

  // 16-way branch node

  public class Branch : INode {
    public long prefix, mask, epoch;
    public int offsett;
    long countt;
    public INode[] children;

    public Branch(long prefix, int offset, long epoch, long count, INode[] children) {
      this.prefix = prefix;
      this.offsett = offset;
      this.epoch = epoch;
      this.mask = 0xfL << offset;
      this.countt = count;
      this.children = children;
    }

    public Branch(long prefix, int offset, long epoch, INode[] children) {
      this.prefix = prefix;
      this.offsett = offset;
      this.epoch = epoch;
      this.mask = 0xfL << offset;
      this.countt = -1;
      this.children = children;
    }

    public int indexOf(long key) {
      return (int) ((key & mask) >>> offsett);
    }

    private INode[] arraycopy() {
      INode[] copy = new INode[16];
      Array.Copy(children, 0, copy, 0, 16);
      return copy;
    }

    // returns 0 for no overlap, 1 if there's some overlap, 2 if the left completely covers the right
    private static int overlap(long min0, long max0, long min1, long max1) {
      if (min0 <= min1 && max1 <= max0) {
        return 2;
      }
      if (min0 <= max1 && min1 <= max0) {
        return 1;
      }
      return 0;
    }

    public INode range(long min, long max) {
      long nodeMask = offsett < 60 ? ((1L << (offsett + 4)) - 1) : ~(1L<<63);
      long nodeMin = prefix & ~nodeMask;
      long nodeMax = prefix | nodeMask;
      switch (overlap(min, max, nodeMin, nodeMax)) {
        case 2:
          return this;
        case 0:
          return null;
        default:
          break;
      }

      INode[] children = null;
      // true if already known that at least one child of this node should be removed in the result
      bool atLeastOneDropped = false;

      int minI = min <= nodeMin ? -1 : indexOf(Math.Min(nodeMax, min));
      int maxI = max >= nodeMax ? 16 : indexOf(Math.Max(nodeMin, max));

      INode onlyChild = null;
      int numChildren = 0;

      for (int i = 0; i < 16; i++) {
        INode c = this.children[i];
        if (c != null) {

          // if i is outside [minI, maxI], it must have no intersection with the range
          // if it is strictly inside (minI, maxI) exclusive, it must be fully in the range
          // otherwise it is minI or maxI, so it may be partially in the range so we have to recurse
          INode child =
                  (i < minI || maxI < i) ? null :
                          (minI < i && i < maxI) ? c :
                          c.range(min, max);

          if (children != null) {
            children[i] = child;
          } else if (child == null) {
            if (numChildren != 0) {
              // dropping `child`, but the children at previous indices are all identical to this.children, so clone
              // indices 0..(i-1)
              children = (INode[])this.children.Clone();
              children[i] = null;
            } else {
              atLeastOneDropped = true;
            }
          } else if (child != c) {
            children = numChildren != 0 ? (INode[])this.children.Clone() : new INode[16];
            children[i] = child;
            // we could set atLeastOneChild = true here, but it will never be used since now children != null
          } else if (atLeastOneDropped) {
            children = new INode[16];
            children[i] = child;
            // we could set atLeastOneChild = true here, but it will never be used since now children != null
          }
          if (child != null) {
            numChildren += 1;
            onlyChild = child;
          }
        }
      }

      return numChildren == 0 ? null :
              numChildren == 1 ? onlyChild :
              children == null ? this :
              new Branch(prefix, offsett, epoch, children);
    }
    public IEnumerator iterator(INode.IterationType type, bool reverse) {
      byte idx = (byte)(reverse ? 16 : -1);
      while (reverse ? --idx >= 0 : ++idx < 16) {
        INode c = children[idx];
        if (c != null) {
          IEnumerator iterator = children[idx].iterator(type, reverse);
          while (iterator.MoveNext()) {
            yield return iterator.Current;
          }
        }
      }
    }

    public Object get(long k, Object defaultVal) {
      INode n = children[indexOf(k)];
      return n == null ? defaultVal : n.get(k, defaultVal);
    }

    public long count() {
      long count = 0;
      for (int i = 0; i < 16; i++) {
        INode n = children[i];
        if (n != null) count += n.count();
      }
      this.countt = count;
      return count;
    }

    public INode merge(INode node, long epoch, IFn f) {
      if (node is Branch) {
        Branch branch = (Branch) node;
        int offsetPrime = Nodes.offset(prefix, branch.prefix);

        if (branch.prefix < 0 && this.prefix >= 0) {
          return new BinaryBranch(branch, this);
        } else if (branch.prefix >= 0 && this.prefix < 0) {
          return new BinaryBranch(this, branch);
        }

        if (offsetPrime > offsett && offsetPrime > branch.offsett) {
            return new Branch(prefix, Nodes.offset(prefix, branch.prefix), epoch, new INode[16])
                .merge(this, epoch, f)
                .merge(node, epoch, f);
        }

        // we contain the other node
        if (offsett > branch.offsett) {
          int idx = indexOf(branch.prefix);
          INode[] childrenn = arraycopy();
          INode n = childrenn[idx];
          childrenn[idx] = n != null ? n.merge(node, epoch, f) : node;
          return new Branch(prefix, offsett, epoch, childrenn);

        }

        if (offsett < branch.offsett) {
          return branch.merge(this, epoch, invert(f));
        }

        INode[] children = new INode[16];
        INode[] branchChildren = branch.children;
        int offset = this.offsett;

        for (int i = 0; i < 16; i++) {
          INode n = this.children[i];
          INode nPrime = branchChildren[i];
          if (n == null) {
            children[i] = nPrime;
          } else if (nPrime == null) {
            children[i] = n;
          } else {
            children[i] = n.merge(nPrime, epoch, f);
          }
        }
        return new Branch(prefix, offset, epoch, children);

      } else {
        return node.merge(this, epoch, invert(f));
      }
    }

    public INode assoc(long k, long epoch, IFn f, Object v) {
      int offsetPrime = offset(k, prefix);

      // need a new branch above us both
      if (prefix < 0 && k >= 0) {
        return new BinaryBranch(this, new Leaf(k, v));
      } else if (k < 0 && prefix >= 0) {
        return new BinaryBranch(new Leaf(k, v), this);
      } else if (offsetPrime > this.offsett) {
        return new Branch(k, offsetPrime, epoch, new INode[16])
                .merge(this, epoch, null)
                .assoc(k, epoch, f, v);

        // somewhere at or below our level
      } else {
        int idx = indexOf(k);
        INode n = children[idx];
        if (n == null) {
          if (epoch == this.epoch) {
            children[idx] = new Leaf(k, v);
            countt = -1;
            return this;
          } else {
            INode[] children = arraycopy();
            children[idx] = new Leaf(k, v);
            return new Branch(prefix, offsett, epoch, countt, children);
          }
        } else {
          INode nPrime = n.assoc(k, epoch, f, v);
          if (nPrime == n) {
            countt = -1;
            return this;
          } else {
            INode[] children = arraycopy();
            children[idx] = nPrime;
            return new Branch(prefix, offsett, epoch, countt, children);
          }
        }
      }
    }

    public INode dissoc(long k, long epoch) {
      int idx = indexOf(k);
      INode n = children[idx];
      if (n == null) {
        return this;
      } else {
        INode nPrime = n.dissoc(k, epoch);
        if (nPrime == n) {
          countt = -1;
          return this;
        } else {
          INode[] children = arraycopy();
          children[idx] = nPrime;
          for (int i = 0; i < 16; i++) {
            if (children[i] != null) {
              return new Branch(prefix, offsett, epoch, countt, children);
            }
          }
          return null;
        }
      }
    }

    public INode update(long k, long epoch, IFn f) {
      int offsetPrime = offset(k, prefix);

      // need a new branch above us both
      if (prefix < 0 && k >= 0) {
        return new BinaryBranch(this, new Leaf(k, f.invoke(null)));
      } else if (k < 0 && prefix >= 0) {
        return new BinaryBranch(new Leaf(k, f.invoke(null)), this);
      } else if (offsetPrime > this.offsett) {
        return new Branch(k, offsetPrime, epoch, new INode[16])
                .merge(this, epoch, null)
                .update(k, epoch, f);
      }

      int idx = indexOf(k);
      INode n = children[idx];
      if (n == null) {
        if (epoch == this.epoch) {
          children[idx] = new Leaf(k, f.invoke(null));
          countt = -1;
          return this;
        } else {
          INode[] children = arraycopy();
          children[idx] = new Leaf(k, f.invoke(null));
          return new Branch(prefix, offsett, epoch, countt, children);
        }
      } else {
        INode nPrime = n.update(k, epoch, f);
        if (nPrime == n) {
          countt = -1;
          return this;
        } else {
          INode[] children = arraycopy();
          children[idx] = nPrime;
          return new Branch(prefix, offsett, epoch, countt, children);
        }
      }
    }

    public Object kvreduce(IFn f, Object init) {
      for (int i = 0; i < 16; i++) {
        INode n = children[i];
        if (n != null) init = n.kvreduce(f, init);
        if (RT.isReduced(init)) break;
      }
      return init;
    }

    public Object reduce(IFn f, Object init) {
      for (int i = 0; i < 16; i++) {
        INode n = children[i];
        if (n != null) init = n.reduce(f, init);
        if (RT.isReduced(init)) break;
      }
      return init;
    }

    // // adapted from the PersistentHashMap.ArrayNode implementation
    // static public Object foldTasks(List<Callable> tasks, final IFn combiner, final IFn fjtask, final IFn fjfork, final IFn fjjoin) {
    //
    //   if (tasks.isEmpty()) {
    //     return combiner.invoke();
    //
    //     // just wait on the one value
    //   } else if (tasks.size() == 1) {
    //     try {
    //       return tasks.get(0).call();
    //     } catch (Exception e) {
    //       throw Util.sneakyThrow(e);
    //     }
    //
    //     // divide and conquer
    //   } else {
    //     List<Callable> t1 = tasks.subList(0, tasks.size() / 2);
    //     final List<Callable> t2 = tasks.subList(tasks.size() / 2, tasks.size());
    //
    //     Object forked = fjfork.invoke(fjtask.invoke(new Callable() {
    //       public Object call() throws Exception {
    //         return foldTasks(t2, combiner, fjtask, fjfork, fjjoin);
    //       }
    //     }));
    //
    //     return combiner.invoke(foldTasks(t1, combiner, fjtask, fjfork, fjjoin), fjjoin.invoke(forked));
    //   }
    // }

    // public Object fold(long n, final IFn combiner, final IFn reducer, final IFn fjtask, final IFn fjfork, final IFn fjjoin) {
    //   if (n > count()) {
    //     List<Callable> tasks = new ArrayList();
    //     for (int i = 0; i < 16; i++) {
    //       final INode node = children[i];
    //       if (node != null) {
    //         tasks.add(new Callable() {
    //           public Object call() throws Exception {
    //             return node.fold(n, combiner, reducer, fjtask, fjfork, fjjoin);
    //           }
    //         });
    //       }
    //     }
    //     return foldTasks(tasks, combiner, fjtask, fjfork, fjjoin);
    //   } else {
    //     return kvreduce(reducer, combiner.invoke());
    //   }
    // }
  }

  // leaf node
  public class Leaf : INode {
    public long key;
    public Object value;

    public Leaf(long key, Object value) {
      this.key = key;
      this.value = value;
    }

    public IEnumerator iterator(INode.IterationType type, bool reverse) {
      switch(type) {
        case INode.IterationType.KEYS:
          yield return key;
          break;
        case INode.IterationType.VALS:
          yield return value;
          break;
        case INode.IterationType.ENTRIES:
          yield return new MapEntry(key, value);
          break;
        default:
          throw new InvalidOperationException();
      }
    }

    public INode range(long min, long max) {
      return (min <= key && key <= max) ? this : null;
    }

    public Object reduce(IFn f, Object init) {
      return f.invoke(init, new clojure.lang.MapEntry(key, value));
    }

    public Object kvreduce(IFn f, Object init) {
      return f.invoke(init, key, value);
    }

    public Object fold(long n, IFn combiner, IFn reducer, IFn fjtask, IFn fjfork, IFn fjjoin) {
      return kvreduce(reducer, combiner.invoke());
    }

    public long count() {
      return 1;
    }

    public INode merge(INode node, long epoch, IFn f) {
      return node.assoc(key, epoch, invert(f), value);
    }

    public INode assoc(long k, long epoch, IFn f, Object v) {
      if (k == key) {
        v = f == null ? v : f.invoke(value, v);
        return new Leaf(k, v);
      } else if (key < 0 && k >= 0) {
        return new BinaryBranch(this, new Leaf(k, v));
      } else if (k < 0 && key >= 0) {
        return new BinaryBranch(new Leaf(k, v), this);
      } else {
        return new Branch(k, offset(k, key), epoch, new INode[16])
                .assoc(key, epoch, f, value)
                .assoc(k, epoch, f, v);
      }
    }

    public INode dissoc(long k, long epoch) {
      if (key == k) {
        return null;
      } else {
        return this;
      }
    }

    public INode update(long k, long epoch, IFn f) {
      if (k == key) {
        Object v = f.invoke(value);
        return new Leaf(k, v);
      } else {
        return this.assoc(k, epoch, null, f.invoke(null));
      }
    }

    public Object get(long k, Object defaultVal) {
      if (k == key) return value;
      return defaultVal;
    }
  }

  // empty node
  public class Empty : INode {

    public static Empty EMPTY = new Empty();

    Empty() {
    }

    public INode range(long min, long max) {
      return this;
    }

    public IEnumerator iterator(INode.IterationType type, bool reverse)
    {
      yield break;
    }

    public Object reduce(IFn f, Object init) {
      return init;
    }

    public Object kvreduce(IFn f, Object init) {
      return init;
    }

    // public Object fold(long n, IFn combiner, IFn reducer, IFn fjtask, IFn fjfork, IFn fjjoin) {
    //   return combiner.invoke();
    // }

    public long count() {
      return 0;
    }

    public INode merge(INode node, long epoch, IFn f) {
      return node;
    }

    public INode assoc(long k, long epoch, IFn f, Object v) {
      return new Leaf(k, v);
    }

    public INode dissoc(long k, long epoch) {
      return this;
    }

    public INode update(long k, long epoch, IFn f) {
      return new Leaf(k, f.invoke(null));
    }

    public Object get(long k, Object defaultVal) {
      return defaultVal;
    }
  }
}
