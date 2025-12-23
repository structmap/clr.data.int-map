//  Copyright (c) Zach Tellman, Rich Hickey and contributors. All rights reserved.
//  The use and distribution terms for this software are covered by the
//  Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
//  which can be found in the file epl-v10.html at the root of this distribution.
//  By using this software in any fashion, you are agreeing to be bound by
//  the terms of this license.
//  You must not remove this notice, or any other, from this software.

namespace clojure.data.int_map;

using clojure.lang;

public class IntSet : ISet {

  public class BitSetContainer : ISet {
    public long epoch;
    public BitSet bitSet;

    public BitSetContainer(long epoch, BitSet bitSet) {
      this.epoch = epoch;
      this.bitSet = bitSet;
    }

    public ISet add(long epoch, long val) {
      if (epoch == this.epoch) {
        bitSet.Set((short) val);
        return this;
      } else {
        BitSet bitSet = (BitSet) this.bitSet.Clone();
        bitSet.Set((short) val);
        return new BitSetContainer(epoch, bitSet);
      }
    }

    public ISet remove(long epoch, long val) {
      if (epoch == this.epoch) {
        bitSet.Set((short) val, false);
        return this;
      } else {
        BitSet bitSet = (BitSet) this.bitSet.Clone();
        bitSet.Set((short) val, false);
        return new BitSetContainer(epoch, bitSet);
      }
    }

    public bool contains(long val) {
      return bitSet.Get((short) val);
    }

    public ISet range(long epoch, long min, long max) {
      BitSet bitSet = (BitSet) this.bitSet.Clone();

      int size = bitSet.Size;
      bitSet.Set(0, (int)Math.Max(min, 0), false);
      if (max < size) {
        bitSet.Set(Math.Min((short)max+1, size), size, false);
      }
      return new BitSetContainer(epoch, bitSet);
    }

    // public Iterator elements(long offset, bool reverse) {
    //   List<long> ns = new ArrayList<long>(bitSet.Cardinality());
    //   int idx = 0;
    //   while (idx < bitSet.Length()) {
    //     idx = bitSet.NextSetBit(idx);
    //     ns.Add(offset + idx);
    //     idx++;
    //   }
    //   if (reverse) {
    //     Collections.reverse(ns);
    //   }
    //   return ns.iterator();
    // }

    public long count() {
      return bitSet.Cardinality();
    }

    public BitSet toBitSet() {
      return bitSet;
    }

    public ISet intersection(long epoch, ISet val) {
      BitSet bitSet = (BitSet) this.bitSet.Clone();
      bitSet.And(val.toBitSet());
      return new BitSetContainer(epoch, bitSet);
    }

    public ISet union(long epoch, ISet val) {
      BitSet bitSet = (BitSet) this.bitSet.Clone();
      bitSet.Or(val.toBitSet());
      return new BitSetContainer(epoch, bitSet);
    }

    public ISet difference(long epoch, ISet val) {
      BitSet bitSet = (BitSet) this.bitSet.Clone();
      bitSet.AndNot(val.toBitSet());
      return new BitSetContainer(epoch, bitSet);
    }
  }

  public class SingleContainer : ISet {
    public short val;

    public SingleContainer(short val) {
      this.val = val;
    }

    public ISet add(long epoch, long val) {
      if (val == this.val) {
        return this;
      } else {
        BitSet bitSet = new BitSet(Math.Max((short) val, this.val));
        bitSet.Set((short) val);
        bitSet.Set(this.val);
        return new BitSetContainer(epoch, bitSet);
      }
    }

    public ISet remove(long epoch, long val) {
      return val == this.val ? null : this;
    }

    public bool contains(long val) {
      return val == this.val;
    }

    public ISet range(long epoch, long min, long max) {
      return (min <= val && max >= val) ? this : null;
    }

    public long count() {
      return 1;
    }

    // public Iterator elements(long offset, bool reverse) {
    //   long val = this.val + offset;
    //   return new Iterator() {
    //
    //     private bool isDone = false;
    //
    //     public bool hasNext() {
    //       return !isDone;
    //     }
    //
    //     public Object next() {
    //       if (isDone) throw new NoSuchElementException();
    //       isDone = true;
    //       return val;
    //     }
    //
    //     public void remove() {
    //       throw new UnsupportedOperationException();
    //     }
    //   };
    // }

    public BitSet toBitSet() {
      BitSet bitSet = new BitSet(val);
      bitSet.Set(val);
      return bitSet;
    }

    public ISet intersection(long epoch, ISet sv) {
      return sv == null
          ? null
          : sv.contains(val)
          ? this
          : null;
    }

    public ISet union(long epoch, ISet sv) {
      return sv == null
          ? this
          : sv.contains(val)
          ? sv
          : sv.add(epoch, val);
    }

    public ISet difference(long epoch, ISet sv) {
      return sv == null
          ? this
          : sv.contains(val)
          ? null
          : this;
    }

  }

  public INode map;
  public short leafSize, log2LeafSize;
  public volatile int countt = -1;

  public IntSet(short leafSize) {
    this.leafSize = leafSize;
    this.log2LeafSize = (short) Nodes.bitLog2(leafSize);
    map = Nodes.Empty.EMPTY;
  }

  IntSet(short leafSize, short log2LeafSize, INode map) {
    this.leafSize = leafSize;
    this.log2LeafSize = log2LeafSize;
    this.map = map;
  }

  // public int leafSize() {
  //   return this.leafSize;
  // }

  private long mapKey(long val) {
    return val >> log2LeafSize;
  }

  private short leafOffset(long val) {
    return (short) (val & (leafSize - 1));
  }

  public ISet add(long epoch, long val) {
    INode mapPrime = map.update(mapKey(val), epoch,
            new AFn() {
              public Object invoke(Object v) {
                ISet s = (ISet) v;
                return s == null ? new SingleContainer(leafOffset(val)) : s.add(epoch, leafOffset(val));
              }
            });
    if (mapPrime == map) {
      countt = -1;
      return this;
    } else {
      return new IntSet(leafSize, log2LeafSize, mapPrime);
    }
  }

  public ISet remove(long epoch, long val) {
    INode mapPrime = map.update(mapKey(val), epoch,
            new AFn() {
              public Object invoke(Object v) {
                ISet s = (ISet) v;
                return s == null ? null : s.remove(epoch, leafOffset(val));
              }
            });
    if (mapPrime == map) {
      countt = -1;
      return this;
    } else {
      return new IntSet(leafSize, log2LeafSize, mapPrime);
    }
  }

  public bool contains(long val) {
    ISet s = (ISet) map.get(mapKey(val), null);
    return s != null && s.contains(leafOffset(val));
  }

  public ISet range(long epoch, long min, long max) {

    if (max < min) {
      return new IntSet(leafSize);
    }

    if (mapKey(min) == mapKey(max)) {
      ISet set = (ISet) map.get(mapKey(min), null);
      set = set == null ? null : set.range(epoch, leafOffset(min), leafOffset(max));

      return set == null
              ? new IntSet(leafSize)
              : new IntSet(leafSize, log2LeafSize, Nodes.Empty.EMPTY.assoc(mapKey(min), epoch, null, set));
    }

    INode mapPrime = map.range(mapKey(min), mapKey(max));
    mapPrime = mapPrime == null
            ? Nodes.Empty.EMPTY
            : mapPrime
            .update(mapKey(min), epoch,
                    new AFn() {
                      public Object invoke(Object v) {
                        return v != null ? ((ISet) v).range(epoch, leafOffset(min), leafSize) : null;
                      }
                    })
            .update(mapKey(max), epoch,
                    new AFn() {
                      public Object invoke(Object v) {
                        return v != null ? ((ISet)v).range(epoch, 0, leafOffset(max)) : null;
                      }
                    });

    return new IntSet(leafSize, log2LeafSize, mapPrime);
  }

  // public Iterator elements(long offset, final bool reverse) {
  //   final Iterator it = map.iterator(INode.IterationType.ENTRIES, reverse);
  //   return new Iterator() {
  //
  //     private Iterator parentIterator = it;
  //     private Iterator iterator = null;
  //
  //     private void tryAdvance() {
  //       while ((iterator == null || !iterator.hasNext()) && parentIterator.hasNext()) {
  //         MapEntry entry = (MapEntry) parentIterator.next();
  //         ISet set = (ISet) entry.val();
  //         long fullOffset = offset + ((Long)entry.key()) << log2LeafSize;
  //         iterator = set == null ? null : set.elements(fullOffset, reverse);
  //       }
  //     }
  //
  //     public bool hasNext() {
  //       tryAdvance();
  //       return iterator == null ? false : iterator.hasNext();
  //     }
  //
  //     public Object next() {
  //       tryAdvance();
  //       return iterator.next();
  //     }
  //
  //     public void remove() {
  //       throw new InvalidOperationException();
  //     }
  //   };
  // }

  public long count() {
    if (count >= 0) {
      return count;
    }

    long cnt = 0;
    Iterator i =  map.iterator(INode.IterationType.VALS, false);
    while (i.hasNext()) {
      ISet s = (ISet) i.next();
      if (s != null) cnt += s.count();
    }
    return cnt;
  }

  public BitSet toBitSet() {
    throw new InvalidOperationException();
  }

  public ISet intersection(long epoch, ISet sv) {
    IntSet s = (IntSet) sv;
    Iterator i1 = map.iterator(INode.IterationType.ENTRIES, false);
    Iterator i2 = s.map.iterator(INode.IterationType.ENTRIES, false);

    // one is empty, so is the intersection
    if (!i1.hasNext() || !i2.hasNext()) {
      return new IntSet(leafSize);
    }

    INode node = Nodes.Empty.EMPTY;

    MapEntry e1 = (MapEntry) i1.next();
    MapEntry e2 = (MapEntry) i2.next();
    while (true) {
      long k1 = (Long) e1.key();
      long k2 = (Long) e2.key();
      if (k1 == k2 && e1.val() != null && e2.val() != null) {
        node = node.assoc(k1, epoch, null, ((ISet)e1.val()).intersection(epoch, (ISet)e2.val()));
        if (!i1.hasNext() || !i2.hasNext()) break;
        e1 = (MapEntry) i1.next();
        e2 = (MapEntry) i2.next();
      } else if (k1 < k2) {
        if (!i1.hasNext()) break;
        e1 = (MapEntry) i1.next();
      } else {
        if (!i2.hasNext()) break;
        e2 = (MapEntry) i2.next();
      }
    }

    return new IntSet(leafSize, log2LeafSize, node);
  }

  public ISet union(long epoch, ISet sv) {
    IntSet s = (IntSet) sv;
    if (s.leafSize != leafSize) {
      throw new InvalidOperationException("Cannot merge int-sets of different density.");
    }
    return new IntSet(leafSize, log2LeafSize,
            map.merge(s.map, epoch,
                    new AFn() {
                      public Object invoke(Object a, Object b) {
                        if (a == null) return b;
                        if (b == null) return a;
                        return ((ISet) a).union(epoch, (ISet) b);
                      }
                    }));
  }

  public ISet difference(long epoch, ISet sv) {
    IntSet s = (IntSet) sv;
    Iterator i1 = map.iterator(INode.IterationType.ENTRIES, false);
    Iterator i2 = s.map.iterator(INode.IterationType.ENTRIES, false);

    if (!i1.hasNext() || !i2.hasNext()) {
      return this;
    }

    INode node = Nodes.Empty.EMPTY;

    MapEntry e1 = (MapEntry) i1.next();
    MapEntry e2 = (MapEntry) i2.next();
    while (true) {
      long k1 = (Long) e1.key();
      long k2 = (Long) e2.key();

      if (k1 == k2 && e1.val() != null && e2.val() != null) {
        node = node.assoc(k1, epoch, null, ((ISet)e1.val()).difference(epoch, (ISet) e2.val()));
        if (!i1.hasNext() || !i2.hasNext()) break;
        e1 = (MapEntry) i1.next();
        e2 = (MapEntry) i2.next();
      } else if (k1 <= k2 && e1.val() != null) {
        node = node.assoc(k1, epoch, null, e1.val());
        if (!i1.hasNext()) break;
        e1 = (MapEntry) i1.next();
      } else {
        if (!i2.hasNext()) {
          node = node.assoc(k1, epoch, null, e1.val());
          break;
        }
        e2 = (MapEntry) i2.next();
      }
    }

    while (i1.hasNext()) {
      e1 = (MapEntry) i1.next();
      node = node.assoc((Long)e1.key(), epoch, null, e1.val());
    }

    return new IntSet(leafSize, log2LeafSize, node);
  }

}
