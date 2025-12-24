//  Copyright (c) Zach Tellman, Rich Hickey and contributors. All rights reserved.
//  The use and distribution terms for this software are covered by the
//  Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
//  which can be found in the file epl-v10.html at the root of this distribution.
//  By using this software in any fashion, you are agreeing to be bound by
//  the terms of this license.
//  You must not remove this notice, or any other, from this software.

using System.Collections;
using clojure.lang;

namespace clojure.data.int_map;

public class IntSet : ISet
{
    public volatile int countt = -1;
    public short leafSize, log2LeafSize;

    public INode map;

    public IntSet(short leafSize)
    {
        this.leafSize = leafSize;
        log2LeafSize = (short)Nodes.bitLog2(leafSize);
        map = Nodes.Empty.EMPTY;
    }

    private IntSet(short leafSize, short log2LeafSize, INode map)
    {
        this.leafSize = leafSize;
        this.log2LeafSize = log2LeafSize;
        this.map = map;
    }

    public ISet add(long epoch, long val)
    {
        var mapPrime = map.update(mapKey(val), epoch, new AddFn(leafSize, epoch, val));
        if (mapPrime == map)
        {
            countt = -1;
            return this;
        }

        return new IntSet(leafSize, log2LeafSize, mapPrime);
    }

    public ISet remove(long epoch, long val)
    {
        var mapPrime = map.update(mapKey(val), epoch, new RemoveFn(leafSize, epoch, val));
        if (mapPrime == map)
        {
            countt = -1;
            return this;
        }

        return new IntSet(leafSize, log2LeafSize, mapPrime);
    }

    public bool contains(long val)
    {
        var s = (ISet)map.get(mapKey(val), null);
        return s != null && s.contains(leafOffset(leafSize, val));
    }

    public ISet range(long epoch, long min, long max)
    {
        if (max < min) return new IntSet(leafSize);

        if (mapKey(min) == mapKey(max))
        {
            var set = (ISet)map.get(mapKey(min), null);
            set = set == null ? null : set.range(epoch, leafOffset(leafSize, min), leafOffset(leafSize, max));

            return set == null
                ? new IntSet(leafSize)
                : new IntSet(leafSize, log2LeafSize, Nodes.Empty.EMPTY.assoc(mapKey(min), epoch, null, set));
        }

        var mapPrime = map.range(mapKey(min), mapKey(max));
        mapPrime = mapPrime == null
            ? Nodes.Empty.EMPTY
            : mapPrime
                .update(mapKey(min), epoch, new RangeFnA(leafSize, epoch, min))
                .update(mapKey(max), epoch, new RangeFnB(leafSize, epoch, max));

        return new IntSet(leafSize, log2LeafSize, mapPrime);
    }

    public IEnumerator elements(long offset, bool reverse)
    {
        var it = map.iterator(INode.IterationType.ENTRIES, reverse);
        while (it.MoveNext())
        {
            var entry = (MapEntry)it.Current;
            var set = (ISet)entry.val();
            var fullOffset = (offset + (long)entry.key()) << log2LeafSize;
            var iterator = set == null ? null : set.elements(fullOffset, reverse);
            while (iterator.MoveNext()) yield return iterator.Current;
        }
    }

    public long count()
    {
        if (countt >= 0) return countt;

        long cnt = 0;
        var i = map.iterator(INode.IterationType.VALS, false);
        while (i.MoveNext())
        {
            var s = (ISet)i.Current;
            if (s != null) cnt += s.count();
        }

        return cnt;
    }

    public BitSet toBitSet()
    {
        throw new InvalidOperationException();
    }

    public ISet intersection(long epoch, ISet sv)
    {
        var s = (IntSet)sv;
        var i1 = map.iterator(INode.IterationType.ENTRIES, false);
        var i2 = s.map.iterator(INode.IterationType.ENTRIES, false);

        // one is empty, so is the intersection
        if (!i1.MoveNext() || !i2.MoveNext()) return new IntSet(leafSize);
        ;

        INode node = Nodes.Empty.EMPTY;

        var e1 = (MapEntry)i1.Current;
        var e2 = (MapEntry)i2.Current;
        while (true)
        {
            if (e1 == null || e2 == null)
                throw new InvalidOperationException("Something went wrong");
            var k1 = (long)e1.key();
            var k2 = (long)e2.key();
            if (k1 == k2 && e1.val() != null && e2.val() != null)
            {
                node = node.assoc(k1, epoch, null, ((ISet)e1.val()).intersection(epoch, (ISet)e2.val()));
                if (!i1.MoveNext() || !i2.MoveNext()) break;
                e1 = (MapEntry)i1.Current;
                e2 = (MapEntry)i2.Current;
            }
            else if (k1 < k2)
            {
                if (!i1.MoveNext()) break;
                e1 = (MapEntry)i1.Current;
            }
            else
            {
                if (!i2.MoveNext()) break;
                e2 = (MapEntry)i2.Current;
            }
        }

        return new IntSet(leafSize, log2LeafSize, node);
    }

    public ISet union(long epoch, ISet sv)
    {
        var s = (IntSet)sv;
        if (s.leafSize != leafSize) throw new InvalidOperationException("Cannot merge int-sets of different density.");

        return new IntSet(leafSize, log2LeafSize, map.merge(s.map, epoch, new UnionFn(epoch)));
    }

    public ISet difference(long epoch, ISet sv)
    {
        var s = (IntSet)sv;
        var i1 = map.iterator(INode.IterationType.ENTRIES, false);
        var i2 = s.map.iterator(INode.IterationType.ENTRIES, false);

        if (!i1.MoveNext() || !i2.MoveNext()) return this;
        ;

        INode node = Nodes.Empty.EMPTY;

        var e1 = (MapEntry)i1.Current;
        var e2 = (MapEntry)i2.Current;
        while (true)
        {
            if (e1 == null || e2 == null)
                throw new InvalidOperationException("Something went wrong");

            var k1 = (long)e1.key();
            var k2 = (long)e2.key();

            if (k1 == k2 && e1.val() != null && e2.val() != null)
            {
                node = node.assoc(k1, epoch, null, ((ISet)e1.val()).difference(epoch, (ISet)e2.val()));
                if (!i1.MoveNext() || !i2.MoveNext()) break;
                e1 = (MapEntry)i1.Current;
                e2 = (MapEntry)i2.Current;
            }
            else if (k1 <= k2 && e1.val() != null)
            {
                node = node.assoc(k1, epoch, null, e1.val());
                if (!i1.MoveNext()) break;
                e1 = (MapEntry)i1.Current;
            }
            else
            {
                if (!i2.MoveNext())
                {
                    node = node.assoc(k1, epoch, null, e1.val());
                    break;
                }

                e2 = (MapEntry)i2.Current;
            }
        }

        while (i1.MoveNext())
        {
            e1 = (MapEntry)i1.Current;
            node = node.assoc((long)e1.key(), epoch, null, e1.val());
        }

        return new IntSet(leafSize, log2LeafSize, node);
    }

    // public int leafSize() {
    //   return this.leafSize;
    // }

    private long mapKey(long val)
    {
        return val >> log2LeafSize;
    }

    private static short leafOffset(short leafSize, long val)
    {
        return (short)(val & (leafSize - 1));
    }

    public class BitSetContainer : ISet
    {
        public BitSet bitSet;
        public long epoch;

        public BitSetContainer(long epoch, BitSet bitSet)
        {
            this.epoch = epoch;
            this.bitSet = bitSet;
        }

        public ISet add(long epoch, long val)
        {
            var bitSet = (BitSet)this.bitSet.Clone();
            if (epoch == this.epoch)
            {
                bitSet.Set((short)val);
                return this;
            }

            bitSet.Set((short)val);
            return new BitSetContainer(epoch, bitSet);
        }

        public ISet remove(long epoch, long val)
        {
            var bitSet = (BitSet)this.bitSet.Clone();
            if (epoch == this.epoch)
            {
                bitSet.Set((short)val, false);
                return this;
            }

            bitSet.Set((short)val, false);
            return new BitSetContainer(epoch, bitSet);
        }

        public bool contains(long val)
        {
            return bitSet.Get((short)val);
        }

        public ISet range(long epoch, long min, long max)
        {
            var bitSet = (BitSet)this.bitSet.Clone();

            var size = bitSet.Size;
            bitSet.Set(0, (int)Math.Max(min, 0), false);
            if (max < size) bitSet.Set(Math.Min((short)max + 1, size), size, false);
            return new BitSetContainer(epoch, bitSet);
        }

        public IEnumerator elements(long offset, bool reverse)
        {
            ArrayList ns = new(bitSet.Cardinality());
            var idx = 0;
            while (idx < bitSet.Length)
            {
                idx = bitSet.NextSetBit(idx);
                ns.Add(offset + idx);
                idx++;
            }

            if (reverse) ns.Reverse();
            return ns.GetEnumerator();
        }

        public long count()
        {
            return bitSet.Cardinality();
        }

        public BitSet toBitSet()
        {
            return bitSet;
        }

        public ISet intersection(long epoch, ISet val)
        {
            var bitSet = (BitSet)this.bitSet.Clone();
            bitSet.And(val.toBitSet());
            return new BitSetContainer(epoch, bitSet);
        }

        public ISet union(long epoch, ISet val)
        {
            var bitSet = (BitSet)this.bitSet.Clone();
            bitSet.Or(val.toBitSet());
            return new BitSetContainer(epoch, bitSet);
        }

        public ISet difference(long epoch, ISet val)
        {
            var bitSet = (BitSet)this.bitSet.Clone();
            bitSet.AndNot(val.toBitSet());
            return new BitSetContainer(epoch, bitSet);
        }
    }

    public class SingleContainer : ISet
    {
        public short val;

        public SingleContainer(short val)
        {
            this.val = val;
        }

        public ISet add(long epoch, long val)
        {
            if (val == this.val) return this;

            var bitSet = new BitSet(Math.Max((short)val, this.val));
            bitSet.Set((short)val);
            bitSet.Set(this.val);
            return new BitSetContainer(epoch, bitSet);
        }

        public ISet remove(long epoch, long val)
        {
            return val == this.val ? null : this;
        }

        public bool contains(long val)
        {
            return val == this.val;
        }

        public ISet range(long epoch, long min, long max)
        {
            return min <= val && max >= val ? this : null;
        }

        public long count()
        {
            return 1;
        }

        public IEnumerator elements(long offset, bool reverse)
        {
            yield return val + offset;
        }

        public BitSet toBitSet()
        {
            var bitSet = new BitSet(val);
            bitSet.Set(val);
            return bitSet;
        }

        public ISet intersection(long epoch, ISet sv)
        {
            return sv == null
                ? null
                : sv.contains(val)
                    ? this
                    : null;
        }

        public ISet union(long epoch, ISet sv)
        {
            return sv == null
                ? this
                : sv.contains(val)
                    ? sv
                    : sv.add(epoch, val);
        }

        public ISet difference(long epoch, ISet sv)
        {
            return sv == null
                ? this
                : sv.contains(val)
                    ? null
                    : this;
        }
    }

    private class AddFn : AFn
    {
        public readonly long epoch;
        public readonly short leafSize;
        public readonly long val;

        public AddFn(short leafSize, long epoch, long val)
        {
            this.leafSize = leafSize;
            this.epoch = epoch;
            this.val = val;
        }

        public override object invoke(object v)
        {
            var s = (ISet)v;
            return s == null ? new SingleContainer(leafOffset(leafSize, val)) : s.add(epoch, leafOffset(leafSize, val));
        }
    }

    private class RemoveFn : AFn
    {
        public readonly long epoch;
        public readonly short leafSize;
        public readonly long val;

        public RemoveFn(short leafSize, long epoch, long val)
        {
            this.leafSize = leafSize;
            this.epoch = epoch;
            this.val = val;
        }

        public override object invoke(object v)
        {
            var s = (ISet)v;
            return s == null ? null : s.remove(epoch, leafOffset(leafSize, val));
        }
    }

    private class RangeFnA : AFn
    {
        public readonly long epoch;
        public readonly short leafSize;
        public readonly long min;

        public RangeFnA(short leafSize, long epoch, long min)
        {
            this.leafSize = leafSize;
            this.epoch = epoch;
            this.min = min;
        }

        public override object invoke(object v)
        {
            return v != null ? ((ISet)v).range(epoch, leafOffset(leafSize, min), leafSize) : null;
        }
    }

    private class RangeFnB : AFn
    {
        public readonly long epoch;
        public readonly short leafSize;
        public readonly long max;

        public RangeFnB(short leafSize, long epoch, long max)
        {
            this.leafSize = leafSize;
            this.epoch = epoch;
            this.max = max;
        }

        public override object invoke(object v)
        {
            return v != null ? ((ISet)v).range(epoch, 0, leafOffset(leafSize, max)) : null;
        }
    }

    private class UnionFn : AFn
    {
        public readonly long epoch;

        public UnionFn(long epoch)
        {
            this.epoch = epoch;
        }

        public override object invoke(object a, object b)
        {
            if (a == null) return b;
            if (b == null) return a;
            return ((ISet)a).union(epoch, (ISet)b);
        }
    }
}