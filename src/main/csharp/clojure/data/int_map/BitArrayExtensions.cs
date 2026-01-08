//  Copyright (c) James Davidson. All rights reserved.
//  The use and distribution terms for this software are covered by the
//  Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
//  which can be found in the file epl-v10.html at the root of this distribution.
//  By using this software in any fashion, you are agreeing to be bound by
//  the terms of this license.
//  You must not remove this notice, or any other, from this software.

using System.Collections;
using System.Reflection;
using System.Runtime.InteropServices;

namespace clojure.data.int_map;

public static class BitArrayExtensions
{
    public static int Cardinality(this BitArray bitArray)
    {
        int acc = 0;
#if BEFOREDOTNET10
        FieldInfo _fieldInfo = typeof(BitArray).GetField("m_array", BindingFlags.NonPublic | BindingFlags.Instance);
        var m_array = (int[])_fieldInfo.GetValue(bitArray);
        for (int i = 0; i < m_array.Length; i++)
        {
            acc += Int32.PopCount(m_array[i]);
        }
#else
        Span<byte> bytes = CollectionsMarshal.AsBytes(bitArray);
        for (int i = 0; i < bytes.Length; i++)
        {
            acc += Int32.PopCount(bytes[i]);
        }
#endif
        return acc;
    }

    public static void AndNot(this BitArray bitArray, BitArray other)
    {
        var otherCopy = (BitArray)other.Clone();
        otherCopy.Length = bitArray.Length;
        otherCopy.Not();
        bitArray.And(otherCopy);
    }

    public static void SafeSet(this BitArray bitArray, int index, bool value)
    {
        if (index >= bitArray.Length)
            bitArray.Length = index + 1;
        bitArray.Set(index, value);
    }
}
