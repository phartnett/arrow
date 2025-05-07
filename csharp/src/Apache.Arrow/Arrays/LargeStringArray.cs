﻿// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow;

public class LargeStringArray: LargeBinaryArray, IReadOnlyList<string>, ICollection<string>
{
    public static readonly Encoding DefaultEncoding = StringArray.DefaultEncoding;

    public LargeStringArray(ArrayData data)
        : base(ArrowTypeId.LargeString, data) { }

    public LargeStringArray(int length,
        ArrowBuffer valueOffsetsBuffer,
        ArrowBuffer dataBuffer,
        ArrowBuffer nullBitmapBuffer,
        int nullCount = 0, int offset = 0)
        : this(new ArrayData(LargeStringType.Default, length, nullCount, offset,
            new[] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
    { }

    public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

    /// <summary>
    /// Get the string value at the given index
    /// </summary>
    /// <param name="index">Input index</param>
    /// <param name="encoding">Optional: the string encoding, default is UTF8</param>
    /// <returns>The string object at the given index</returns>
    public string GetString(int index, Encoding encoding = default)
    {
        encoding ??= DefaultEncoding;

        ReadOnlySpan<byte> bytes = GetBytes(index, out bool isNull);

        if (isNull)
        {
            return null;
        }

        if (bytes.Length == 0)
        {
            return string.Empty;
        }

        unsafe
        {
            fixed (byte* data = &MemoryMarshal.GetReference(bytes))
            {
                return encoding.GetString(data, bytes.Length);
            }
        }
    }


    int IReadOnlyCollection<string>.Count => Length;

    string IReadOnlyList<string>.this[int index] => GetString(index);

    IEnumerator<string> IEnumerable<string>.GetEnumerator()
    {
        for (int index = 0; index < Length; index++)
        {
            yield return GetString(index);
        };
    }

    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<string>)this).GetEnumerator();

    int ICollection<string>.Count => Length;
    bool ICollection<string>.IsReadOnly => true;
    void ICollection<string>.Add(string item) => throw new NotSupportedException("Collection is read-only.");
    bool ICollection<string>.Remove(string item) => throw new NotSupportedException("Collection is read-only.");
    void ICollection<string>.Clear() => throw new NotSupportedException("Collection is read-only.");

    bool ICollection<string>.Contains(string item)
    {
        for (int index = 0; index < Length; index++)
        {
            if (GetString(index) == item)
                return true;
        }

        return false;
    }

    void ICollection<string>.CopyTo(string[] array, int arrayIndex)
    {
        for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
        {
            array[destIndex] = GetString(srcIndex);
        }
    }

    public new class Builder : LargeBinaryArray.Builder
    {
        public Builder() : base(LargeStringType.Default) { }

        protected override LargeStringArray Build(ArrayData data)
        {
            return new LargeStringArray(data);
        }

        public Builder Append(string value, Encoding encoding = null)
        {
            if (value == null)
            {
                return AppendNull();
            }
            encoding = encoding ?? DefaultEncoding;
            byte[] span = encoding.GetBytes(value);
            return Append(span.AsSpan());
        }

        /// <summary>
        /// Sets the value at the specified index to null.
        /// </summary>
        /// <param name="index">The index to set to null.</param>
        /// <returns>Returns the builder (for fluent-style composition).</returns>
        public Builder SetNull(int index)
        {
            if (index < 0 || index >= Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            ValidityBuffer.Set(index, false);
            // Clear the bytes in the value buffer for this index
            long startOffset = ValueOffsetsBuffer.Span.CastTo<long>()[index];
            long endOffset = ValueOffsetsBuffer.Span.CastTo<long>()[index + 1];
            for (long i = startOffset; i < endOffset; i++)
            {
                ValueBuffer.Set((int)i, 0);
            }
            return this;
        }
    }
}
