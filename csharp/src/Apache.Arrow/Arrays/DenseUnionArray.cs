// Licensed to the Apache Software Foundation (ASF) under one or more
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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow
{
    public class DenseUnionArray : UnionArray
    {
        public ArrowBuffer ValueOffsetBuffer => Data.Buffers[1];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetBuffer.Span.CastTo<int>().Slice(Offset, Length);

        public DenseUnionArray(
            IArrowType dataType,
            int length,
            IEnumerable<IArrowArray> children,
            ArrowBuffer typeIds,
            ArrowBuffer valuesOffsetBuffer,
            int nullCount = 0,
            int offset = 0)
            : base(new ArrayData(
                dataType, length, nullCount, offset, new[] { typeIds, valuesOffsetBuffer },
                children.Select(child => child.Data)))
        {
            ValidateMode(UnionMode.Dense, Type.Mode);
        }

        public DenseUnionArray(ArrayData data) 
            : base(data)
        {
            ValidateMode(UnionMode.Dense, Type.Mode);
            data.EnsureBufferCount(2);
        }

        protected override bool FieldIsValid(IArrowArray fieldArray, int index)
        {
            return fieldArray.IsValid(ValueOffsets[index]);
        }

        internal new static int ComputeNullCount(ArrayData data)
        {
            var offset = data.Offset;
            var length = data.Length;
            var typeIds = data.Buffers[0].Span.Slice(offset, length);
            var valueOffsets = data.Buffers[1].Span.CastTo<int>().Slice(offset, length);
            var childArrays = new IArrowArray[data.Children.Length];
            for (var childIdx = 0; childIdx < data.Children.Length; ++childIdx)
            {
                childArrays[childIdx] = ArrowArrayFactory.BuildArray(data.Children[childIdx]);
            }

            var nullCount = 0;
            for (var i = 0; i < length; ++i)
            {
                var typeId = typeIds[i];
                var valueOffset = valueOffsets[i];
                nullCount += childArrays[typeId].IsNull(valueOffset) ? 1 : 0;
            }

            return nullCount;
        }

        public class Builder : UnionArray.Builder
        {
            private readonly ArrowBuffer.Builder<int> _valueOffsetsBuilder;

            public Builder(IArrowType dataType) : base(dataType)
            {
                _valueOffsetsBuilder = new ArrowBuffer.Builder<int>();
            }

            /// <summary>
            /// Sets the value at the specified index to null.
            /// </summary>
            /// <param name="index">The index to set to null.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public override Builder SetNull(int index)
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                // Set the validity buffer to false at the given index
                _validityBufferBuilder.Set(index, false);
                _nullCount++;

                // Set the type ID to the first field type
                _typeIdsBuilder.Set(index, 0);

                // Set the offset to the current length of the first field
                _valueOffsetsBuilder.Set(index, _fieldBuilders[0].Length);

                // Set the value to null in the first field
                _fieldBuilders[0].SetNull(_fieldBuilders[0].Length);

                return this;
            }

            public override DenseUnionArray Build(MemoryAllocator allocator = default)
            {
                var typeIds = _typeIdsBuilder.Build(allocator);
                var valueOffsets = _valueOffsetsBuilder.Build(allocator);
                var validityBuffer = _nullCount > 0 ? _validityBufferBuilder.Build(allocator) : ArrowBuffer.Empty;

                var children = new IArrowArray[_fieldBuilders.Length];
                for (int i = 0; i < _fieldBuilders.Length; i++)
                {
                    children[i] = _fieldBuilders[i].Build(allocator);
                }

                return new DenseUnionArray(
                    _dataType,
                    _length,
                    children,
                    typeIds,
                    valueOffsets,
                    _nullCount);
            }

            public override Builder Reserve(int capacity)
            {
                _validityBufferBuilder.Reserve(capacity);
                _typeIdsBuilder.Reserve(capacity);
                _valueOffsetsBuilder.Reserve(capacity);
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.Reserve(capacity);
                }
                return this;
            }

            public override Builder Resize(int length)
            {
                _validityBufferBuilder.Resize(length);
                _typeIdsBuilder.Resize(length);
                _valueOffsetsBuilder.Resize(length);
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.Resize(length);
                }
                _length = length;
                return this;
            }

            public override Builder Clear()
            {
                _validityBufferBuilder.Clear();
                _typeIdsBuilder.Clear();
                _valueOffsetsBuilder.Clear();
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.Clear();
                }
                _length = 0;
                _nullCount = 0;
                return this;
            }
        }
    }
}
