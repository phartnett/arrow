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

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Apache.Arrow
{
    public abstract class UnionArray : IArrowArray
    {
        protected IReadOnlyList<IArrowArray> _fields;

        public IReadOnlyList<IArrowArray> Fields =>
            LazyInitializer.EnsureInitialized(ref _fields, InitializeFields);

        public ArrayData Data { get; }

        public UnionType Type => (UnionType)Data.DataType;

        public UnionMode Mode => Type.Mode;

        public ArrowBuffer TypeBuffer => Data.Buffers[0];

        public ReadOnlySpan<byte> TypeIds => TypeBuffer.Span.Slice(Offset, Length);

        public int Length => Data.Length;

        public int Offset => Data.Offset;

        public int NullCount => Data.GetNullCount();

        public bool IsValid(int index) => NullCount == 0 || FieldIsValid(Fields[TypeIds[index]], index);

        public bool IsNull(int index) => !IsValid(index);

        protected UnionArray(ArrayData data) 
        {
            Data = data;
            data.EnsureDataType(ArrowTypeId.Union);
        }

        public static UnionArray Create(ArrayData data)
        {
            return ((UnionType)data.DataType).Mode switch
            {
                UnionMode.Dense => new DenseUnionArray(data),
                UnionMode.Sparse => new SparseUnionArray(data),
                _ => throw new InvalidOperationException("unknown union mode in array creation")
            };
        }

        public void Accept(IArrowArrayVisitor visitor) => Array.Accept(this, visitor);

        protected abstract bool FieldIsValid(IArrowArray field, int index);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Data.Dispose();
            }
        }

        protected static void ValidateMode(UnionMode expected, UnionMode actual)
        {
            if (expected != actual)
            {
                throw new ArgumentException(
                    $"Specified union mode <{actual}> does not match expected mode <{expected}>",
                    "Mode");
            }
        }

        internal static int ComputeNullCount(ArrayData data)
        {
            return ((UnionType)data.DataType).Mode switch
            {
                UnionMode.Sparse => SparseUnionArray.ComputeNullCount(data),
                UnionMode.Dense => DenseUnionArray.ComputeNullCount(data),
                _ => throw new InvalidOperationException("unknown union mode in null count computation")
            };
        }

        private IReadOnlyList<IArrowArray> InitializeFields()
        {
            IArrowArray[] result = new IArrowArray[Data.Children.Length];
            for (int i = 0; i < Data.Children.Length; i++)
            {
                var childData = Data.Children[i];
                if (Mode == UnionMode.Sparse && (Data.Offset != 0 || childData.Length != Data.Length))
                {
                    // We only slice the child data for sparse mode,
                    // so that the sliced value offsets remain valid in dense mode
                    childData = childData.Slice(Data.Offset, Data.Length);
                }
                result[i] = ArrowArrayFactory.BuildArray(childData);
            }
            return result;
        }

        public abstract class Builder : IArrowArrayBuilder<UnionArray, Builder>
        {
            protected readonly ArrowBuffer.BitmapBuilder _validityBufferBuilder;
            protected readonly ArrowBuffer.Builder<byte> _typeIdsBuilder;
            protected readonly IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>>[] _fieldBuilders;
            protected int _length;
            protected int _nullCount;

            public Builder(IArrowType dataType)
            {
                _validityBufferBuilder = new ArrowBuffer.BitmapBuilder();
                _typeIdsBuilder = new ArrowBuffer.Builder<byte>();
                _fieldBuilders = new IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>>[0];
            }

            /// <summary>
            /// Sets the value at the specified index to null.
            /// </summary>
            /// <param name="index">The index to set to null.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public virtual Builder SetNull(int index)
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                // Set the validity buffer to false at the given index
                _validityBufferBuilder.Set(index, false);
                _nullCount++;

                // Set all field values to null
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.SetNull(index);
                }

                return this;
            }

            public abstract UnionArray Build(MemoryAllocator allocator = default);
            public abstract Builder Reserve(int capacity);
            public abstract Builder Resize(int length);
            public abstract Builder Clear();
        }
    }
}
