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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Apache.Arrow
{
    public class StructArray : Array, IArrowRecord
    {
        private IReadOnlyList<IArrowArray> _fields;

        public IReadOnlyList<IArrowArray> Fields =>
            LazyInitializer.EnsureInitialized(ref _fields, InitializeFields);

        public StructArray(
            IArrowType dataType, int length,
            IEnumerable<IArrowArray> children,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(
                dataType, length, nullCount, offset, new[] { nullBitmapBuffer },
                children.Select(child => child.Data)))
        {
        }

        public StructArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Struct);
        }

        public override void Accept(IArrowArrayVisitor visitor)
        {
            switch (visitor)
            {
                case IArrowArrayVisitor<StructArray> structArrayVisitor:
                    structArrayVisitor.Visit(this);
                    break;
                case IArrowArrayVisitor<IArrowRecord> arrowStructVisitor:
                    arrowStructVisitor.Visit(this);
                    break;
                default:
                    visitor.Visit(this);
                    break;
            }
        }

        private IReadOnlyList<IArrowArray> InitializeFields()
        {
            IArrowArray[] result = new IArrowArray[Data.Children.Length];
            for (int i = 0; i < Data.Children.Length; i++)
            {
                var childData = Data.Children[i];
                if (Data.Offset != 0 || childData.Length != Data.Length)
                {
                    childData = childData.Slice(Data.Offset, Data.Length);
                }
                result[i] = ArrowArrayFactory.BuildArray(childData);
            }
            return result;
        }

        IRecordType IArrowRecord.Schema => (StructType)Data.DataType;

        int IArrowRecord.ColumnCount => Fields.Count;

        IArrowArray IArrowRecord.Column(string columnName, IEqualityComparer<string> comparer) =>
            Fields[((StructType)Data.DataType).GetFieldIndex(columnName, comparer)];

        IArrowArray IArrowRecord.Column(int columnIndex) => Fields[columnIndex];

        public class Builder : IArrowArrayBuilder<StructArray, Builder>
        {
            private readonly IArrowType _dataType;
            private readonly IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>>[] _fieldBuilders;
            private readonly ArrowBuffer.BitmapBuilder _validityBufferBuilder;
            private int _length;
            private int _nullCount;

            public Builder(IArrowType dataType)
            {
                _dataType = dataType;
                _fieldBuilders = new IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>>[0];
                _validityBufferBuilder = new ArrowBuffer.BitmapBuilder();
            }

            public Builder Append()
            {
                _validityBufferBuilder.Append(true);
                _length++;
                return this;
            }

            public Builder AppendNull()
            {
                _validityBufferBuilder.Append(false);
                _nullCount++;
                _length++;
                return this;
            }

            /// <summary>
            /// Sets the value at the specified index to null.
            /// </summary>
            /// <param name="index">The index to set to null.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder SetNull(int index)
            {
                if (index < 0 || index >= _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                // Set the validity buffer to false at the given index
                _validityBufferBuilder.Set(index, false);

                // Set all field values to null
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.SetNull(index);
                }

                return this;
            }

            public StructArray Build(MemoryAllocator allocator = default)
            {
                ArrowBuffer validityBuffer = _nullCount > 0
                    ? _validityBufferBuilder.Build(allocator)
                    : ArrowBuffer.Empty;

                var children = new IArrowArray[_fieldBuilders.Length];
                for (int i = 0; i < _fieldBuilders.Length; i++)
                {
                    children[i] = _fieldBuilders[i].Build(allocator);
                }

                return new StructArray(_dataType, _length, children, validityBuffer, _nullCount);
            }

            public Builder Reserve(int capacity)
            {
                _validityBufferBuilder.Reserve(capacity);
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.Reserve(capacity);
                }
                return this;
            }

            public Builder Resize(int length)
            {
                _validityBufferBuilder.Resize(length);
                foreach (var fieldBuilder in _fieldBuilders)
                {
                    fieldBuilder.Resize(length);
                }
                _length = length;
                return this;
            }

            public Builder Clear()
            {
                _validityBufferBuilder.Clear();
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
