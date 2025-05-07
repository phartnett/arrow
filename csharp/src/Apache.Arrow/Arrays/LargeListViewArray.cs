using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Types;
using Apache.Arrow.Buffers;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Arrays
{
    public class LargeListViewArray : Array
    {
        public class Builder : IArrowArrayBuilder<LargeListViewArray, Builder>
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length => ValueOffsetsBufferBuilder.Length;

            private ArrowBuffer.Builder<long> ValueOffsetsBufferBuilder { get; }

            private ArrowBuffer.Builder<long> SizesBufferBuilder { get; }

            private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }

            public int NullCount { get; protected set; }

            private IArrowType DataType { get; }

            private long Start { get; set; }

            public Builder(IArrowType valueDataType) : this(new LargeListViewType(valueDataType))
            {
            }

            public Builder(Field valueField) : this(new LargeListViewType(valueField))
            {
            }

            internal Builder(LargeListViewType dataType)
            {
                ValueBuilder = ArrowArrayBuilderFactory.Build(dataType.ValueDataType);
                ValueOffsetsBufferBuilder = new ArrowBuffer.Builder<long>();
                SizesBufferBuilder = new ArrowBuffer.Builder<long>();
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();
                DataType = dataType;
                Start = -1;
            }

            /// <summary>
            /// Start a new variable-length list slot
            ///
            /// This function should be called before beginning to append elements to the
            /// value builder. TODO: Consider adding builder APIs to support construction
            /// of overlapping lists.
            /// </summary>
            public Builder Append()
            {
                AppendPrevious();

                ValidityBufferBuilder.Append(true);

                return this;
            }

            public Builder AppendNull()
            {
                AppendPrevious();

                ValidityBufferBuilder.Append(false);
                ValueOffsetsBufferBuilder.Append(Start);
                SizesBufferBuilder.Append(0);
                NullCount++;
                Start = -1;

                return this;
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

                // Set the validity buffer to false at the given index
                ValidityBufferBuilder.Set(index, false);

                // Update the value offsets and sizes buffers to maintain consistency
                long startOffset = ValueOffsetsBufferBuilder.Span[index];
                long size = SizesBufferBuilder.Span[index];

                // Clear the values in the value builder for this index
                for (long i = 0; i < size; i++)
                {
                    ValueBuilder.SetNull((int)(startOffset + i));
                }

                // Set the size to 0 for this index
                SizesBufferBuilder.Set(index, 0);

                return this;
            }

            private void AppendPrevious()
            {
                if (Start >= 0)
                {
                    ValueOffsetsBufferBuilder.Append(Start);
                    SizesBufferBuilder.Append(ValueBuilder.Length - Start);
                }
                Start = ValueBuilder.Length;
            }

            public LargeListViewArray Build(MemoryAllocator allocator = default)
            {
                AppendPrevious();

                ArrowBuffer validityBuffer = NullCount > 0
                                        ? ValidityBufferBuilder.Build(allocator)
                                        : ArrowBuffer.Empty;

                return new LargeListViewArray(DataType, Length,
                    ValueOffsetsBufferBuilder.Build(allocator), SizesBufferBuilder.Build(allocator),
                    ValueBuilder.Build(allocator),
                    validityBuffer, NullCount, 0);
            }

            public Builder Reserve(int capacity)
            {
                ValueOffsetsBufferBuilder.Reserve(capacity);
                SizesBufferBuilder.Reserve(capacity);
                ValidityBufferBuilder.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                ValueOffsetsBufferBuilder.Resize(length);
                SizesBufferBuilder.Resize(length);
                ValidityBufferBuilder.Resize(length);
                return this;
            }

            public Builder Clear()
            {
                ValueOffsetsBufferBuilder.Clear();
                SizesBufferBuilder.Clear();
                ValueBuilder.Clear();
                ValidityBufferBuilder.Clear();
                return this;
            }
        }
    }
} 