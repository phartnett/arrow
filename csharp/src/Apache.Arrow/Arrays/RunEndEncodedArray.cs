using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Arrays
{
    public class RunEndEncodedArray : Array
    {
        public new class Builder : IArrowArrayBuilder<RunEndEncodedArray, Builder>
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> RunEndBuilder { get; }
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length => RunEndBuilder.Length;

            public int NullCount => ValueBuilder.NullCount;

            public RunEndEncodedType DataType { get; }

            public Builder(RunEndEncodedType dataType, IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> runEndBuilder, IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> valueBuilder)
            {
                DataType = dataType;
                RunEndBuilder = runEndBuilder;
                ValueBuilder = valueBuilder;
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

                ValueBuilder.SetNull(index);
                return this;
            }

            public RunEndEncodedArray Build(MemoryAllocator allocator = default)
            {
                return new RunEndEncodedArray(DataType, Length, RunEndBuilder.Build(allocator), ValueBuilder.Build(allocator));
            }

            public Builder Reserve(int capacity)
            {
                RunEndBuilder.Reserve(capacity);
                ValueBuilder.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                RunEndBuilder.Resize(length);
                ValueBuilder.Resize(length);
                return this;
            }

            public Builder Clear()
            {
                RunEndBuilder.Clear();
                ValueBuilder.Clear();
                return this;
            }
        }
    }
} 