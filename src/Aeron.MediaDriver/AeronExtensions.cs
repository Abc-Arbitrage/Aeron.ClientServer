using Adaptive.Agrona.Concurrent;

namespace Aeron.MediaDriver
{
    internal static class AeronExtensions
    {
        public static unsafe void Release(this UnsafeBuffer buffer)
            => buffer.Wrap((byte*)0, 0);
    }
}
