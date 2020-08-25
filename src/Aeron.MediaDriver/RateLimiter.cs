using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Aeron.MediaDriver
{
    public class RateLimiter
    {
        // ReSharper disable once InconsistentNaming
        public const int CHUNK_SIZE = 64 * 1024;

        private const int GROW_FACTOR = 4;

        public double BwLimitBytes { get; }

        private static readonly double _frequency = Stopwatch.Frequency;

        // long is enough for ~22 years at 100 GBits/sec
        private long _head;
        private long _tail;
        private long _tailTicks;

        public RateLimiter(long bandwidthLimitMegabits = 1024)
        {
            BwLimitBytes = 1024.0 * 1024 * bandwidthLimitMegabits / 8;
            _tailTicks = Stopwatch.GetTimestamp();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ApplyRateLimit(int bufferSize)
        {
            ApplyRateLimit(bufferSize, out _, out _);
        }

        public void ApplyRateLimit(int bufferSize, out long newHead, out long newTicks)
        {
            var tail = Volatile.Read(ref _tail);
            var tailTicks = Volatile.Read(ref _tailTicks);

            newHead = Interlocked.Add(ref _head, bufferSize);

            newTicks = Stopwatch.GetTimestamp();

            var initialTicks = tailTicks;

            while (true)
            {
                var writtenBytes = newHead - tail;
                var secondsElapsed = (newTicks - tailTicks) / _frequency;
                var bw = (writtenBytes - (CHUNK_SIZE - (CHUNK_SIZE >> GROW_FACTOR))) / secondsElapsed;

                if (bw <= BwLimitBytes)
                {
                    newTicks = Stopwatch.GetTimestamp();
                    break;
                }

                // just wait, on every iteration GetTimestamp increases and calculated BW decreases
                Thread.SpinWait(1);

                tail = Volatile.Read(ref _tail);
                tailTicks = Volatile.Read(ref _tailTicks);
                newTicks = Stopwatch.GetTimestamp();
            }

            // Only one thread could succeed with CAS
            // First, we update tail ticks, making instant BW higher, then we update the tail.
            if (// initialTicks < newTicks &&
                newHead - Volatile.Read(ref _tail) >= CHUNK_SIZE
                && initialTicks == Interlocked.CompareExchange(ref _tailTicks, newTicks, initialTicks))
            {
                Interlocked.Add(ref _tail, CHUNK_SIZE >> GROW_FACTOR);
            }
        }
    }
}
