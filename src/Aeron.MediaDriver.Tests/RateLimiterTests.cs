using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Aeron.MediaDriver.Tests
{
    [TestFixture]
    [SuppressMessage("ReSharper", "HeapView.ClosureAllocation")]
    public class RateLimiterTests
    {
        private volatile bool _isRunning;
        private static readonly Stopwatch _sw = new Stopwatch();
        private static long _counter;
        private const int _iterations = 10_000_000;
        

        [Test, Explicit("manual long running")]
        public async Task RateLimiterNeitherExceedsLimitNorDrifts()
        {
            (long newHead, long newTicks)[] data = new (long, long)[_iterations];

            _isRunning = true;
            var rateLimiter = new RateLimiter();
            var writers = Math.Max(3, Environment.ProcessorCount / 2);
            Console.WriteLine($"Using {writers} number of writers");
            var tasks = new Task[writers];

            for (int i = 0; i < writers; i++)
            {
                var ii = i;
                tasks[i] = Task.Factory.StartNew(() =>
                {
                    var c = 0;
                    
                    while (_isRunning)
                    {
                        var bufferSize = 97 * (1 + ii);
                        rateLimiter.ApplyRateLimit(bufferSize, out var newHead, out var newTicks);
                        var idx = Interlocked.Increment(ref _counter);
                        if(idx >= _iterations)
                            return;

                        data[idx] = (newHead, newTicks);
                        c++;
                    }
                }, TaskCreationOptions.LongRunning);
            }

            await Task.WhenAll(tasks);

            data = data.OrderBy(x => x.Item2).ToArray();

            double previousTicks = data[0].newTicks;
            double previousWritten = data[0].newHead;
            double previousMaxBw = 0;
            
            
            for (int i = 1; i < _iterations; i++)
            {
                var currentTicks = data[i].newTicks;
                var currentWritten = data[i].newHead;
                
                if (currentWritten - previousWritten >= RateLimiter.CHUNK_SIZE)
                {
                    var bw = Math.Round((8 * (currentWritten - previousWritten) / ((currentTicks - previousTicks) / Stopwatch.Frequency)) / (1024 * 1024), 3);

                    if (bw > previousMaxBw)
                    {
                        Console.WriteLine($"New Max BW: {bw:N2} over {currentWritten - previousWritten:N0} bytes");
                        previousMaxBw = bw;
                    }

                    if (bw > 1.5 * rateLimiter.BwLimitBytes * 8 / (1024 * 1024))
                    {
                        Console.WriteLine($"High BW: {bw:N2}");
                    }

                    if (i % 1000 == 0)
                    {
                        Console.WriteLine($"BW: {bw:N2}, Max BW: {previousMaxBw:N2}");
                    }

                    previousTicks = currentTicks;
                    previousWritten = currentWritten;

                }
            }

            _isRunning = false;
            
            Console.WriteLine("Finished...");
        }

        private static void NOP(double durationSeconds)
        {
            _sw.Restart();
            if (Math.Abs(durationSeconds) < 0.000_000_01)
            {
                return;
            }

            var durationTicks = Math.Round(durationSeconds * Stopwatch.Frequency);

            while (_sw.ElapsedTicks < durationTicks)
            {
                Thread.SpinWait(1);
            }
        }
    }
}
