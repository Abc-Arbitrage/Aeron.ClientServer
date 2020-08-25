using System;
using Adaptive.Agrona.Concurrent;
using Aeron.MediaDriver.Native;

namespace Aeron.MediaDriver
{
    public static class ConfigExtensions
    {
        public static IIdleStrategy GetClientIdleStrategy(this IdleStrategy x)
        {
            if (x.Name == IdleStrategy.Sleeping.Name)
                return new SleepingIdleStrategy(1);

            if (x.Name == IdleStrategy.Yielding.Name)
                return new YieldingIdleStrategy();

            if (x.Name == IdleStrategy.Spinning.Name)
                return new BusySpinIdleStrategy();

            if (x.Name == IdleStrategy.NoOp.Name)
                return new NoOpIdleStrategy();

            if (x.Name == IdleStrategy.Backoff.Name)
                return new BackoffIdleStrategy(1, 10, 1, 1);

            throw new ArgumentException();
        }
    }
}
