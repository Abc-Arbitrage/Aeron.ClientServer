using System;
using Adaptive.Aeron.Driver.Native;
using Adaptive.Agrona.Concurrent;

namespace Abc.Aeron.ClientServer
{
    internal static class Extensions
    {
        public static unsafe void Release(this UnsafeBuffer buffer)
            => buffer.Wrap((byte*)0, 0);
        
        public static IIdleStrategy GetClientIdleStrategy(this DriverIdleStrategy driverIdleStrategy)
        {
            if (driverIdleStrategy.Name == DriverIdleStrategy.SLEEPING.Name)
                return new SleepingIdleStrategy(1);

            if (driverIdleStrategy.Name == DriverIdleStrategy.YIELDING.Name)
                return new YieldingIdleStrategy();

            if (driverIdleStrategy.Name == DriverIdleStrategy.SPINNING.Name)
                return new BusySpinIdleStrategy();

            if (driverIdleStrategy.Name == DriverIdleStrategy.NOOP.Name)
                return new NoOpIdleStrategy();

            if (driverIdleStrategy.Name == DriverIdleStrategy.BACKOFF.Name)
                return new BackoffIdleStrategy(1, 10, 1, 1);

            throw new ArgumentException();
        }
    }
}
