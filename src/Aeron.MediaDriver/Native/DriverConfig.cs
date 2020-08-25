using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace Aeron.MediaDriver.Native
{
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    public class DriverConfig
    {
        internal const string ClientCountersFileName = "client.counters";

        /// <summary>
        /// Offset (in bytes) of client stream id counter .
        /// </summary>
        internal const int ClientStreamIdCounterOffset = 0;

        public DriverConfig(string dir)
        {
            Dir = Path.GetFullPath(dir);
        }

        public bool UseActiveIfPresent { get; set; } = false;

        public string Dir { get; }

        public bool DirDeleteOnStart { get; set; } = true;

        public bool DirDeleteOnShutdown { get; set; } = true;

        // see low-latency config here https://github.com/real-logic/aeron/blob/master/aeron-driver/src/main/resources/low-latency.properties

        /// <summary>
        /// The length in bytes of a publication buffer to hold a term of messages. It must be a power of 2 and be in the range of 64KB to 1GB.
        /// </summary>
        public int TermBufferLength { get; set; } = 16 * 1024 * 1024;

        public int DriverTimeout { get; set; } = 10_000;

        public int SocketSoSndBuf { get; set; } = 1 * 1024 * 1024;
        public int SocketSoRcvBuf { get; set; } = 1 * 1024 * 1024;
        public int RcvInitialWindowLength { get; set; } = 128 * 1024;
        public AeronThreadingModeEnum ThreadingMode { get; set; } = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork;
        public IdleStrategy SenderIdleStrategy { get; set; } = IdleStrategy.Yielding;
        public IdleStrategy ReceiverIdleStrategy { get; set; } = IdleStrategy.Yielding;
        public IdleStrategy ConductorIdleStrategy { get; set; } = IdleStrategy.Sleeping;
        public IdleStrategy SharedNetworkIdleStrategy { get; set; } = IdleStrategy.Spinning;
        public IdleStrategy SharedIdleStrategy { get; set; } = IdleStrategy.Spinning;
        public IdleStrategy ClientIdleStrategy { get; set; } = IdleStrategy.Yielding;

        public string Code
        {
            get
            {
                var (mode, idle) = ThreadingMode switch
                {
                    AeronThreadingModeEnum.AeronThreadingModeDedicated     => ("d", SenderIdleStrategy.Name[0].ToString()),
                    AeronThreadingModeEnum.AeronThreadingModeSharedNetwork => ("n", SharedNetworkIdleStrategy.Name[0].ToString()),
                    AeronThreadingModeEnum.AeronThreadingModeShared        => ("s", SharedIdleStrategy.Name[0].ToString()),
                    _                                                      => throw new ArgumentOutOfRangeException()
                };

                return $"ac{mode}{idle}";
            }
        }

        public static DriverConfig DedicatedYielding(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeDedicated,
                SenderIdleStrategy = IdleStrategy.Yielding,
                ReceiverIdleStrategy = IdleStrategy.Yielding,
                ConductorIdleStrategy = IdleStrategy.Sleeping,
                ClientIdleStrategy = IdleStrategy.Yielding
            };
        }

        public static DriverConfig DedicatedSpinning(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeDedicated,
                SenderIdleStrategy = IdleStrategy.Spinning,
                ReceiverIdleStrategy = IdleStrategy.Spinning,
                ConductorIdleStrategy = IdleStrategy.Yielding,
                ClientIdleStrategy = IdleStrategy.Spinning
            };
        }

        public static DriverConfig DedicatedNoOp(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeDedicated,
                SenderIdleStrategy = IdleStrategy.NoOp,
                ReceiverIdleStrategy = IdleStrategy.NoOp,
                ConductorIdleStrategy = IdleStrategy.Spinning,
                ClientIdleStrategy = IdleStrategy.NoOp
            };
        }

        public static DriverConfig SharedNetworkSleeping(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SenderIdleStrategy = IdleStrategy.Sleeping,
                ReceiverIdleStrategy = IdleStrategy.Sleeping,
                SharedNetworkIdleStrategy = IdleStrategy.Sleeping,
                ConductorIdleStrategy = IdleStrategy.Sleeping,
                ClientIdleStrategy = IdleStrategy.Sleeping
            };
        }

        public static DriverConfig SharedNetworkBackoff(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SenderIdleStrategy = IdleStrategy.Backoff,
                ReceiverIdleStrategy = IdleStrategy.Backoff,
                SharedNetworkIdleStrategy = IdleStrategy.Backoff,
                ConductorIdleStrategy = IdleStrategy.Sleeping,
                ClientIdleStrategy = IdleStrategy.Backoff
            };
        }

        public static DriverConfig SharedNetworkYielding(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SenderIdleStrategy = IdleStrategy.Yielding,
                ReceiverIdleStrategy = IdleStrategy.Yielding,
                SharedNetworkIdleStrategy = IdleStrategy.Yielding,
                ConductorIdleStrategy = IdleStrategy.Sleeping,
                ClientIdleStrategy = IdleStrategy.Yielding
            };
        }

        public static DriverConfig SharedNetworkSpinning(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SharedNetworkIdleStrategy = IdleStrategy.Spinning,
                ConductorIdleStrategy = IdleStrategy.Yielding,
                ClientIdleStrategy = IdleStrategy.Spinning
            };
        }

        public static DriverConfig SharedNetworkNoOp(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SharedNetworkIdleStrategy = IdleStrategy.NoOp,
                ConductorIdleStrategy = IdleStrategy.Spinning,
                ClientIdleStrategy = IdleStrategy.NoOp
            };
        }

        public static DriverConfig SharedYielding(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared,
                SharedIdleStrategy = IdleStrategy.Yielding,
                ClientIdleStrategy = IdleStrategy.Yielding
            };
        }

        public static DriverConfig SharedSpinning(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared,
                SharedIdleStrategy = IdleStrategy.Spinning,
                ClientIdleStrategy = IdleStrategy.Spinning
            };
        }

        public static DriverConfig SharedNoOp(string directory)
        {
            return new DriverConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared,
                SharedIdleStrategy = IdleStrategy.NoOp,
                ClientIdleStrategy = IdleStrategy.NoOp
            };
        }
    }
}
