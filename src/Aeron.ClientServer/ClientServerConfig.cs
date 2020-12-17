using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Adaptive.Aeron.Driver.Native;

namespace Abc.Aeron.ClientServer
{
    [SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    public class ClientServerConfig
    {
        /// <summary>
        /// Offset (in bytes) of client stream id counter .
        /// </summary>
        internal const int ClientStreamIdCounterOffset = 0;

        public ClientServerConfig(string dir)
        {
            if (string.IsNullOrWhiteSpace(dir))
                throw new ArgumentException("Aeron directory must be a valid path.");
            Dir = Path.GetFullPath(dir);
        }
        
        public string Dir { get; }
        public int DriverTimeout { get; set; } = 10_000;
        
        public bool UseActiveDriverIfPresent { get; set; } = false;
        
        public bool DirDeleteOnStart { get; set; } = true;

        public bool DirDeleteOnShutdown { get; set; } = true;

        // see low-latency config here https://github.com/real-logic/aeron/blob/master/aeron-driver/src/main/resources/low-latency.properties

        /// <summary>
        /// The length in bytes of a publication buffer to hold a term of messages. It must be a power of 2 and be in the range of 64KB to 1GB.
        /// </summary>
        public int TermBufferLength { get; set; } = 16 * 1024 * 1024;
        
        public int SocketSoSndBuf { get; set; } = 1 * 1024 * 1024;
        public int SocketSoRcvBuf { get; set; } = 1 * 1024 * 1024;
        public int RcvInitialWindowLength { get; set; } = 128 * 1024;
        public AeronThreadingModeEnum ThreadingMode { get; set; } = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork;
        public DriverIdleStrategy SenderIdleStrategy { get; set; } = DriverIdleStrategy.YIELDING;
        public DriverIdleStrategy ReceiverIdleStrategy { get; set; } = DriverIdleStrategy.YIELDING;
        public DriverIdleStrategy ConductorIdleStrategy { get; set; } = DriverIdleStrategy.SLEEPING;
        public DriverIdleStrategy SharedNetworkIdleStrategy { get; set; } = DriverIdleStrategy.SPINNING;
        public DriverIdleStrategy SharedIdleStrategy { get; set; } = DriverIdleStrategy.SPINNING;
        public DriverIdleStrategy ClientIdleStrategy { get; set; } = DriverIdleStrategy.YIELDING;

        public string Code
        {
            get
            {
                var code = ThreadingMode switch
                {
                    AeronThreadingModeEnum.AeronThreadingModeDedicated     =>$"acd{SenderIdleStrategy.Name[0].ToString()}",
                    AeronThreadingModeEnum.AeronThreadingModeSharedNetwork =>$"acn{SharedNetworkIdleStrategy.Name[0].ToString()}",
                    AeronThreadingModeEnum.AeronThreadingModeShared        =>$"acs{SharedIdleStrategy.Name[0].ToString()}",
                    _                                                      => throw new ArgumentOutOfRangeException()
                };

                return code;
            }
        }

        public static ClientServerConfig DedicatedYielding(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeDedicated,
                SenderIdleStrategy = DriverIdleStrategy.YIELDING,
                ReceiverIdleStrategy = DriverIdleStrategy.YIELDING,
                ConductorIdleStrategy = DriverIdleStrategy.SLEEPING,
                ClientIdleStrategy = DriverIdleStrategy.YIELDING
            };
        }

        public static ClientServerConfig DedicatedSpinning(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeDedicated,
                SenderIdleStrategy = DriverIdleStrategy.SPINNING,
                ReceiverIdleStrategy = DriverIdleStrategy.SPINNING,
                ConductorIdleStrategy = DriverIdleStrategy.YIELDING,
                ClientIdleStrategy = DriverIdleStrategy.SPINNING
            };
        }

        public static ClientServerConfig DedicatedNoOp(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeDedicated,
                SenderIdleStrategy = DriverIdleStrategy.NOOP,
                ReceiverIdleStrategy = DriverIdleStrategy.NOOP,
                ConductorIdleStrategy = DriverIdleStrategy.SPINNING,
                ClientIdleStrategy = DriverIdleStrategy.NOOP
            };
        }

        public static ClientServerConfig SharedNetworkSleeping(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SenderIdleStrategy = DriverIdleStrategy.SLEEPING,
                ReceiverIdleStrategy = DriverIdleStrategy.SLEEPING,
                SharedNetworkIdleStrategy = DriverIdleStrategy.SLEEPING,
                ConductorIdleStrategy = DriverIdleStrategy.SLEEPING,
                ClientIdleStrategy = DriverIdleStrategy.SLEEPING
            };
        }

        public static ClientServerConfig SharedNetworkBackoff(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SenderIdleStrategy = DriverIdleStrategy.BACKOFF,
                ReceiverIdleStrategy = DriverIdleStrategy.BACKOFF,
                SharedNetworkIdleStrategy = DriverIdleStrategy.BACKOFF,
                ConductorIdleStrategy = DriverIdleStrategy.SLEEPING,
                ClientIdleStrategy = DriverIdleStrategy.BACKOFF
            };
        }

        public static ClientServerConfig SharedNetworkYielding(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SenderIdleStrategy = DriverIdleStrategy.YIELDING,
                ReceiverIdleStrategy = DriverIdleStrategy.YIELDING,
                SharedNetworkIdleStrategy = DriverIdleStrategy.YIELDING,
                ConductorIdleStrategy = DriverIdleStrategy.SLEEPING,
                ClientIdleStrategy = DriverIdleStrategy.YIELDING
            };
        }

        public static ClientServerConfig SharedNetworkSpinning(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SharedNetworkIdleStrategy = DriverIdleStrategy.SPINNING,
                ConductorIdleStrategy = DriverIdleStrategy.YIELDING,
                ClientIdleStrategy = DriverIdleStrategy.SPINNING
            };
        }

        public static ClientServerConfig SharedNetworkNoOp(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeSharedNetwork,
                SharedNetworkIdleStrategy = DriverIdleStrategy.NOOP,
                ConductorIdleStrategy = DriverIdleStrategy.SPINNING,
                ClientIdleStrategy = DriverIdleStrategy.NOOP
            };
        }

        public static ClientServerConfig SharedYielding(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared,
                SharedIdleStrategy = DriverIdleStrategy.YIELDING,
                ClientIdleStrategy = DriverIdleStrategy.YIELDING
            };
        }

        public static ClientServerConfig SharedSpinning(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared,
                SharedIdleStrategy = DriverIdleStrategy.SPINNING,
                ClientIdleStrategy = DriverIdleStrategy.SPINNING
            };
        }

        public static ClientServerConfig SharedNoOp(string directory)
        {
            return new ClientServerConfig(directory)
            {
                ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared,
                SharedIdleStrategy = DriverIdleStrategy.NOOP,
                ClientIdleStrategy = DriverIdleStrategy.NOOP
            };
        }

        internal AeronDriver.DriverContext ToDriverContext()
        {
            return new Adaptive.Aeron.Aeron.Context()
                .AeronDirectoryName(Dir)
                .DriverTimeoutMs(DriverTimeout)
                .DriverContext()
                .UseActiveDriverIfPresent(UseActiveDriverIfPresent)
                .DirDeleteOnStart(DirDeleteOnStart)
                .DirDeleteOnShutdown(DirDeleteOnShutdown)
                .TermBufferLength(TermBufferLength)
                .SocketSndbufLength(SocketSoSndBuf)
                .SocketRcvbufLength(SocketSoRcvBuf)
                .InitialWindowLength(RcvInitialWindowLength)
                .ThreadingMode(ThreadingMode)
                .SenderIdleStrategy(SenderIdleStrategy)
                .ReceiverIdleStrategy(ReceiverIdleStrategy)
                .ConductorIdleStrategy(ConductorIdleStrategy)
                .SharedNetworkIdleStrategy(SharedNetworkIdleStrategy)
                .SenderIdleStrategy(SharedIdleStrategy);
            
            // Note that ClientIdleStrategy is not for the driver, but for Client/Server
        }
    }
}
