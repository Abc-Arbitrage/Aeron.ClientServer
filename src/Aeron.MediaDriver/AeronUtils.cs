using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Adaptive.Aeron;
using log4net;

namespace Aeron.MediaDriver
{
    public static class AeronUtils
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(AeronUtils));

        public const byte CurrentProtocolVersion = 1;

        public const string IpcChannel = "aeron:ipc";

        public static string RemoteChannel(string host, int port)
            => $"aeron:udp?endpoint={host}:{port}";

        public static string GroupIdToPath(string groupId)
        {
            if (string.IsNullOrEmpty(groupId))
                throw new ArgumentException("Empty group id");

            return Path.Combine(Path.GetTempPath(), $"Aeron-{groupId}");
        }

        public static AeronResultType InterpretPublicationOfferResult(long errorCode)
        {
            if (errorCode >= 0)
                return AeronResultType.Success;

            switch (errorCode)
            {
                case Publication.BACK_PRESSURED:
                case Publication.ADMIN_ACTION:
                    return AeronResultType.ShouldRetry;

                case Publication.NOT_CONNECTED:
                    _log.Error("Trying to send to an unconnected publication");
                    return AeronResultType.Error;

                case Publication.CLOSED:
                    _log.Error("Trying to send to a closed publication");
                    return AeronResultType.Error;

                case Publication.MAX_POSITION_EXCEEDED:
                    _log.Error("Max position exceeded");
                    return AeronResultType.Error;

                default:
                    _log.Error($"Unknown error code: {errorCode}");
                    return AeronResultType.Error;
            }
        }
        
        /// <summary>
        /// Get local IP address that OS chooses for the remote destination.
        /// This should be more reliable than iterating over local interfaces
        /// and trying to guess the right one.
        /// </summary>
        public static string GetLocalIPAddress(string serverHost, int serverPort)
        {
            using Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, 0);
            socket.Connect(serverHost, serverPort);
            if (socket.LocalEndPoint is IPEndPoint endPoint)
                return endPoint.Address.ToString();
            throw new InvalidOperationException("socket.LocalEndPoint is not IPEndPoint");
        }
    }
}
