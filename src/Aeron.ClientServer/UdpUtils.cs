using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace Abc.Aeron.ClientServer
{
    public static class UdpUtils
    {
        public static int GetRandomUnusedPort()
        {
            using var udpc = new UdpClient(0);
            return ((IPEndPoint)udpc.Client.LocalEndPoint).Port;
        }

        public static IReadOnlyList<int> GetRandomUnusedPorts(int count)
        {
            var ports = new int[count];
            var listeners = new UdpClient[count];

            for (var i = 0; i < count; ++i)
            {
                var listener = new UdpClient(0);
                listeners[i] = listener;
                ports[i] = ((IPEndPoint)listener.Client.LocalEndPoint).Port;
            }

            for (var i = 0; i < count; ++i)
                listeners[i].Dispose();

            return ports;
        }

        public static bool IsPortUnused(int port)
        {
            try
            {
                using var _ = new UdpClient(port);  
            }
            catch (Exception)
            {
                return false;
            }

            return true;
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