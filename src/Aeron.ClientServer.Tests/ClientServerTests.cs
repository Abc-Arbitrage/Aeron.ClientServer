using System;
using System.IO;
using System.Linq;
using System.Threading;
using NUnit.Framework;

namespace Abc.Aeron.ClientServer.Tests
{
    [TestFixture]
    public class ClientServerTests
    {
        [Test, Explicit("manual")]
        public void CouldStartClientServer()
        {
            const bool deleteOnShutdown = true;
            var baseDir = @"D:\tmp\aeron";
            var serverDir = Path.Combine(baseDir!, "server-" + Guid.NewGuid().ToString("N"));
            var clientDir = Path.Combine(baseDir!, "client-" + Guid.NewGuid().ToString("N"));

            var config = ClientServerConfig.SharedNetworkSleeping(serverDir);
            config.DirDeleteOnShutdown = deleteOnShutdown;

            var serverPort = 43210;
            var server = new AeronServer(serverPort, config);

            server.MessageReceived += (identity, message) =>
            {
                Console.WriteLine("SERVER: MESSAGE RECEIVED");
                server.Send((int)identity, message);
            };

            server.ClientConnected += l =>
            {
                Console.WriteLine($"SERVER: CLIENT CONNECTED {l}");
            };

            server.ClientDisconnected += l =>
            {
                Console.WriteLine($"SERVER: CLIENT DISCONNECTED {l}");
            };
            
            server.Start();

            var mre = new ManualResetEventSlim(false);
            var connectedMre = new ManualResetEventSlim(false);

            var client = AeronClient.GetClientReference(clientDir,
                                                        dir =>
                                                        {
                                                            var config = ClientServerConfig.SharedNetworkSleeping(dir);
                                                            config.DirDeleteOnShutdown = deleteOnShutdown;
                                                            return config;
                                                        });
            client.Start();

            var timeoutMs = 10000;
            byte[] received = null;
            var connectionId = client.Connect("127.0.0.1",
                                              serverPort,
                                              () =>
                                              {
                                                  Console.WriteLine("CLIENT: CONNECTED");
                                                  connectedMre.Set();
                                              },
                                              () =>
                                              {
                                                  Console.WriteLine("CLIENT: DISCONNECTED");
                                              },
                                              message =>
                                              {
                                                  Console.WriteLine($"CLIENT: MESSAGE RECEIVED with length: {message.Length}");
                                                  received = message.ToArray();
                                                  mre.Set();
                                              },
                                              timeoutMs);

            connectedMre.Wait();
            var sent = new byte[] { 1, 2, 3, 4, 5 };
            client.Send(connectionId, sent);

            mre.Wait(timeoutMs);
            Assert.True(sent.SequenceEqual(received));

            client.Disconnect(connectionId);

            Thread.Sleep(1000);

            client.Dispose();
            
            server.Dispose();
        }
    }
}
