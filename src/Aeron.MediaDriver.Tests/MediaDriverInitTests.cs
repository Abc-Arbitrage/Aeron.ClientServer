using System;
using System.Net;
using System.Net.Sockets;
using Aeron.MediaDriver.Native;
using NUnit.Framework;
using static Aeron.MediaDriver.Tests.DriverConfigUtil;

namespace Aeron.MediaDriver.Tests
{
    [TestFixture]
    public class MediaDriverInitTests
    {
        [Test]
        public void CouldStartAndStopMediaDriverEmbedded()
        {
            var config = CreateMediaDriverConfig();
            config.DirDeleteOnStart = true;
            config.DirDeleteOnShutdown = true;

            var md = Driver.Start(config);

            Assert.IsTrue(Driver.IsDriverActive(config.Dir));

            var counter = md.GetNewClientStreamId();

            Assert.AreEqual(counter + 1, md.GetNewClientStreamId());

            Console.WriteLine($"Counter: {counter + 1}");

            md.Dispose();

            Assert.IsFalse(Driver.IsDriverActive(config.Dir, config.DriverTimeout));
        }

        [Test]
        public void CouldReusePort()
        {
            int port;
            using (var udpc = new UdpClient(0))
            {
                port = ((IPEndPoint) udpc.Client.LocalEndPoint).Port;
                Assert.IsTrue(port > 0);
                udpc.Close();
            }

            var config = CreateMediaDriverConfig();
            using var c = new AeronConnection(config);

            using var _ = c.Aeron.AddSubscription($"aeron:udp?endpoint=0.0.0.0:{port}", 1);
        }
    }
}