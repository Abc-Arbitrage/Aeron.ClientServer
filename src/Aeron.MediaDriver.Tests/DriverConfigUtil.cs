using System;
using System.IO;
using Aeron.MediaDriver.Native;

namespace Aeron.MediaDriver.Tests
{
    public static class DriverConfigUtil
    {
        public static DriverConfig CreateMediaDriverConfig(bool isServer = false)
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var dir = Path.Combine(baseDir, "mediadrivers", (isServer ? "server_" : "") + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            var config = new DriverConfig(dir);
            config.DriverTimeout = 1000;
            config.ThreadingMode = AeronThreadingModeEnum.AeronThreadingModeShared;
            config.SharedIdleStrategy = IdleStrategy.Sleeping;
            config.TermBufferLength = 128 * 1024;
            return config;
        }
    }
}
