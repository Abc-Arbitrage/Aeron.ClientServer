using System;
using System.Diagnostics;
using System.Runtime.ConstrainedExecution;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Agrona.Concurrent;
using Aeron.MediaDriver.Native;
using log4net;

namespace Aeron.MediaDriver
{
    public class AeronConnection : CriticalFinalizerObject, IDisposable
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(AeronConnection));

        public readonly Adaptive.Aeron.Aeron Aeron;
        private readonly Driver _driver;

        public bool IsRunning => !Aeron.IsClosed;

        public AeronConnection(DriverConfig config)
        {
            _driver = Driver.Start(config);

            var aeronContext = new Adaptive.Aeron.Aeron.Context()
                               .AvailableImageHandler(i => ImageAvailable?.Invoke(i))
                               .UnavailableImageHandler(i => ImageUnavailable?.Invoke(i))
                               .AeronDirectoryName(config.Dir)
                               .DriverTimeoutMs(Debugger.IsAttached ? 120 * 60 * 1000 : 30_000)
                               .ErrorHandler(OnError)
                               .ResourceLingerDurationNs(0)
                ;

            Aeron = Adaptive.Aeron.Aeron.Connect(aeronContext);
        }

        private void OnError(Exception exception)
        {
            _log.Error("Aeron connection error", exception);

            if (Aeron.IsClosed)
                return;

            switch (exception)
            {
                case AeronException _:
                case AgentTerminationException _:
                    _log.Error("Unrecoverable Media Driver error");
                    TerminatedUnexpectedly?.Invoke();
                    break;
            }
        }

        public event Action<Image>? ImageAvailable;
        public event Action<Image>? ImageUnavailable;

        public event Action? TerminatedUnexpectedly;

        public int GetNewClientStreamId()
        {
            while (true)
            {
                var streamId = _driver.GetNewClientStreamId();
                if (streamId != 0 && streamId != 1)
                {
                    return streamId;
                }
            }
        }

        public void Dispose()
        {
            Aeron.Dispose();
            _driver?.Dispose();
        }
    }
}
