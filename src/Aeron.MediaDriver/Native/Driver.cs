﻿using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Threading;
using log4net;
using static Aeron.MediaDriver.Native.Interop;

namespace Aeron.MediaDriver.Native
{
    public class Driver : CriticalFinalizerObject, IDisposable
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(Driver));

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void LogFuncDelegate([MarshalAs(UnmanagedType.LPStr)] string pChar);

        public DriverConfig Config { get; }

        private volatile bool _isRunning;

        /// <summary>
        /// This instance started the media driver
        /// </summary>
        private bool _ownDriver;

        private IntPtr _ctx;
        private IntPtr _driver;

        private readonly FixedSizeDirectFile _clientCountersFile;
        private readonly EventHandler _processExitHandler;

        private Driver(DriverConfig config)
        {
            Config = config;
            _isRunning = true;

            _processExitHandler = OnCurrentDomainOnProcessExit;

            AppDomain.CurrentDomain.ProcessExit += _processExitHandler;

            if (!Config.UseActiveIfPresent && IsDriverActive(config.Dir, config.DriverTimeout))
            {
                for (int i = 0; i < 12; i++) // 20% more max
                {
                    Thread.Sleep(config.DriverTimeout / 10);
                    if (!IsDriverActive(config.Dir, config.DriverTimeout))
                    {
                        break;
                    }

                    if (i == 11)
                    {
                        var errorMsg =
                            $"Cannot start media driver: there is an active driver in the directory {config.Dir}";
                        _log.Error(errorMsg);
                        throw new InvalidOperationException(errorMsg);
                    }
                }
            }

            if (!IsDriverActive(config.Dir, config.DriverTimeout))
            {
                StartEmbedded(config);
            }

            if (!IsDriverActive(config.Dir, config.DriverTimeout))
                throw new Exception("Cannot start media driver");

            // Client stream id should be unique per machine, not per media driver.
            // Temp is ok, we only need to avoid same stream id among concurrently
            // running clients (within 10 seconds), not forever. If a machine
            // restarts and cleans temp there are no other running client and
            // we could start from 0 again.

            var dirPath =
                Path.Combine(Path.GetTempPath(), "aeron_client_counters"); // any name, but do not start with 'aeron-'
            Directory.CreateDirectory(dirPath);
            _clientCountersFile =
                new FixedSizeDirectFile(Path.Combine(dirPath, DriverConfig.ClientCountersFileName));
        }

        private void OnCurrentDomainOnProcessExit(object? sender, EventArgs args)
        {
            Dispose();
        }

        private void StartEmbedded(DriverConfig config)
        {
            if (!Environment.Is64BitProcess)
                throw new InvalidOperationException("Embedded Aeron Media Driver is not supported on 32 bits");

            _log.Info($"Aeron Media Driver: {AeronVersionFull()}");
            _log.Info($"Using embedded C media driver at dir: {config.Dir}");

            if (AeronDriverContextInit(out _ctx) < 0)
                throw new MediaDriverException($"AeronDriverContextInit: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetDirDeleteOnStart(_ctx, config.DirDeleteOnStart) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetDirDeleteOnStart: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetDirDeleteOnShutdown(_ctx, config.DirDeleteOnShutdown) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetDirDeleteOnShutdown: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetDirWarnIfExists(_ctx, false) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetDirWarnIfExists: ({AeronErrcode()}) {AeronErrmsg()}");

            Directory.CreateDirectory(config.Dir);
            if (AeronDriverContextSetDir(_ctx, config.Dir) < 0)
                throw new MediaDriverException($"AeronDriverContextSetDir: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetDriverTimeoutMs(_ctx, (ulong) config.DriverTimeout) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetDriverTimeoutMs: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetTermBufferSparseFile(_ctx, false) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetTermBufferSparseFile: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetTermBufferLength(_ctx, (IntPtr) config.TermBufferLength) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetTermBufferLength: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetCountersBufferLength(_ctx, (IntPtr) (10 * 1024 * 1024)) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetCountersBufferLength: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetSocketSoSndbuf(_ctx, (IntPtr) config.SocketSoSndBuf) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetSocketSoSndbuf: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetClientLivenessTimeoutNs(_ctx,
                (ulong) (Debugger.IsAttached ? 120 * 60 : 30) * 1_000_000_000) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetClientLivenessTimeoutNs: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetSocketSoRcvbuf(_ctx, (IntPtr) config.SocketSoRcvBuf) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetSocketSoRcvbuf: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetRcvInitialWindowLength(_ctx, (IntPtr) config.RcvInitialWindowLength) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetRcvInitialWindowLength: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverContextSetThreadingMode(_ctx, config.ThreadingMode) < 0)
                throw new MediaDriverException($"AeronDriverContextSetDir: ({AeronErrcode()}) {AeronErrmsg()}");

            _log.Info($"Threading mode: {AeronDriverContextGetThreadingMode(_ctx):G}");

            if (AeronDriverContextSetSenderIdleStrategy(_ctx, config.SenderIdleStrategy.Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetSenderIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");

            _log.Info($"SenderIdleStrategy: {AeronDriverContextGetSenderIdleStrategy(_ctx)}");

            if (AeronDriverContextSetReceiverIdleStrategy(_ctx, config.ReceiverIdleStrategy.Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetReceiverIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");

            _log.Info($"ReceiverIdleStrategy: {AeronDriverContextGetReceiverIdleStrategy(_ctx)}");

            if (AeronDriverContextSetConductorIdleStrategy(_ctx, config.ConductorIdleStrategy.Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetConductorIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");

            _log.Info($"ConductorIdleStrategy: {AeronDriverContextGetConductorIdleStrategy(_ctx)}");

            if (AeronDriverContextSetSharednetworkIdleStrategy(_ctx, config.SharedNetworkIdleStrategy.Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetConductorIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");

            _log.Info($"SharednetworkIdleStrategy: {AeronDriverContextGetSharednetworkIdleStrategy(_ctx)}");

            if (AeronDriverContextSetSharedIdleStrategy(_ctx, config.SharedIdleStrategy.Name) < 0)
                throw new MediaDriverException(
                    $"AeronDriverContextSetSharedIdleStrategy: ({AeronErrcode()}) {AeronErrmsg()}");

            _log.Info($"SharedIdleStrategy: {AeronDriverContextGetSharedIdleStrategy(_ctx)}");

            if (AeronDriverInit(out _driver, _ctx) < 0)
                throw new MediaDriverException($"AeronDriverInit: ({AeronErrcode()}) {AeronErrmsg()}");

            if (AeronDriverStart(_driver, false) < 0)
                throw new MediaDriverException($"AeronDriverStart: ({AeronErrcode()}) {AeronErrmsg()}");

            _ownDriver = true;

            Thread.Sleep(500);
        }

        public static bool IsDriverActive(string directory, int timeoutMs = 10000)
        {
            var callback = new LogFuncDelegate(s => { });
            var result = AeronIsDriverActive(directory, timeoutMs, Marshal.GetFunctionPointerForDelegate(callback));
            GC.KeepAlive(callback);
            return result;
        }

        public int GetNewClientStreamId()
        {
            var id = Interlocked.Increment(
                ref Unsafe.As<byte, int>(ref _clientCountersFile.Span[DriverConfig.ClientStreamIdCounterOffset]));
            _clientCountersFile.Flush(true);
            return id;
        }

        public static Driver Start(DriverConfig config)
        {
            return new Driver(config);
        }

        private void Dispose(bool disposing)
        {
            if (!_isRunning)
                return;

            _isRunning = false;

            _log.Info($"Disposing media driver at: {Config.Dir}");

            AppDomain.CurrentDomain.ProcessExit -= _processExitHandler;
            _clientCountersFile?.Dispose();

            if (!_ownDriver)
                return;


            if (AeronDriverClose(_driver) < 0)
            {
                if (!disposing)
                    return;

                throw new MediaDriverException($"AeronDriverClose: ({AeronErrcode()}) {AeronErrmsg()}");
            }

            if (AeronDriverContextClose(_ctx) < 0)
            {
                if (!disposing)
                    return;

                throw new MediaDriverException($"AeronDriverContextClose: ({AeronErrcode()}) {AeronErrmsg()}");
            }
        }

        ~Driver()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public class MediaDriverException : Exception
        {
            public MediaDriverException(string message)
                : base(message)
            {
            }
        }
    }
}