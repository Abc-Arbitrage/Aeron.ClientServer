using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Adaptive.Agrona;
using Adaptive.Agrona.Util;

namespace Abc.Aeron.ClientServer
{
    /// <summary>
    /// Persistent counters per machine (as opposed to Aeron's built-in per driver counters)
    /// </summary>
    public class MachineCounters : CriticalFinalizerObject, IDisposable
    {
        public static readonly MachineCounters Instance = new MachineCounters();

        private const string ClientCountersFileName = "client.counters";

        /// <summary>
        /// Offset (in bytes) of client stream id counter.
        /// </summary>
        internal const int ClientStreamIdCounterOffset = 0;

        private MappedByteBuffer _clientCounters;

        private MachineCounters()
        {
            // Client stream id should be unique per machine, not per media driver.
            // Temp is ok, we only need to avoid same stream id among concurrently
            // running clients (within 10 seconds), not forever. If a machine
            // restarts and cleans temp there are no other running client and
            // we could start from 0 again.

            var dirPath =
                Path.Combine(Path.GetTempPath(), "aeron_client_counters"); // any name, but do not start with 'aeron-'
            Directory.CreateDirectory(dirPath);
            var counterFile = Path.Combine(dirPath, ClientCountersFileName);
            _clientCounters = IoUtil.MapNewOrExistingFile(new FileInfo(counterFile), 4096);
        }

        public unsafe int GetNewClientStreamId()
        {
            var id = Interlocked.Increment(
                ref Unsafe.AsRef<int>((void*) IntPtr.Add(_clientCounters.Pointer, ClientStreamIdCounterOffset))
            );
            _clientCounters.Flush();
            return id;
        }

        private void DoDispose()
        {
            var counters = Interlocked.Exchange(ref _clientCounters, null!);
            if (counters == null!)
                return;
            counters.Dispose();
        }

        public void Dispose()
        {
            DoDispose();
            GC.SuppressFinalize(this);
        }

        ~MachineCounters() => DoDispose();
    }
}