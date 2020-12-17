using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Adaptive.Aeron;
using Adaptive.Aeron.Driver.Native;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using log4net;
using ProtoBuf;

namespace Abc.Aeron.ClientServer
{
    public delegate void AeronClientMessageReceivedHandler(ReadOnlySpan<byte> message);

    public class AeronClient : IDisposable
    {
        private static readonly ILog _log = LogManager.GetLogger(typeof(AeronClient));
        private static readonly ILog _driverLog = LogManager.GetLogger(typeof(AeronDriver));

        private static readonly object _clientsLock = new object();

        private static readonly Dictionary<string, AeronClient> _clients =
            new Dictionary<string, AeronClient>(RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                ? StringComparer.Ordinal
                : StringComparer.OrdinalIgnoreCase);

        private int _clientPort;
        private const int _frameCountLimit = 16384;
        private int _roundRobinIndex;
        private int _referenceCount;

        private volatile bool _isRunning;
        private bool _isTerminatedUnexpectedly;

        private readonly object _connectionsLock = new object();
        private AeronClientSession?[] _clientSessions;

        private readonly ClientServerConfig _config;
        private readonly AeronDriver _driver;
        
        private Thread? _receiveThread;

        public event Action? TerminatedUnexpectedly;

        private AeronClient(ClientServerConfig config)
        {
            _config = config;
            AeronDriver.DriverContext driverContext = config.ToDriverContext();
            driverContext.Ctx
                .ErrorHandler(OnError)
                .AvailableImageHandler(ConnectionOnImageAvailable)
                .UnavailableImageHandler(ConnectionOnImageUnavailable);
            
            driverContext
                .LoggerInfo(_driverLog.Info)
                .LoggerWarning(_driverLog.Warn)
                .LoggerWarning(_driverLog.Error);

            _driver = AeronDriver.Start(driverContext);
            
            const int sessionsLen =
#if DEBUG
                1;
#else
                64;
#endif

            _clientSessions = new AeronClientSession[sessionsLen];

            
        }

        private void OnError(Exception exception)
        {
            _driverLog.Error("Aeron connection error", exception);
        
            if (_driver.IsClosed)
                return;
        
            switch (exception)
            {
                case AeronException _:
                case AgentTerminationException _:
                    _log.Error("Unrecoverable Media Driver error");
                    ConnectionOnTerminatedUnexpectedly();
                    break;
            }
        }
        
        public int Connect(string serverHost, int serverPort, Action onConnectedDelegate, Action onDisconnectedDelegate,
            AeronClientMessageReceivedHandler onMessageReceived, int connectionResponseTimeoutMs)
        {
            if (serverHost == null)
                throw new ArgumentNullException(nameof(serverHost));

            if (_isTerminatedUnexpectedly)
                return -1;

            string serverHostIp;
            if (serverHost.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
                serverHost.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase))
            {
                // TODO IPC channel for local communication
                serverHostIp = "127.0.0.1";
            }
            else
            {
                var ipv4 =
                    Dns.GetHostEntry(serverHost).AddressList
                        .FirstOrDefault(x => x.AddressFamily == AddressFamily.InterNetwork) ??
                    throw new ArgumentException($"Cannot resolve serverHost ip for {serverHost}");
                serverHostIp = ipv4.ToString();
            }

            var serverChannel = Utils.RemoteChannel(serverHostIp, serverPort);
            var publication = _driver.AddExclusivePublication(serverChannel, AeronServer.ServerStreamId);

            var localIp = UdpUtils.GetLocalIPAddress(serverHostIp, serverPort);
            var streamId = MachineCounters.Instance.GetNewClientStreamId();
            GetChannelAndSubscription(localIp, streamId, out var clientChannel, out var subscription);

            var session = new AeronClientSession(this, serverChannel, publication, subscription, onConnectedDelegate,
                onDisconnectedDelegate, onMessageReceived);
            var connectionId = AddSession(session);

            _log.Info($"Connecting: {session}");

            var handshakeRequest = new AeronHandshakeRequest
            {
                Channel = clientChannel,
                StreamId = streamId
            };

            if (!session.SendHandshake(handshakeRequest, connectionResponseTimeoutMs))
            {
                RemoveSession(session);
                session.Dispose();
                return -1;
            }

            return connectionId;
        }

        private void GetChannelAndSubscription(string localIp, int streamId, out string clientChannel,
            out Subscription subscription)
        {
            // Finding port for the first time is slow and in tests with
            // parallel connections many clients have different ports.
            // Simple lock is needed.
            lock (_connectionsLock)
            {
                while (true)
                {
                    if (_clientPort == 0)
                    {
                        var clientPort = 0;
                        while (true)
                        {
                            try
                            {
                                clientPort = UdpUtils.GetRandomUnusedPort();
                                clientChannel = Utils.RemoteChannel(localIp, clientPort);
                                subscription = _driver.AddSubscription(clientChannel, streamId);
                                break;
                            }
                            catch (RegistrationException ex)
                            {
                                // if port was already reused by someone
                                _log.Warn($"Could not subscribe on new port {clientPort}", ex);
                            }
                        }

                        _clientPort = clientPort;
                        return;
                    }

                    try
                    {
                        clientChannel = Utils.RemoteChannel(localIp, _clientPort);
                        subscription = _driver.AddSubscription(clientChannel, streamId);
                        return;
                    }
                    catch (RegistrationException ex)
                    {
                        _log.Warn($"Could not subscribe on existing port {_clientPort}", ex);
                        _clientPort = 0;
                    }
                }
            }
        }

        private int AddSession(AeronClientSession session)
        {
            if (session == null)
                throw new ArgumentNullException(nameof(session));

            var connectionId = 0;

            lock (_connectionsLock)
            {
                for (var i = 0; i < _clientSessions.Length; i++)
                {
                    if (_clientSessions[i] == null)
                    {
                        connectionId = ConnectionIndexToId(i);
                        _clientSessions[i] = session;
                        break;
                    }
                }

                if (connectionId == 0)
                {
                    var newSessions = new AeronClientSession[_clientSessions.Length * 2];
                    _clientSessions.CopyTo(newSessions, 0);
                    connectionId = ConnectionIndexToId(_clientSessions.Length);
                    newSessions[_clientSessions.Length] = session;
                    _clientSessions = newSessions;
                }
            }

            return connectionId;
        }

        private AeronClientSession? RemoveSession(int connectionId)
        {
            lock (_connectionsLock)
            {
                var sessionIndex = ConnectionIdToIndex(connectionId);
                if ((uint) sessionIndex >= _clientSessions.Length)
                    throw new IndexOutOfRangeException(nameof(connectionId));

                var session = _clientSessions[sessionIndex];
                _clientSessions[sessionIndex] = null;
                return session;
            }
        }

        private void RemoveSession(AeronClientSession session)
        {
            lock (_connectionsLock)
            {
                for (var i = 0; i < _clientSessions.Length; i++)
                {
                    if (_clientSessions[i] == session)
                    {
                        _clientSessions[i] = null;
                        break;
                    }
                }
            }
        }

        public void Disconnect(int connectionId)
            => Disconnect(connectionId, true);

        private void Disconnect(int connectionId, bool sendNotification)
        {
            if (connectionId == 0)
                return;

            var session = RemoveSession(connectionId);
            if (session == null)
                return;

            _log.Info($"Disconnecting: {session}");

            if (sendNotification)
            {
                Debug.Assert(_receiveThread != Thread.CurrentThread,
                    "Notification is only sent from FeedClient.Stop method");
                // Notify server that we are disconnecting. There should be no ack from server.
                session.SendDisconnectNotification();
            }

            session.Dispose();
        }

        private void SessionDisconnected(AeronClientSession session)
        {
            RemoveSession(session);
            session.OnDisconnected.Invoke();
            session.Dispose();
        }

        private void ConnectionOnImageAvailable(Image image)
        {
            var subscription = image.Subscription;
            if (subscription == null)
                return;

            if (_log.IsDebugEnabled)
                _log.Debug(
                    $"Available image on {subscription.Channel} streamId={subscription.StreamId} sessionId={image.SessionId} from {image.SourceIdentity}");

            lock (_connectionsLock)
            {
                foreach (var session in _clientSessions)
                {
                    if (session?.Subscription == subscription)
                    {
                        session.SetSubscriptionImageAvailable();
                        break;
                    }
                }
            }
        }

        private void ConnectionOnImageUnavailable(Image image)
        {
            var subscription = image.Subscription;
            if (subscription == null)
                return;

            lock (_connectionsLock)
            {
                for (var i = 0; i < _clientSessions.Length; i++)
                {
                    var session = _clientSessions[i];

                    if (session?.Subscription == subscription)
                    {
                        // Do not send a disconnect notification, server is unavailable
                        _clientSessions[i] = null;
                        session.OnDisconnected.Invoke();
                        session.Dispose();
                        break;
                    }
                }
            }

            if (_log.IsDebugEnabled)
                _log.Debug(
                    $"Unavailable image on {subscription.Channel} streamId={subscription.StreamId} sessionId={image.SessionId} from {image.SourceIdentity}");
        }

        public void Start()
        {
            if (_isRunning)
                return;

            _isRunning = true;
            _receiveThread = new Thread(PollThread)
            {
                IsBackground = true,
                Name = "AeronClient Poll Thread"
            };

            _receiveThread.Start();
        }

        private void PollThread()
        {
            _log.Info($"AeronClient receive thread started with thread id: {Thread.CurrentThread.ManagedThreadId}");

            try
            {
                var idleStrategy = _config.ClientIdleStrategy.GetClientIdleStrategy();

                while (_isRunning)
                {
                    var count = Poll();
                    idleStrategy.Idle(count);
                }
            }
            catch (Exception ex)
            {
                _log.Error("Unhandled exception in receive thread", ex);
                throw;
            }
        }

        private int Poll()
        {
            // ReSharper disable once InconsistentlySynchronizedField : atomic reference swap on growth
            var clientSessions = _clientSessions;

            var length = clientSessions.Length;
            var count = 0;
            var startIndex = _roundRobinIndex;
            int index;
            if (startIndex >= length)
                _roundRobinIndex = startIndex = 0;

            var lastPolledClient = startIndex;

            for (index = startIndex; index < length && count < _frameCountLimit; ++index)
            {
                var session = clientSessions[index];
                lastPolledClient = index;

                if (session is null)
                    continue;

                count += session.Poll(_frameCountLimit - count);
            }

            for (index = 0; index < startIndex && count < _frameCountLimit; ++index)
            {
                var session = clientSessions[index];
                lastPolledClient = index;

                if (session is null)
                    continue;

                count += session.Poll(_frameCountLimit - count);
            }

            _roundRobinIndex = lastPolledClient + 1;

            return count;
        }

        public void Send(int connectionId, ReadOnlySpan<byte> message)
        {
            var session = GetSession(connectionId);
            session?.Send(message);
        }

        private AeronClientSession? GetSession(int connectionId)
        {
            if (connectionId <= 0)
                return null;

            // ReSharper disable once InconsistentlySynchronizedField : atomic reference swap on growth
            var clientSessions = _clientSessions;

            var connectionIndex = ConnectionIdToIndex(connectionId);
            if ((uint) connectionIndex >= clientSessions.Length)
                return null;

            return clientSessions[connectionIndex];
        }

        private void ConnectionOnTerminatedUnexpectedly()
        {
            _isTerminatedUnexpectedly = true;
            TerminatedUnexpectedly?.Invoke();
            Dispose();
        }

        public void DisposeReferenceCounted()
        {
            lock (_clientsLock)
            {
                if (--_referenceCount > 0)
                    return;

                Dispose();
            }
        }

        public void Dispose()
        {
            lock (_clientsLock)
            {
                if (!_isRunning)
                    return;

                _isRunning = false;

                if (_receiveThread != null)
                {
                    if (_receiveThread.Join(1000))
                        _log.Info("Exited receive thread");
                    else
                        _log.Warn("Timed out when joining receive thread");
                }

                try
                {
                    lock (_connectionsLock)
                    {
                        for (var i = 0; i < _clientSessions.Length; i++)
                        {
                            if (_clientSessions[i] == null)
                                continue;

                            _clientSessions[i]?.Dispose();
                            _clientSessions[i] = null;
                        }
                    }

                    _driver.Dispose(); // last
                }
                finally
                {
                    _clients.Remove(_config.Dir);
                }
            }
        }

        public static AeronClient GetClientReference(string path, Func<string, ClientServerConfig>? mdConfigFactory)
        {
            path = Path.GetFullPath(path);

            lock (_clientsLock)
            {
                if (!_clients.TryGetValue(path, out var client))
                {
                    client = new AeronClient(mdConfigFactory?.Invoke(path) ?? new ClientServerConfig(path));
                    _clients.Add(path, client);
                }

                client._referenceCount++;
                return client;
            }
        }

        internal void DisposeDriver() => _driver.Dispose();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ConnectionIndexToId(int connectionIndex)
            => connectionIndex + 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int ConnectionIdToIndex(int connectionId)
            => connectionId - 1;

        private class AeronClientSession : IDisposable
        {
            private readonly AeronClient _client;
            private readonly string _serverChannel;
            private readonly Publication _publication;
            private readonly UnsafeBuffer _buffer;
            public readonly Subscription Subscription;
            public readonly Action OnDisconnected;
            private readonly AeronClientMessageReceivedHandler _onMessageReceived;

            private readonly Action _onConnected;

            private bool _isConnected;
            private volatile bool _isSubscriptionImageAvailable;
            private bool _isDisposed;
            private readonly FragmentAssembler _fragmentAssembler;

            private int _serverAssignedSessionId;
            private ReservedValueSupplier? _dataReservedValueSupplier;

            public AeronClientSession(AeronClient client,
                string serverChannel,
                Publication publication,
                Subscription subscription,
                Action onConnected,
                Action onDisconnected,
                AeronClientMessageReceivedHandler onMessageReceived)
            {
                _client = client;
                _serverChannel = serverChannel;
                _publication = publication;
                _buffer = new UnsafeBuffer();
                Subscription = subscription;
                OnDisconnected = onDisconnected;
                _onMessageReceived = onMessageReceived;
                _onConnected = onConnected;
                _isConnected = false;
                _fragmentAssembler = new FragmentAssembler(HandlerHelper.ToFragmentHandler(SubscriptionHandler));
            }

            private bool IsConnected => _isConnected && _isSubscriptionImageAvailable && !_isDisposed;

            public void Dispose()
            {
                _isSubscriptionImageAvailable = false;

                if (_isDisposed)
                    return;

                _isDisposed = true;

                Task.Factory.StartNew(
                    async x =>
                    {
                        // need to sleep, otherwise fast restart of a client connection causes segfault (e.g. should_allow_to_stop_and_start_client test)
                        await Task.Delay(1000);

                        var session = (AeronClientSession) x!;

                        session._publication.Dispose();
                        session.Subscription.Dispose();
                        session._buffer.Dispose();
                    },
                    this
                );
            }

            public void SetSubscriptionImageAvailable()
            {
                _isSubscriptionImageAvailable = true;
            }

            private void SetConnected(int serverAssignedSessionId)
            {
                _serverAssignedSessionId = serverAssignedSessionId;

                var dataReservedValue = (long) new AeronReservedValue(Utils.CurrentProtocolVersion,
                    AeronMessageType.Data, serverAssignedSessionId);
                _dataReservedValueSupplier = (buffer, offset, length) => dataReservedValue;

                _isConnected = true;
                _onConnected.Invoke();
            }

            public unsafe void Send(ReadOnlySpan<byte> message)
            {
                if (!IsConnected)
                    throw new InvalidOperationException("Trying to send when not connected");

                fixed (byte* ptr = message)
                {
                    _buffer.Wrap(ptr, message.Length);

                    var spinWait = new SpinWait();

                    while (true)
                    {
                        var errorCode = _publication.Offer(_buffer, 0, message.Length, _dataReservedValueSupplier);
                        
                        if(errorCode >= 0)
                            break;
                        
                        var result = Utils.InterpretPublicationOfferResult(errorCode);

                        if (result == AeronResultType.Success)
                            break;

                        if (result == AeronResultType.ShouldRetry)
                        {
                            spinWait.SpinOnce();
                            continue;
                        }

                        _client.SessionDisconnected(this);
                        return;
                    }

                    _buffer.Release();
                }
            }

            public bool SendHandshake(AeronHandshakeRequest handshakeRequest, int connectionResponseTimeoutMs)
            {
                using var handshakeRequestStream = new MemoryStream();
                Serializer.SerializeWithLengthPrefix(handshakeRequestStream, handshakeRequest, PrefixStyle.Base128);
                _buffer.Wrap(handshakeRequestStream.GetBuffer(), 0, (int) handshakeRequestStream.Length);

                var spinWait = new SpinWait();
                var stopwatch = Stopwatch.StartNew();

                while (true)
                {
                    var errorCode = _publication.Offer(_buffer, 0, (int) handshakeRequestStream.Length,
                        (buffer, offset, length) => (long) new AeronReservedValue(Utils.CurrentProtocolVersion,
                            AeronMessageType.Connected, 0));

                    if (errorCode == Publication.NOT_CONNECTED)
                    {
                        // This will happen as we just created the publication - we need to wait for Aeron to do its stuff

                        if (stopwatch.ElapsedMilliseconds > connectionResponseTimeoutMs)
                        {
                            _log.Error($"Failed to send handshake (not connected): {this}");
                            return false;
                        }

                        spinWait.SpinOnce();
                        continue;
                    }

                    var result = Utils.InterpretPublicationOfferResult(errorCode);

                    if (result == AeronResultType.Success)
                        break;

                    if (result == AeronResultType.ShouldRetry)
                    {
                        spinWait.SpinOnce();
                        continue;
                    }

                    _log.Error($"Failed to send handshake: {this}");
                    return false;
                }

                _buffer.Release();
                return true;
            }

            public void SendDisconnectNotification()
            {
                while (true)
                {
                    var errorCode = _publication.Offer(_buffer, 0, 0,
                        (buffer, offset, length) => (long) new AeronReservedValue(Utils.CurrentProtocolVersion,
                            AeronMessageType.Disconnected, _serverAssignedSessionId));
                    var result = Utils.InterpretPublicationOfferResult(errorCode);

                    if (result == AeronResultType.ShouldRetry)
                    {
                        Thread.SpinWait(1);
                        continue;
                    }

                    break;
                }
            }

            public int Poll(int maxCount)
            {
                return _isSubscriptionImageAvailable ? Subscription.Poll(_fragmentAssembler, maxCount) : 0;
            }

            private unsafe void SubscriptionHandler(IDirectBuffer buffer, int offset, int length, Header header)
            {
                var reservedValue = (AeronReservedValue) header.ReservedValue;

                if (reservedValue.ProtocolVersion != Utils.CurrentProtocolVersion)
                {
                    _log.Error(
                        $"Received message with unsupported protocol version: {reservedValue.ProtocolVersion} from {this}, ignoring");
                    return;
                }

                switch (reservedValue.MessageType)
                {
                    case AeronMessageType.Data:
                        _onMessageReceived(new ReadOnlySpan<byte>((byte*) buffer.BufferPointer + offset, length));
                        break;

                    case AeronMessageType.Connected:
                        // set server-side session for subsequent use in Send
                        SetConnected(reservedValue.SessionId);
                        break;

                    case AeronMessageType.Disconnected:
                        // graceful disconnect notification from the server
                        _client.SessionDisconnected(this);
                        break;
                }

                // ignore other reserved values
            }

            public override string ToString() =>
                $"{_serverChannel}, Publication: {_publication.SessionId}, Subscription StreamId: {Subscription.StreamId}";
        }
    }
}