using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
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
    public delegate void AeronServerMessageReceivedHandler(long identity, ReadOnlySpan<byte> message);

    public class AeronServer : IDisposable
    {
        private const int _frameCountLimit = 16384;
        internal const int ServerStreamId = 1;

        private static readonly ILog _log = LogManager.GetLogger(typeof(AeronServer));
        private static readonly ILog _driverLog = LogManager.GetLogger(typeof(AeronDriver));
        
        private readonly ClientServerConfig _config;
        private readonly AeronDriver.DriverContext _driverContext;
        private readonly AeronDriver _driver;
        
        private readonly IIdleStrategy _publicationIdleStrategy;
        private readonly Subscription _subscription;

        
        private readonly ConcurrentDictionary<int, ClientSession> _clientSessions = new ConcurrentDictionary<int, ClientSession>();

        private volatile bool _isRunning;
        private volatile bool _isTerminating;
        private Thread? _pollThread;

        public AeronServer(int serverPort, ClientServerConfig config)
        {
            _config = config;
            _driverContext = config.ToDriverContext();
            _driverContext.Ctx
                .ErrorHandler(OnError)
                .AvailableImageHandler(ConnectionOnImageAvailable)
                .UnavailableImageHandler(ConnectionOnImageUnavailable);
            
            _driverContext
                .LoggerInfo(_driverLog.Info)
                .LoggerWarning(_driverLog.Warn)
                .LoggerWarning(_driverLog.Error);

            _driver = AeronDriver.Start(_driverContext);

            _publicationIdleStrategy = _config.ClientIdleStrategy.GetClientIdleStrategy();
            
            _subscription = _driver.AddSubscription($"aeron:udp?endpoint=0.0.0.0:{serverPort}", ServerStreamId);
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
        
        public event AeronServerMessageReceivedHandler? MessageReceived;

        public event Action<long>? ClientConnected;
        public event Action<long>? ClientDisconnected;

        public event Action? TerminatedUnexpectedly;

        public bool IsRunning => _isRunning;

        private static void ConnectionOnImageAvailable(Image image)
        {
            if (_log.IsDebugEnabled)
            {
                var subscription = image.Subscription;
                _log.Debug(
                    $"Available image on {subscription.Channel} streamId={subscription.StreamId:D} sessionId={image.SessionId:D} from {image.SourceIdentity}");
            }
        }

        private void ConnectionOnImageUnavailable(Image image)
        {
            var subscription = image.Subscription;

            var peer = GetSession(image);

            if (peer != null)
                DisconnectPeer(peer.Publication.SessionId);

            Debug.Assert(subscription.StreamId == ServerStreamId, "subscription.StreamId == ServerStreamId");

            if (_log.IsDebugEnabled)
                _log.Debug(
                    $"Unavailable image on {subscription.Channel} streamId={subscription.StreamId:D} sessionId={image.SessionId:D} from {image.SourceIdentity}");
        }

        private ClientSession? GetSession(Image image)
        {
            var peer = _clientSessions.SingleOrDefault(kvp => kvp.Value.Image == image);
            return peer.Value;
        }

        private bool TryAddSession(ClientSession session)
        {
            if (GetSession(session.Image) != null)
            {
                _log.Warn(
                    $"Tried to add a session with existing source identity [{session.Image.SourceIdentity}] and session id [{session.Image.SessionId}]");
                return false;
            }

            return _clientSessions.TryAdd(session.Publication.SessionId, session);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void DisconnectPeer(int publicationSessionId)
        {
            if (_clientSessions.TryRemove(publicationSessionId, out var session))
            {
                _log.Info($"Disconnected client: {session}");
                ClientDisconnected?.Invoke(session.ToIdentity());

                // re-entry is forbidden from callback
                Task.Run(session.Dispose);
            }
        }

        public void Start()
        {
            if (_isRunning)
                return;

            _pollThread = new Thread(PollThread)
            {
                IsBackground = true, Name = "AeronServer Poll Thread"
            };
            _pollThread.Start();

            _isRunning = true;
        }

        private void PollThread()
        {
            var idleStrategy = _config.ClientIdleStrategy.GetClientIdleStrategy();
            var fragmentHandler = new FragmentAssembler(HandlerHelper.ToFragmentHandler(SubscriptionHandler));

            while (_isRunning && !_isTerminating)
            {
                idleStrategy.Idle(_subscription.Poll(fragmentHandler, _frameCountLimit));
            }
        }

        private void SubscriptionHandler(IDirectBuffer buffer, int offset, int length, Header header)
        {
            if (!_isRunning)
                return;

            var reservedValue = (AeronReservedValue) header.ReservedValue;

            if (reservedValue.ProtocolVersion != Utils.CurrentProtocolVersion)
            {
                _log.Error(
                    $"Received message with unsupported protocol version: {reservedValue.ProtocolVersion} from {(header.Context as Image)?.SourceIdentity}, sessionId={header.SessionId}, ignoring");
                return;
            }

            if (reservedValue.MessageType == AeronMessageType.Data)
                DataHandler(buffer, offset, length, reservedValue.SessionId);
            else
                ConnectionHandler(buffer, offset, length, header);
        }

        private unsafe void ConnectionHandler(IDirectBuffer buffer, int offset, int length, Header header)
        {
            var reservedValue = (AeronReservedValue) header.ReservedValue;
            var messageType = reservedValue.MessageType;

            if (messageType == AeronMessageType.Connected)
            {
                if (!(header.Context is Image image))
                {
                    _log.Warn("Received connection without image, ignoring");
                    return;
                }

                var handshake = Serializer.DeserializeWithLengthPrefix<AeronHandshakeRequest>(
                    new UnmanagedMemoryStream((byte*) buffer.BufferPointer + offset, length), PrefixStyle.Base128);

                var publication = _driver.AddPublication(handshake.Channel, handshake.StreamId);
                var session = new ClientSession(this, publication, image);

                if (TryAddSession(session))
                {
                    _log.Info($"New client session: {session}");

                    // Do not block the polling thread waiting for the other side to connect
                    Task.Run(() => InitializeSession(session));
                }
                else
                {
                    _log.Warn($"Duplicate client session: {session}");
                    session.Dispose();
                }
            }
            else if (messageType == AeronMessageType.Disconnected)
            {
                DisconnectPeer(reservedValue.SessionId);
            }
        }

        [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
        private void InitializeSession(ClientSession session)
        {
            try
            {
                session.Buffer.Release();

                var spinWait = new SpinWait();
                var stopwatch = Stopwatch.StartNew();

                while (true)
                {
                    var errorCode = session.Publication.Offer(session.Buffer, 0, 0,
                        (buffer, offset, length) => (long) new AeronReservedValue(Utils.CurrentProtocolVersion,
                            AeronMessageType.Connected, session.Publication.SessionId));

                    if (errorCode == Publication.NOT_CONNECTED)
                    {
                        // This will happen as we just created the publication - we need to wait for Aeron to do its stuff

                        if (stopwatch.Elapsed > TimeSpan.FromSeconds(30))
                            throw new InvalidOperationException(
                                $"Timed out while waiting to send handshake to {session}");

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

                    throw new InvalidOperationException($"Failed to send handshake to {session}");
                }

                session.Buffer.Release();

                _log.Info($"Connected client: {session}");
                ClientConnected?.Invoke(session.ToIdentity());
            }
            catch (Exception ex)
            {
                _log.Error($"Session initialization failed for {session}: {ex}");
                _clientSessions.TryRemove(session.Publication.SessionId, out _);
                session.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void DataHandler(IDirectBuffer buffer, int offset, int length, int publicationSessionId)
        {
            if (_clientSessions.ContainsKey(publicationSessionId))
            {
                MessageReceived?.Invoke(publicationSessionId,
                    new ReadOnlySpan<byte>((byte*) buffer.BufferPointer + offset, length));
            }
            else
            {
                _log.Warn($"Received message from unknown peer. Publication SessionId: {publicationSessionId}");
            }
        }

        public void Send(int identity, ReadOnlySpan<byte> message)
        {
            var publicationSessionId = identity;

            if (_clientSessions.TryGetValue(publicationSessionId, out var session))
                session.Send(message);
        }

        private void ConnectionOnTerminatedUnexpectedly()
        {
            _isTerminating = true;

            try
            {
                foreach (var kvp in _clientSessions)
                {
                    var session = kvp.Value;
                    try
                    {
                        _log.Info($"Disconnected client due to unhandled error: {session}");
                        ClientDisconnected?.Invoke(session.ToIdentity());
                    }
                    catch (Exception ex)
                    {
                        _log.Error(ex);
                    }
                }
            }
            finally
            {
                TerminatedUnexpectedly?.Invoke();
                DisposeImpl(false);
            }
        }

        public void Dispose()
            => DisposeImpl(true);

        private void DisposeImpl(bool sendDisconnectMessage)
        {
            if (!_isRunning)
                return;

            _isRunning = false;

            foreach (var kvp in _clientSessions)
            {
                var session = kvp.Value;
                if (sendDisconnectMessage)
                {
                    while (true)
                    {
                        if (!_driver.IsDriverRunning)
                            break;

                        var result = Utils.InterpretPublicationOfferResult(
                            session.Publication.Offer(session.Buffer, 0, 0,
                                (buffer, offset, length) =>
                                    (long) new AeronReservedValue(Utils.CurrentProtocolVersion,
                                        AeronMessageType.Disconnected, session.Publication.SessionId)));

                        if (result == AeronResultType.ShouldRetry)
                        {
                            Thread.SpinWait(1);
                            continue;
                        }

                        break;
                    }
                }

                session.Dispose();
            }

            _clientSessions.Clear();

            if (_pollThread != null && !_pollThread.Join(TimeSpan.FromSeconds(30)))
                _log.Warn("Could not join poll thread");

            // linger for disconnect messages and pool thread
            Thread.Sleep(500);

            _subscription.Dispose();
            _driver.Dispose(); // last
        }

        private class ClientSession : IDisposable
        {
            private readonly AeronServer _server;
            private readonly ReservedValueSupplier _dataReservedValueSupplier;

            public Image Image { get; }
            public Publication Publication { get; }

            public UnsafeBuffer Buffer { get; } = new UnsafeBuffer();

            public ClientSession(AeronServer server, Publication publication, Image image)
            {
                _server = server;
                Image = image;
                Publication = publication;

                var dataReservedValue = (long) new AeronReservedValue(Utils.CurrentProtocolVersion,
                    AeronMessageType.Data, Publication.SessionId);
                _dataReservedValueSupplier = (buffer, offset, length) => dataReservedValue;
            }

            public void Dispose()
            {
                Publication.Dispose();
                Buffer.Dispose();
            }

            public int ToIdentity() => Publication.SessionId;

            public unsafe void Send(ReadOnlySpan<byte> message)
            {
                fixed (byte* ptr = message)
                {
                    Buffer.Wrap(ptr, message.Length);

                    if (Publication.Offer(Buffer, 0, message.Length, _dataReservedValueSupplier) < 0)
                    {
                        _server._publicationIdleStrategy.Reset();

                        while (true)
                        {
                            var result = Utils.InterpretPublicationOfferResult(
                                Publication.Offer(Buffer, 0, message.Length, _dataReservedValueSupplier));

                            if (result == AeronResultType.Success)
                                break;

                            if (result == AeronResultType.ShouldRetry)
                            {
                                _server._publicationIdleStrategy.Idle();
                                continue;
                            }

                            _server.DisconnectPeer(Publication.SessionId);
                            return;
                        }
                    }

                    Buffer.Release();
                }
            }

            public override string ToString() =>
                $"{Image.SourceIdentity}, Image: {Image.SessionId}, Publication: {Publication.SessionId}, StreamId: {Publication.StreamId}";
        }
    }
}