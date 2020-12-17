using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Abc.Aeron.ClientServer
{
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 8)]
    [SuppressMessage("ReSharper", "PrivateFieldCanBeConvertedToLocalVariable")]
    internal readonly struct AeronReservedValue
    {
        public readonly byte ProtocolVersion;
        public readonly AeronMessageType MessageType;
        private readonly short _reservedShort;
        public readonly int SessionId;

        public AeronReservedValue(byte protocolVersion, AeronMessageType messageType, int sessionId)
        {
            ProtocolVersion = protocolVersion;
            MessageType = messageType;
            _reservedShort = default;
            SessionId = sessionId;
        }

        public static unsafe explicit operator long(AeronReservedValue value)
        {
            return Unsafe.ReadUnaligned<long>(&value);
        }

        public static unsafe explicit operator AeronReservedValue(long value)
        {
            return Unsafe.ReadUnaligned<AeronReservedValue>(&value);
        }
    }
}
