using ProtoBuf;

namespace Aeron.MediaDriver
{
    [ProtoContract]
    internal class AeronHandshakeRequest
    {
        [ProtoMember(1, IsRequired = false)]
        public string Channel { get; set; } = default!;

        [ProtoMember(2, IsRequired = false)]
        public int StreamId { get; set; }
    }
}
