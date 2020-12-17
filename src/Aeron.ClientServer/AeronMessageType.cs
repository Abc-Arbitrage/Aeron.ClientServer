namespace Abc.Aeron.ClientServer
{
    /// <summary>
    /// Reserved field tag
    /// </summary>
    internal enum AeronMessageType : byte
    {
        Data = 0,
        Connected = 1,
        Disconnected = 2
    }
}
