using System.Buffers.Binary;
using Google.Protobuf;

namespace GameServer.Shared.Protocol;

public static class ProtobufPacketCodec
{
    public const int HeaderSize = 4;
    public const int MaxPacketSize = 32 * 1024;

    public static async ValueTask WriteAsync(Stream stream, IMessage message, CancellationToken cancellationToken)
    {
        var payload = message.ToByteArray();

        if (payload.Length > MaxPacketSize)
        {
            throw new InvalidOperationException($"Packet is larger than {MaxPacketSize} bytes.");
        }

        Span<byte> header = stackalloc byte[HeaderSize];
        BinaryPrimitives.WriteInt32BigEndian(header, payload.Length);
        await stream.WriteAsync(header.ToArray(), cancellationToken);
        await stream.WriteAsync(payload, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    public static async ValueTask<T?> ReadAsync<T>(
        Stream stream,
        MessageParser<T> parser,
        CancellationToken cancellationToken)
        where T : class, IMessage<T>
    {
        var header = new byte[HeaderSize];
        var headerRead = await ReadExactOrEndAsync(stream, header, cancellationToken);
        if (!headerRead)
        {
            return default;
        }

        var length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length <= 0 || length > MaxPacketSize)
        {
            throw new InvalidDataException($"Invalid packet length: {length}.");
        }

        var payload = new byte[length];
        var payloadRead = await ReadExactOrEndAsync(stream, payload, cancellationToken);
        if (!payloadRead)
        {
            return default;
        }

        return parser.ParseFrom(payload);
    }

    private static async ValueTask<bool> ReadExactOrEndAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(offset, buffer.Length - offset), cancellationToken);
            if (read == 0)
            {
                return false;
            }

            offset += read;
        }

        return true;
    }
}
