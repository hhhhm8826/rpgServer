using System.Buffers;
using System.Buffers.Binary;
using Google.Protobuf;

namespace GameServer.Shared.Protocol;

public static class ProtobufPacketCodec
{
    public const int HeaderSize = 4;
    public const int MaxPacketSize = 32 * 1024;

    public static async ValueTask WriteAsync(Stream stream, IMessage message, CancellationToken cancellationToken)
    {
        var payloadLength = message.CalculateSize();

        if (payloadLength > MaxPacketSize)
        {
            throw new InvalidOperationException($"Packet is larger than {MaxPacketSize} bytes.");
        }

        // 매번 byte[]를 만들지 않고 pool에서 빌려 GC 압박을 줄일 수 있음.
        var header = ArrayPool<byte>.Shared.Rent(HeaderSize);
        var payload = ArrayPool<byte>.Shared.Rent(payloadLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(0, HeaderSize), payloadLength);

            var output = new CodedOutputStream(payload);
            message.WriteTo(output);
            // ArrayPool은 요청보다 큰 배열을 줄 수 있으므로 실제 직렬화 길이만 검증
            if (output.Position != payloadLength)
            {
                throw new InvalidOperationException(
                    $"Packet serialization size mismatch. Expected {payloadLength}, wrote {output.Position}.");
            }

            await stream.WriteAsync(header.AsMemory(0, HeaderSize), cancellationToken);
            await stream.WriteAsync(payload.AsMemory(0, payloadLength), cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload);
            ArrayPool<byte>.Shared.Return(header);
        }
    }

    public static async ValueTask<T?> ReadAsync<T>(
        Stream stream,
        MessageParser<T> parser,
        CancellationToken cancellationToken)
        where T : class, IMessage<T>
    {
        // 수신도 header/payload 배열을 재사용해서 대량 패킷 처리 시 할당을 줄임
        var header = ArrayPool<byte>.Shared.Rent(HeaderSize);
        try
        {
            var headerRead = await ReadExactOrEndAsync(stream, header.AsMemory(0, HeaderSize), cancellationToken);
            if (!headerRead)
            {
                return default;
            }

            var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(0, HeaderSize));
            if (length <= 0 || length > MaxPacketSize)
            {
                throw new InvalidDataException($"Invalid packet length: {length}.");
            }

            var payload = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                var payloadRead = await ReadExactOrEndAsync(stream, payload.AsMemory(0, length), cancellationToken);
                if (!payloadRead)
                {
                    return default;
                }

                return parser.ParseFrom(payload, 0, length);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(payload);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
        }
    }

    private static async ValueTask<bool> ReadExactOrEndAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer[offset..], cancellationToken);
            if (read == 0)
            {
                return false;
            }

            offset += read;
        }

        return true;
    }
}
