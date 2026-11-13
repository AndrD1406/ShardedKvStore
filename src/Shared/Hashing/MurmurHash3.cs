using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace ShardedKvStore.Shared.Hashing;

/// <summary>
/// MurmurHash3 x86 32-bit (Austin Appleby, public domain). Fast, non-cryptographic,
/// with good avalanche behaviour — a solid choice for placing points on a hash ring.
/// </summary>
public static class MurmurHash3
{
    private const uint C1 = 0xcc9e2d51;
    private const uint C2 = 0x1b873593;
    private const int StackAllocThreshold = 256;

    public static uint Hash32(string value, uint seed = 0)
    {
        ArgumentNullException.ThrowIfNull(value);

        int maxBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxBytes <= StackAllocThreshold
            ? stackalloc byte[StackAllocThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));

        try
        {
            int length = Encoding.UTF8.GetBytes(value, buffer);
            return Hash32(buffer[..length], seed);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    public static uint Hash32(ReadOnlySpan<byte> data, uint seed = 0)
    {
        uint h1 = seed;
        int blockCount = data.Length / 4;

        for (int i = 0; i < blockCount; i++)
        {
            uint k1 = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i * 4, 4));
            k1 *= C1;
            k1 = BitOperations.RotateLeft(k1, 15);
            k1 *= C2;

            h1 ^= k1;
            h1 = BitOperations.RotateLeft(h1, 13);
            h1 = (h1 * 5) + 0xe6546b64;
        }

        ReadOnlySpan<byte> tail = data[(blockCount * 4)..];
        uint k = 0;
        switch (tail.Length)
        {
            case 3:
                k ^= (uint)tail[2] << 16;
                goto case 2;
            case 2:
                k ^= (uint)tail[1] << 8;
                goto case 1;
            case 1:
                k ^= tail[0];
                k *= C1;
                k = BitOperations.RotateLeft(k, 15);
                k *= C2;
                h1 ^= k;
                break;
        }

        h1 ^= (uint)data.Length;
        return FinalMix(h1);
    }

    private static uint FinalMix(uint h)
    {
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }
}
