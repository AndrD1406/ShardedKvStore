using System.Diagnostics.CodeAnalysis;

namespace ShardedKvStore.Shared.Hashing;

/// <summary>
/// Classic consistent-hashing ring with virtual nodes.
/// <para>
/// Every physical node is projected onto a 32-bit ring <see cref="VirtualNodesPerNode"/> times
/// using the labels <c>"{nodeKey}#vnode-{i}"</c>. A key is owned by the first virtual node whose
/// hash is &gt;= hash(key), wrapping around to the first point of the ring.
/// </para>
/// <para>
/// Lookups are lock-free O(log V) binary searches over an immutable sorted array snapshot;
/// membership changes rebuild the snapshot under a lock (copy-on-write).
/// </para>
/// </summary>
public sealed class ConsistentHashRing<T> where T : notnull
{
    public const int DefaultVirtualNodes = 150;

    private readonly Func<T, string> _nodeKey;
    private readonly Func<string, uint> _hash;
    private readonly Lock _gate = new();
    private RingState _state = RingState.Empty;

    public ConsistentHashRing(
        int virtualNodesPerNode = DefaultVirtualNodes,
        Func<T, string>? nodeKey = null,
        Func<string, uint>? hashFunction = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(virtualNodesPerNode, 1);

        VirtualNodesPerNode = virtualNodesPerNode;
        _nodeKey = nodeKey ?? (node => node.ToString() ?? throw new ArgumentException("Node key cannot be null."));
        _hash = hashFunction ?? (value => MurmurHash3.Hash32(value));
    }

    public int VirtualNodesPerNode { get; }

    /// <summary>Number of physical nodes.</summary>
    public int Count => Volatile.Read(ref _state).Nodes.Count;

    /// <summary>Total number of points (virtual nodes) on the ring.</summary>
    public int VirtualNodeCount => Volatile.Read(ref _state).Hashes.Length;

    public IReadOnlyCollection<T> Nodes => [.. Volatile.Read(ref _state).Nodes.Values];

    public bool Contains(T node) => Volatile.Read(ref _state).Nodes.ContainsKey(_nodeKey(node));

    /// <summary>Adds a physical node with all its virtual nodes. Returns false if it is already present.</summary>
    public bool Add(T node)
    {
        string key = _nodeKey(node);
        lock (_gate)
        {
            RingState current = _state;
            if (current.Nodes.ContainsKey(key))
            {
                return false;
            }

            var nodes = new Dictionary<string, T>(current.Nodes, StringComparer.Ordinal) { [key] = node };
            Volatile.Write(ref _state, Build(nodes));
            return true;
        }
    }

    /// <summary>Removes a physical node and all its virtual nodes.</summary>
    public bool Remove(T node)
    {
        string key = _nodeKey(node);
        lock (_gate)
        {
            RingState current = _state;
            if (!current.Nodes.ContainsKey(key))
            {
                return false;
            }

            var nodes = new Dictionary<string, T>(current.Nodes, StringComparer.Ordinal);
            nodes.Remove(key);
            Volatile.Write(ref _state, Build(nodes));
            return true;
        }
    }

    /// <summary>Returns the node owning <paramref name="key"/>. Throws when the ring is empty.</summary>
    public T GetNode(string key) =>
        TryGetNode(key, out T? node)
            ? node
            : throw new InvalidOperationException("The hash ring is empty.");

    public bool TryGetNode(string key, [MaybeNullWhen(false)] out T node)
    {
        ArgumentNullException.ThrowIfNull(key);

        RingState state = Volatile.Read(ref _state);
        if (state.Hashes.Length == 0)
        {
            node = default;
            return false;
        }

        uint hash = _hash(key);
        int index = Array.BinarySearch(state.Hashes, hash);
        if (index < 0)
        {
            index = ~index; // first point with hash > key hash
        }

        if (index == state.Hashes.Length)
        {
            index = 0; // wrap around the ring
        }

        node = state.Owners[index];
        return true;
    }

    /// <summary>
    /// Fraction (0..1) of the 2^32 hash space owned by each physical node.
    /// Useful to show how evenly virtual nodes spread the load.
    /// </summary>
    public IReadOnlyDictionary<T, double> GetOwnershipShares()
    {
        RingState state = Volatile.Read(ref _state);
        var shares = new Dictionary<T, double>();
        if (state.Hashes.Length == 0)
        {
            return shares;
        }

        const double ringSize = 4294967296d; // 2^32
        for (int i = 0; i < state.Hashes.Length; i++)
        {
            uint previous = state.Hashes[i == 0 ? state.Hashes.Length - 1 : i - 1];
            ulong arc = unchecked(state.Hashes[i] - previous);
            if (state.Hashes.Length == 1)
            {
                arc = 1UL << 32;
            }

            T owner = state.Owners[i];
            shares[owner] = shares.GetValueOrDefault(owner) + (arc / ringSize);
        }

        return shares;
    }

    private RingState Build(Dictionary<string, T> nodes)
    {
        var points = new List<(uint Hash, string NodeKey, T Node)>(nodes.Count * VirtualNodesPerNode);
        foreach ((string nodeKey, T node) in nodes)
        {
            for (int i = 0; i < VirtualNodesPerNode; i++)
            {
                points.Add((_hash($"{nodeKey}#vnode-{i}"), nodeKey, node));
            }
        }

        // Deterministic order: by hash, then by node key (stable tie-break for hash collisions).
        points.Sort(static (a, b) =>
        {
            int byHash = a.Hash.CompareTo(b.Hash);
            return byHash != 0 ? byHash : string.CompareOrdinal(a.NodeKey, b.NodeKey);
        });

        var hashes = new List<uint>(points.Count);
        var owners = new List<T>(points.Count);
        foreach ((uint hash, _, T node) in points)
        {
            if (hashes.Count > 0 && hashes[^1] == hash)
            {
                continue; // collision: the first (ordinal-smallest) node keeps the point
            }

            hashes.Add(hash);
            owners.Add(node);
        }

        return new RingState([.. hashes], [.. owners], nodes);
    }

    private sealed record RingState(uint[] Hashes, T[] Owners, IReadOnlyDictionary<string, T> Nodes)
    {
        public static readonly RingState Empty = new([], [], new Dictionary<string, T>(StringComparer.Ordinal));
    }
}
