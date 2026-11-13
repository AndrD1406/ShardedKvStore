namespace ShardedKvStore.Shared.Hashing;

public static class ShardRouting
{
    /// <summary>
    /// The key used to pick a shard. It deliberately ignores the sort key, so every item of a
    /// partition (<c>tableName + partitionKey</c>) lands on the same shard and can be range-queried there.
    /// </summary>
    public static string RoutingKey(string tableName, string partitionKey) => $"{tableName}:{partitionKey}";
}
