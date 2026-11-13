using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using ShardedKvStore.Shared.Contracts;
using ShardedKvStore.Shared.Storage;

namespace ShardedKvStore.Coordinator.Endpoints;

/// <summary>Validation of table names and extraction of key attributes from item bodies.</summary>
internal static partial class ItemKeys
{
    public const int MaxKeyBytes = 1024;

    [GeneratedRegex("^[A-Za-z0-9_.-]{3,255}$")]
    public static partial Regex TableNamePattern();

    [GeneratedRegex("^[A-Za-z0-9_.-]{1,255}$")]
    public static partial Regex AttributeNamePattern();

    /// <summary>
    /// Reads the partition key (and sort key if the table defines one) from an item.
    /// Key attributes must be non-empty strings or numbers.
    /// </summary>
    public static bool TryExtract(
        JsonElement item,
        TableDefinition table,
        [NotNullWhen(true)] out string? partitionKey,
        [NotNullWhen(true)] out string? sortKey,
        [NotNullWhen(false)] out string? error)
    {
        partitionKey = null;
        sortKey = null;

        if (item.ValueKind != JsonValueKind.Object)
        {
            error = "The request body must be a JSON object.";
            return false;
        }

        if (!TryReadAttribute(item, table.PartitionKey, out partitionKey, out error))
        {
            return false;
        }

        if (table.SortKey is null)
        {
            sortKey = PartitionedItemStore.NoSortKey;
            return true;
        }

        return TryReadAttribute(item, table.SortKey, out sortKey, out error);
    }

    /// <summary>Validates a key value received in the URL / query string.</summary>
    public static string? ValidateKeyValue(string name, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return $"'{name}' must not be empty.";
        }

        if (Encoding.UTF8.GetByteCount(value) > MaxKeyBytes)
        {
            return $"'{name}' must not exceed {MaxKeyBytes} bytes.";
        }

        return null;
    }

    private static bool TryReadAttribute(
        JsonElement item,
        string attribute,
        [NotNullWhen(true)] out string? value,
        [NotNullWhen(false)] out string? error)
    {
        value = null;
        if (!item.TryGetProperty(attribute, out JsonElement property))
        {
            error = $"Key attribute '{attribute}' is missing from the item.";
            return false;
        }

        string? raw = property.ValueKind switch
        {
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Number => property.GetRawText(),
            _ => null,
        };

        if (raw is null)
        {
            error = $"Key attribute '{attribute}' must be a string or a number, got {property.ValueKind}.";
            return false;
        }

        if (raw.Contains('/'))
        {
            error = $"Key attribute '{attribute}' must not contain '/' (it is used in resource URLs).";
            return false;
        }

        error = ValidateKeyValue(attribute, raw);
        if (error is not null)
        {
            return false;
        }

        value = raw;
        return true;
    }
}
