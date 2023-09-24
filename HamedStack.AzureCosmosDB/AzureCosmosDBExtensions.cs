// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedType.Global
// ReSharper disable InconsistentNaming
// ReSharper disable UnusedMember.Global

using System.Runtime.CompilerServices;
using Microsoft.Azure.Cosmos;

namespace HamedStack.AzureCosmosDB;

/// <summary>
/// Provides extension methods for working with Azure Cosmos DB.
/// </summary>
public static class AzureCosmosDBExtensions
{
    /// <summary>
    /// Asynchronously enumerates the results of a feed iterator.
    /// </summary>
    /// <typeparam name="T">The type of items to enumerate.</typeparam>
    /// <param name="iterator">The feed iterator to enumerate.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>An asynchronous enumerable of items.</returns>
    public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this FeedIterator<T> iterator,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync(cancellationToken);
            foreach (var item in page)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }
    }

    /// <summary>
    /// Asynchronously creates items in a Cosmos DB container in bulk and returns any errors that occurred.
    /// </summary>
    /// <typeparam name="T">The type of items to create.</typeparam>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="data">The collection of items to create.</param>
    /// <param name="partitionKey">The partition key for the items.</param>
    /// <param name="itemRequestOptions">Options for creating items.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A collection of error messages for items that could not be created.</returns>
    public static async Task<IEnumerable<string>> BulkCreateAsync<T>(this Container container, IEnumerable<T> data,
        PartitionKey? partitionKey = null, ItemRequestOptions? itemRequestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task>();
        var errors = new List<string>();
        foreach (var item in data)
        {
            tasks.Add(container.CreateItemAsync(item, partitionKey, itemRequestOptions, cancellationToken)
                .ContinueWith(itemResponse =>
                {
                    if (!itemResponse.IsCompletedSuccessfully)
                    {
                        var innerExceptions = itemResponse.Exception?.Flatten();
                        if (innerExceptions?.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException)
                            is CosmosException cosmosException)
                        {
                            errors.Add($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                        }
                        else
                        {
                            if (innerExceptions != null)
                                errors.Add($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                        }
                    }
                }, cancellationToken));
        }

        await Task.WhenAll(tasks);
        return errors;
    }

    /// <summary>
    /// Asynchronously upserts (insert or update) items in a Cosmos DB container in bulk and returns any errors that occurred.
    /// </summary>
    /// <typeparam name="T">The type of items to upsert.</typeparam>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="data">The collection of items to upsert.</param>
    /// <param name="partitionKey">The partition key for the items.</param>
    /// <param name="itemRequestOptions">Options for upserting items.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A collection of error messages for items that could not be upserted.</returns>
    public static async Task<IEnumerable<string>> BulkUpsertAsync<T>(this Container container, IEnumerable<T> data,
        PartitionKey? partitionKey = null, ItemRequestOptions? itemRequestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task>();
        var errors = new List<string>();
        foreach (var item in data)
        {
            tasks.Add(container.UpsertItemAsync(item, partitionKey, itemRequestOptions, cancellationToken)
                .ContinueWith(itemResponse =>
                {
                    if (!itemResponse.IsCompletedSuccessfully)
                    {
                        var innerExceptions = itemResponse.Exception?.Flatten();
                        if (innerExceptions?.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException)
                            is CosmosException cosmosException)
                        {
                            errors.Add($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                        }
                        else
                        {
                            if (innerExceptions != null)
                                errors.Add($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                        }
                    }
                }, cancellationToken));
        }

        await Task.WhenAll(tasks);
        return errors;
    }

    /// <summary>
    /// Deletes multiple items from a Cosmos DB container in bulk.
    /// </summary>
    /// <typeparam name="T">The type of items in the container.</typeparam>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="itemIds">The list of item IDs to delete.</param>
    /// <param name="partitionKey">The partition key for the items.</param>
    /// <param name="itemRequestOptions">Optional request options for deleting items.</param>
    /// <param name="cancellationToken">Optional cancellation token for asynchronous operation.</param>
    /// <returns>A collection of error messages for items that could not be deleted.</returns>
    public static async Task<IEnumerable<string>> BulkDeleteAsync<T>(this Container container,
        IEnumerable<string> itemIds,
        PartitionKey partitionKey, ItemRequestOptions? itemRequestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task>();
        var errors = new List<string>();
        foreach (var itemId in itemIds)
        {
            tasks.Add(container.DeleteItemAsync<T>(itemId, partitionKey, itemRequestOptions, cancellationToken)
                .ContinueWith(itemResponse =>
                {
                    if (!itemResponse.IsCompletedSuccessfully)
                    {
                        var innerExceptions = itemResponse.Exception?.Flatten();
                        if (innerExceptions?.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException)
                            is CosmosException cosmosException)
                        {
                            errors.Add($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                        }
                        else
                        {
                            if (innerExceptions != null)
                                errors.Add($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                        }
                    }
                }, cancellationToken));
        }

        await Task.WhenAll(tasks);
        return errors;
    }

    /// <summary>
    /// Replaces multiple items in a Cosmos DB container in bulk.
    /// </summary>
    /// <typeparam name="T">The type of items in the container.</typeparam>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="data">The list of items to replace.</param>
    /// <param name="id">The common ID to use for replacement.</param>
    /// <param name="partitionKey">Optional partition key for the items.</param>
    /// <param name="itemRequestOptions">Optional request options for replacing items.</param>
    /// <param name="cancellationToken">Optional cancellation token for asynchronous operation.</param>
    /// <returns>A collection of error messages for items that could not be replaced.</returns>
    public static async Task<IEnumerable<string>> BulkReplaceAsync<T>(this Container container, IEnumerable<T> data,
        string id,
        PartitionKey? partitionKey = null, ItemRequestOptions? itemRequestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task>();
        var errors = new List<string>();
        foreach (var item in data)
        {
            tasks.Add(container.ReplaceItemAsync(item, id, partitionKey, itemRequestOptions, cancellationToken)
                .ContinueWith(itemResponse =>
                {
                    if (!itemResponse.IsCompletedSuccessfully)
                    {
                        var innerExceptions = itemResponse.Exception?.Flatten();
                        if (innerExceptions?.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException)
                            is CosmosException cosmosException)
                        {
                            errors.Add($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                        }
                        else
                        {
                            if (innerExceptions != null)
                                errors.Add($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                        }
                    }
                }, cancellationToken));
        }

        await Task.WhenAll(tasks);
        return errors;
    }

    /// <summary>
    /// Reads multiple items from a Cosmos DB container in bulk.
    /// </summary>
    /// <typeparam name="T">The type of items in the container.</typeparam>
    /// <param name="container">The Cosmos DB container.</param>
    /// <param name="itemIds">The list of item IDs to read.</param>
    /// <param name="partitionKey">The partition key for the items.</param>
    /// <param name="itemRequestOptions">Optional request options for reading items.</param>
    /// <param name="cancellationToken">Optional cancellation token for asynchronous operation.</param>
    /// <returns>An array of ItemResponse&amp;ltT&amp;gt representing the read items.</returns>
    public static async Task<ItemResponse<T>[]> BulkReadAsync<T>(this Container container, IEnumerable<string> itemIds,
        PartitionKey partitionKey, ItemRequestOptions? itemRequestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var tasks = new List<Task<ItemResponse<T>>>();
        foreach (var itemId in itemIds)
        {
            var result = container.ReadItemAsync<T>(itemId, partitionKey, itemRequestOptions, cancellationToken);
            if (result != null)
                tasks.Add(result);
        }

        return await Task.WhenAll(tasks);
    }


    /// <summary>
    /// Executes a query with parameters and filters the results.
    /// </summary>
    /// <typeparam name="T">The type of items to query.</typeparam>
    /// <param name="container">The Cosmos DB container to query.</param>
    /// <param name="query">The query string.</param>
    /// <param name="parameters">A dictionary of query parameters.</param>
    /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
    /// <returns>A collection of items matching the query and filter.</returns>
    public static async Task<IEnumerable<T>> QueryAndFilterAsync<T>(
        this Container container, string query, Dictionary<string, object>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        var results = new List<T>();
        try
        {
            var queryDefinition = new QueryDefinition(query);

            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    queryDefinition.WithParameter(parameter.Key, parameter.Value);
                }
            }

            var queryIterator = container.GetItemQueryIterator<T>(queryDefinition);

            while (queryIterator.HasMoreResults)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException("Query and filter operation was canceled.");
                }

                var response = await queryIterator.ReadNextAsync(cancellationToken);
                results.AddRange(response);
            }
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error executing query and filter: {ex.Message}", ex);
        }

        return results;
    }

    /// <summary>
    /// Checks if a container exists in a Cosmos DB database.
    /// </summary>
    /// <param name="cosmosClient">The Cosmos DB client.</param>
    /// <param name="databaseName">The name of the database containing the container.</param>
    /// <param name="containerName">The name of the container to check for existence.</param>
    /// <returns>True if the container exists, otherwise false.</returns>
    public static async Task<bool> ContainerExistsAsync(this CosmosClient cosmosClient, string databaseName,
        string containerName)
    {
        var databaseExists = await cosmosClient.DatabaseExistsAsync(databaseName);
        if (!databaseExists)
        {
            return false;
        }

        var containerNames = new List<string>();
        var database = cosmosClient.GetDatabase(databaseName);
        using (var iterator = database.GetContainerQueryIterator<ContainerProperties>())
        {
            while (iterator.HasMoreResults)
            {
                foreach (var containerProperties in await iterator.ReadNextAsync())
                {
                    containerNames.Add(containerProperties.Id);
                }
            }
        }

        return containerNames.Contains(containerName);
    }

    /// <summary>
    /// Checks if a database exists in a Cosmos DB account.
    /// </summary>
    /// <param name="cosmosClient">The Cosmos DB client.</param>
    /// <param name="databaseName">The name of the database to check for existence.</param>
    /// <returns>True if the database exists, otherwise false.</returns>
    public static async Task<bool> DatabaseExistsAsync(this CosmosClient cosmosClient, string databaseName)
    {
        var databaseNames = new List<string>();
        using (var iterator = cosmosClient.GetDatabaseQueryIterator<DatabaseProperties>())
        {
            while (iterator.HasMoreResults)
            {
                foreach (var databaseProperties in await iterator.ReadNextAsync())
                {
                    databaseNames.Add(databaseProperties.Id);
                }
            }
        }

        return databaseNames.Contains(databaseName);
    }
}