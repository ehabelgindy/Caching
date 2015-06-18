// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Framework.Caching.Distributed;
using Microsoft.Framework.Logging;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class SqlOperations : ISqlOperations
    {
        private readonly ILogger _logger;
        private readonly SqlServerCacheOptions _options;

        public SqlOperations(SqlServerCacheOptions options, ILoggerFactory loggerFactory)
        {
            _options = options;
            _logger = loggerFactory.CreateLogger<SqlOperations>();
            SqlQueries = new SqlQueries(options.SchemaName, options.TableName);
        }

        protected SqlQueries SqlQueries { get; }

        public void DeleteCacheItem(string key)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddCacheItemId(key);

                connection.Open();

                command.ExecuteNonQuery();
            }
        }

        public async Task DeleteCacheItemAsync(string key)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.DeleteCacheItem, connection);
                command.Parameters.AddCacheItemId(key);

                await connection.OpenAsync();

                await command.ExecuteNonQueryAsync();
            }
        }

        public virtual byte[] GetCacheItem(string key)
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            byte[] value = null;
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetCacheItem, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                connection.Open();

                var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);

                TimeSpan? slidingExpiration = null;
                DateTime? absoluteExpirationUTC = null;
                DateTime expirationTimeUTC;
                if (reader.Read())
                {
                    var id = reader.GetFieldValue<string>(Columns.Indexes.CacheItemIdIndex);
                    value = reader.GetFieldValue<byte[]>(Columns.Indexes.CacheItemValueIndex);
                    expirationTimeUTC = reader.GetFieldValue<DateTime>(Columns.Indexes.ExpiresAtTimeUTCIndex);

                    if (!reader.IsDBNull(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetFieldValue<long>(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    if (!reader.IsDBNull(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = reader.GetFieldValue<DateTime>(
                            Columns.Indexes.AbsoluteExpirationUTCIndex);
                    }
                }
                else
                {
                    return null;
                }

                var newExpirationTimeUTC = CacheItemExpiration.GetNewExpirationTime(
                    utcNowDateTime, expirationTimeUTC, slidingExpiration, absoluteExpirationUTC);
                if (newExpirationTimeUTC.HasValue)
                {
                    UpdateCacheItemExpiration(key, newExpirationTimeUTC.Value);
                }
            }

            return value;
        }

        public virtual async Task<byte[]> GetCacheItemAsync(string key)
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            byte[] value = null;
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.GetCacheItem, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

                TimeSpan? slidingExpiration = null;
                DateTime? absoluteExpirationUTC = null;
                DateTime expirationTimeUTC;
                if (await reader.ReadAsync())
                {
                    var id = await reader.GetFieldValueAsync<string>(Columns.Indexes.CacheItemIdIndex);
                    value = await reader.GetFieldValueAsync<byte[]>(Columns.Indexes.CacheItemValueIndex);
                    expirationTimeUTC = await reader.GetFieldValueAsync<DateTime>(
                        Columns.Indexes.ExpiresAtTimeUTCIndex);

                    if (!await reader.IsDBNullAsync(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            await reader.GetFieldValueAsync<long>(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    if (!await reader.IsDBNullAsync(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = await reader.GetFieldValueAsync<DateTime>(
                            Columns.Indexes.AbsoluteExpirationUTCIndex);
                    }
                }
                else
                {
                    return null;
                }

                var newExpirationTimeUTC = CacheItemExpiration.GetNewExpirationTime(
                    utcNowDateTime, expirationTimeUTC, slidingExpiration, absoluteExpirationUTC);
                if (newExpirationTimeUTC.HasValue)
                {
                    await UpdateCacheItemExpirationAsync(key, newExpirationTimeUTC.Value);
                }
            }

            return value;
        }

        public virtual void GetTableSchema()
        {
            // Try connecting to the database and check if its available.
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.TableInfo, connection);
                connection.Open();

                var reader = command.ExecuteReader(CommandBehavior.SingleRow);
                if (!reader.Read())
                {
                    throw new InvalidOperationException(
                        $"Could not retrieve information of table with schema '{_options.SchemaName}' and " +
                        $"name '{_options.TableName}'. Make sure you have the table setup and try again. " +
                        $"Connection string: {_options.ConnectionString}");
                }
            }
        }

        public virtual async Task GetTableSchemaAsync()
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var command = new SqlCommand(SqlQueries.TableInfo, connection);
                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                if (!await reader.ReadAsync())
                {
                    throw new InvalidOperationException(
                        $"Could not retrieve information of table with schema '{_options.SchemaName}' and " +
                        $"name '{_options.TableName}'. Make sure you have the table setup and try again. " +
                        $"Connection string: {_options.ConnectionString}");
                }
            }
        }

        public void DeleteExpiredCacheItems()
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            var connection = new SqlConnection(_options.ConnectionString);
            var command = new SqlCommand(SqlQueries.DeleteExpiredCacheItems, connection);
            command.Parameters.AddWithValue("UtcNow", SqlDbType.DateTime, utcNowDateTime);

            try
            {
                connection.Open();

                var effectedRowCount = command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                _logger.LogError("An error occurred while deleting expired cache items.", ex);
            }
            finally
            {
                connection.Close();
            }
        }

        public virtual void SetCacheItem(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            var expirationInfo = CacheItemExpiration.GetExpirationInfo(utcNowDateTime, options);

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var upsertCommand = new SqlCommand(SqlQueries.SetCacheItem, connection);
                upsertCommand.Parameters
                    .AddCacheItemId(key)
                    .AddCacheItemValue(value)
                    .AddExpiresAtTime(expirationInfo.ExpiresAtTimeUTC)
                    .AddSlidingExpirationInTicks(expirationInfo.SlidingExpiration)
                    .AddAbsoluteExpiration(expirationInfo.AbsoluteExpirationUTC);

                connection.Open();

                upsertCommand.ExecuteNonQuery();
            }
        }

        public virtual async Task SetCacheItemAsync(string key, byte[] value, DistributedCacheEntryOptions options)
        {
            var utcNowDateTime = _options.SystemClock.UtcNow.UtcDateTime;

            var expirationInfo = CacheItemExpiration.GetExpirationInfo(utcNowDateTime, options);

            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                var upsertCommand = new SqlCommand(SqlQueries.SetCacheItem, connection);
                upsertCommand.Parameters
                    .AddCacheItemId(key)
                    .AddCacheItemValue(value)
                    .AddExpiresAtTime(expirationInfo.ExpiresAtTimeUTC)
                    .AddSlidingExpirationInTicks(expirationInfo.SlidingExpiration)
                    .AddAbsoluteExpiration(expirationInfo.AbsoluteExpirationUTC);

                await connection.OpenAsync();

                await upsertCommand.ExecuteNonQueryAsync();
            }
        }

        public virtual void UpdateCacheItemExpiration(string key, DateTime newExpirationTimeUTC)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                connection.Open();

                var command = new SqlCommand(SqlQueries.UpdateCacheItemExpiration, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddExpiresAtTime(newExpirationTimeUTC);

                command.ExecuteNonQuery();
            }
        }

        public virtual async Task UpdateCacheItemExpirationAsync(string key, DateTime newExpirationTimeUTC)
        {
            using (var connection = new SqlConnection(_options.ConnectionString))
            {
                await connection.OpenAsync();

                var command = new SqlCommand(SqlQueries.UpdateCacheItemExpiration, connection);
                command.Parameters
                    .AddCacheItemId(key)
                    .AddExpiresAtTime(newExpirationTimeUTC);

                await command.ExecuteNonQueryAsync();
            }
        }
    }
}