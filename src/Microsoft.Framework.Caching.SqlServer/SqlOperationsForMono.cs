// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Framework.Logging;

namespace Microsoft.Framework.Caching.SqlServer
{
    internal class SqlOperationsForMono : SqlOperations
    {
        private readonly ILogger _logger;
        private readonly SqlServerCacheOptions _options;

        public SqlOperationsForMono(SqlServerCacheOptions options, ILoggerFactory loggerFactory)
            : base(options, loggerFactory)
        {
            _options = options;
            _logger = loggerFactory.CreateLogger<SqlOperationsForMono>();
        }

        public override byte[] GetCacheItem(string key)
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

                var reader = command.ExecuteReader(CommandBehavior.SingleRow);

                TimeSpan? slidingExpiration = null;
                DateTime? absoluteExpirationUTC = null;
                DateTime expirationTimeUTC;
                if (reader.Read())
                {
                    var id = reader.GetString(Columns.Indexes.CacheItemIdIndex);
                    value = (byte[])reader[Columns.Indexes.CacheItemValueIndex];
                    expirationTimeUTC = DateTime.Parse(reader[Columns.Indexes.ExpiresAtTimeUTCIndex].ToString());

                    if (!reader.IsDBNull(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            reader.GetInt64(Columns.Indexes.SlidingExpirationInTicksIndex));
                    }

                    if (!reader.IsDBNull(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = DateTime.Parse(
                            reader[Columns.Indexes.AbsoluteExpirationUTCIndex].ToString());
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

        public override async Task<byte[]> GetCacheItemAsync(string key)
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

                var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow);

                TimeSpan? slidingExpiration = null;
                DateTime? absoluteExpirationUTC = null;
                DateTime expirationTimeUTC;
                if (await reader.ReadAsync())
                {
                    var id = reader.GetString(Columns.Indexes.CacheItemIdIndex);
                    value = (byte[])reader[Columns.Indexes.CacheItemValueIndex];
                    expirationTimeUTC = DateTime.Parse(reader[Columns.Indexes.ExpiresAtTimeUTCIndex].ToString());

                    if (!await reader.IsDBNullAsync(Columns.Indexes.SlidingExpirationInTicksIndex))
                    {
                        slidingExpiration = TimeSpan.FromTicks(
                            Convert.ToInt64(reader[Columns.Indexes.SlidingExpirationInTicksIndex].ToString()));
                    }

                    if (!await reader.IsDBNullAsync(Columns.Indexes.AbsoluteExpirationUTCIndex))
                    {
                        absoluteExpirationUTC = DateTime.Parse(
                            reader[Columns.Indexes.AbsoluteExpirationUTCIndex].ToString());
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
    }
}