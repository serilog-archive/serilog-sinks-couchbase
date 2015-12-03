// Copyright 2014 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using Serilog.Debugging;
using Serilog.Sinks.PeriodicBatching;
using LogEvent = Serilog.Sinks.Couchbase.Data.LogEvent;

namespace Serilog.Sinks.Couchbase
{
    using System.Linq;
    using global::Couchbase;
    using global::Couchbase.Configuration;
    using global::Couchbase.Configuration.Client;
    using global::Couchbase.Core;

    /// <summary>
    /// Writes log events as documents to a Couchbase database.
    /// </summary>
    public class CouchbaseSink : PeriodicBatchingSink
    {
        readonly IFormatProvider _formatProvider;

        private readonly Cluster _couchbaseCluster;

        private readonly string _bucketName;

        private readonly string _bucketPassword;

        /// <summary>
        /// A reasonable default for the number of events posted in
        /// each batch.
        /// </summary>
        public const int DefaultBatchPostingLimit = 50;

        /// <summary>
        /// A reasonable default time to wait between checking for event batches.
        /// </summary>
        public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Construct a sink posting to the specified database.
        /// </summary>
        /// <param name="couchbaseUriList">A list of a Couchbase database servers.</param>
        /// <param name="bucketName">The bucket to store batches in.</param>
        /// <param name="batchPostingLimit">The maximum number of events to post in a single batch.</param>
        /// <param name="period">The time to wait between checking for event batches.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="bucketPassword"></param>
        public CouchbaseSink(List<Uri> couchbaseUriList, string bucketName, string bucketPassword, int batchPostingLimit, TimeSpan period, IFormatProvider formatProvider)
            : base(batchPostingLimit, period)
        {
            if (couchbaseUriList == null) throw new ArgumentNullException(nameof(couchbaseUriList));
            if (couchbaseUriList.Count == 0) throw new ArgumentException(nameof(couchbaseUriList));
            if (couchbaseUriList[0] == null) throw new ArgumentNullException(nameof(couchbaseUriList));

            if (bucketName == null) throw new ArgumentNullException(nameof(bucketName));

            var config = new ClientConfiguration();

            // Clearing servers here because for some reason it's trying to auto-add localhost:8091/pools
            config.Servers.Clear();

            foreach (var uri in couchbaseUriList)
                config.Servers.Add(uri);

            var cluster = new Cluster(config);

            IBucket bucket = null;

            try
            {
                // Not using BucketConfigs property off of the Cluster because the OpenBucket method is not
                // accepting 0 arguments in 2.2.2.0 of the Couchbase SDK as it is documented that it should.
                bucket = cluster.OpenBucket(bucketName, bucketPassword);
            }
            catch(CouchbaseBootstrapException exception)
            {
                throw new InvalidOperationException(exception.InnerException.Message);
            }
            finally
            {
                if (bucket != null)
                {
                    cluster.CloseBucket(bucket);
                }
                else
                {
                    throw new InvalidOperationException("bucket '" + bucketName + "' does not exist");
                }
            }

            _bucketName = bucketName;

            _bucketPassword = bucketPassword;

            _couchbaseCluster = cluster;

            _formatProvider = formatProvider;
        }

        /// <summary>
        /// Free resources held by the sink.
        /// </summary>
        /// <param name="disposing">If true, called because the object is being disposed; if false,
        /// the object is being disposed from the finalizer.</param>
        protected override void Dispose(bool disposing)
        {
            // First flush the buffer
            base.Dispose(disposing);

            if (disposing)
                _couchbaseCluster.Dispose();
        }

        /// <summary>
        /// Emit a batch of log events.
        /// </summary>
        /// <param name="events">The events to emit.</param>
        /// <remarks>Override either <see cref="PeriodicBatchingSink.EmitBatch"/> or <see cref="PeriodicBatchingSink.EmitBatchAsync"/>,
        /// not both.</remarks>
        protected override void EmitBatch(IEnumerable<Events.LogEvent> events)
        {
            // This sink doesn't actually write batches, instead only using
            // the PeriodicBatching infrastructure to manage background work.
            // Probably needs modification.
            IBucket bucket = null;

            try
            {
                bucket = _couchbaseCluster.OpenBucket(_bucketName, _bucketPassword);

                foreach (var result in events.Select(logEvent => bucket.Insert(new Document<LogEvent>
                                                                               {
                                                                                   Id = Guid.NewGuid().ToString(),
                                                                                   Content = new LogEvent(logEvent, logEvent.RenderMessage(_formatProvider))
                                                                               })).Where(result => !result.Success))
                {
                    SelfLog.WriteLine("Failed to store value");
                }
            }
            finally
            {
                if (bucket != null)
                {
                    _couchbaseCluster.CloseBucket(bucket);
                }
            }
        }
    }
}
