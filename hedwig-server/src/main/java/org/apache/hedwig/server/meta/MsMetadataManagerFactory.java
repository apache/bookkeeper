/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.meta;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.metastore.MetaStore;
import org.apache.bookkeeper.metastore.MetastoreCallback;
import org.apache.bookkeeper.metastore.MetastoreCursor;
import org.apache.bookkeeper.metastore.MetastoreCursor.ReadEntriesCallback;
import org.apache.bookkeeper.metastore.MetastoreException;
import org.apache.bookkeeper.metastore.MetastoreFactory;
import org.apache.bookkeeper.metastore.MetastoreScannableTable;
import org.apache.bookkeeper.metastore.MetastoreScannableTable.Order;
import org.apache.bookkeeper.metastore.MetastoreTable;
import org.apache.bookkeeper.metastore.MetastoreUtils;

import static org.apache.bookkeeper.metastore.MetastoreTable.*;
import org.apache.bookkeeper.metastore.MetastoreTableItem;
import org.apache.bookkeeper.metastore.MSException;
import org.apache.bookkeeper.metastore.Value;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.topics.HubInfo;
import org.apache.hedwig.util.Callback;

import org.apache.zookeeper.ZooKeeper;

/**
 * MetadataManagerFactory for plug-in metadata storage.
 */
public class MsMetadataManagerFactory extends MetadataManagerFactory {
    protected final static Logger logger = LoggerFactory.getLogger(MsMetadataManagerFactory.class);

    static final String UTF8 = "UTF-8";

    static final int CUR_VERSION = 1;

    static final String OWNER_TABLE_NAME = "owner";
    static final String PERSIST_TABLE_NAME = "persist";
    static final String SUB_TABLE_NAME = "sub";

    static class SyncResult<T> {
        T value;
        int rc;
        boolean finished = false;

        public synchronized void complete(int rc, T value) {
            this.rc = rc;
            this.value = value;
            finished = true;

            notify();
        }

        public synchronized void block() throws InterruptedException {
            while (!finished) {
                wait();
            }
        }

        public int getReturnCode() {
            return rc;
        }

        public T getValue() {
            return value;
        }
    }

    MetaStore metastore;
    MetastoreTable ownerTable;
    MetastoreTable persistTable;
    MetastoreScannableTable subTable;
    ServerConfiguration cfg;

    @Override
    public MetadataManagerFactory initialize(ServerConfiguration cfg, ZooKeeper zk, int version) throws IOException {
        if (CUR_VERSION != version) {
            throw new IOException("Incompatible MsMetadataManagerFactory version " + version
                    + " found, expected version " + CUR_VERSION);
        }
        this.cfg = cfg;
        try {
            metastore = MetastoreFactory.createMetaStore(cfg.getMetastoreImplClass());
            // TODO: need to store metastore class and version in some place.
            metastore.init(cfg.getConf(), metastore.getVersion());
        } catch (Exception e) {
            throw new IOException("Load metastore failed : ", e);
        }

        try {
            ownerTable = metastore.createTable(OWNER_TABLE_NAME);
            if (ownerTable == null) {
                throw new IOException("create owner table failed");
            }

            persistTable = metastore.createTable(PERSIST_TABLE_NAME);
            if (persistTable == null) {
                throw new IOException("create persistence table failed");
            }

            subTable = metastore.createScannableTable(SUB_TABLE_NAME);
            if (subTable == null) {
                throw new IOException("create subscription table failed");
            }
        } catch (MetastoreException me) {
            throw new IOException("Failed to create tables : ", me);
        }

        return this;
    }

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public void shutdown() {
        if (metastore == null) {
            return;
        }

        if (ownerTable != null) {
            ownerTable.close();
            ownerTable = null;
        }

        if (persistTable != null) {
            persistTable.close();
            persistTable = null;
        }

        if (subTable != null) {
            subTable.close();
            subTable = null;
        }

        metastore.close();
        metastore = null;
    }

    @Override
    public Iterator<ByteString> getTopics() throws IOException {
        SyncResult<MetastoreCursor> syn = new SyncResult<MetastoreCursor>();
        persistTable.openCursor(NON_FIELDS, new MetastoreCallback<MetastoreCursor>() {
            public void complete(int rc, MetastoreCursor cursor, Object ctx) {
                @SuppressWarnings("unchecked")
                SyncResult<MetastoreCursor> syn = (SyncResult<MetastoreCursor>) ctx;
                syn.complete(rc, cursor);
            }
        }, syn);
        try {
            syn.block();
        } catch (Exception e) {
            throw new IOException("Interrupted on getting topics list : ", e);
        }

        if (syn.getReturnCode() != MSException.Code.OK.getCode()) {
            throw new IOException("Failed to get topics : ", MSException.create(
                    MSException.Code.get(syn.getReturnCode()), ""));
        }

        final MetastoreCursor cursor = syn.getValue();
        return new Iterator<ByteString>() {
            Iterator<MetastoreTableItem> itemIter = null;

            @Override
            public boolean hasNext() {
                while (null == itemIter || !itemIter.hasNext()) {
                    if (!cursor.hasMoreEntries()) {
                        return false;
                    }

                    try {
                        itemIter = cursor.readEntries(cfg.getMetastoreMaxEntriesPerScan());
                    } catch (MSException mse) {
                        logger.warn("Interrupted when iterating the topics list : ", mse);
                        return false;
                    }
                }
                return true;
            }

            @Override
            public ByteString next() {
                MetastoreTableItem t = itemIter.next();
                return ByteString.copyFromUtf8(t.getKey());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Doesn't support remove topic from topic iterator.");
            }
        };
    }

    @Override
    public TopicOwnershipManager newTopicOwnershipManager() {
        return new MsTopicOwnershipManagerImpl(ownerTable);
    }

    static class MsTopicOwnershipManagerImpl implements TopicOwnershipManager {

        static final String OWNER_FIELD = "owner";

        final MetastoreTable ownerTable;

        MsTopicOwnershipManagerImpl(MetastoreTable ownerTable) {
            this.ownerTable = ownerTable;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public void readOwnerInfo(final ByteString topic, final Callback<Versioned<HubInfo>> callback, Object ctx) {
            ownerTable.get(topic.toStringUtf8(), new MetastoreCallback<Versioned<Value>>() {
                @Override
                public void complete(int rc, Versioned<Value> value, Object ctx) {
                    if (MSException.Code.NoKey.getCode() == rc) {
                        callback.operationFinished(ctx, null);
                        return;
                    }

                    if (MSException.Code.OK.getCode() != rc) {
                        logErrorAndFinishOperation("Could not read ownership for topic " + topic.toStringUtf8(),
                                callback, ctx, rc);
                        return;
                    }

                    HubInfo owner = null;
                    try {
                        byte[] data = value.getValue().getField(OWNER_FIELD);
                        if (data != null) {
                            owner = HubInfo.parse(new String(data));
                        }
                    } catch (HubInfo.InvalidHubInfoException ihie) {
                        logger.warn("Failed to parse hub info for topic " + topic.toStringUtf8(), ihie);
                    }
                    Version version = value.getVersion();
                    callback.operationFinished(ctx, new Versioned<HubInfo>(owner, version));
                }
            }, ctx);
        }

        @Override
        public void writeOwnerInfo(final ByteString topic, final HubInfo owner, final Version version,
                final Callback<Version> callback, Object ctx) {
            Value value = new Value();
            value.setField(OWNER_FIELD, owner.toString().getBytes());

            ownerTable.put(topic.toStringUtf8(), value, version, new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version ver, Object ctx) {
                    if (MSException.Code.OK.getCode() == rc) {
                        callback.operationFinished(ctx, ver);
                        return;
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        // no node
                        callback.operationFailed(
                                ctx,
                                PubSubException.create(StatusCode.NO_TOPIC_OWNER_INFO, "No owner info found for topic "
                                        + topic.toStringUtf8()));
                        return;
                    } else if (MSException.Code.KeyExists.getCode() == rc) {
                        // key exists
                        callback.operationFailed(
                                ctx,
                                PubSubException.create(StatusCode.TOPIC_OWNER_INFO_EXISTS, "Owner info of topic "
                                        + topic.toStringUtf8() + " existed."));
                        return;
                    } else if (MSException.Code.BadVersion.getCode() == rc) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                "Bad version provided to update owner info of topic " + topic.toStringUtf8()));
                        return;
                    } else {
                        logErrorAndFinishOperation("Failed to update ownership of topic " + topic.toStringUtf8()
                                + " to " + owner, callback, ctx, rc);
                        return;
                    }
                }
            }, ctx);
        }

        @Override
        public void deleteOwnerInfo(final ByteString topic, Version version, final Callback<Void> callback,
                Object ctx) {
            ownerTable.remove(topic.toStringUtf8(), version, new MetastoreCallback<Void>() {
                @Override
                public void complete(int rc, Void value, Object ctx) {
                    if (MSException.Code.OK.getCode() == rc) {
                        logger.debug("Successfully deleted owner info for topic {}", topic.toStringUtf8());
                        callback.operationFinished(ctx, null);
                        return;
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        // no node
                        callback.operationFailed(
                                ctx,
                                PubSubException.create(StatusCode.NO_TOPIC_OWNER_INFO, "No owner info found for topic "
                                        + topic.toStringUtf8()));
                        return;
                    } else if (MSException.Code.BadVersion.getCode() == rc) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                "Bad version provided to delete owner info of topic " + topic.toStringUtf8()));
                        return;
                    } else {
                        logErrorAndFinishOperation("Failed to delete owner info for topic " + topic.toStringUtf8(),
                                callback, ctx, rc);
                        return;
                    }
                }
            }, ctx);
        }
    }

    @Override
    public TopicPersistenceManager newTopicPersistenceManager() {
        return new MsTopicPersistenceManagerImpl(persistTable);
    }

    static class MsTopicPersistenceManagerImpl implements TopicPersistenceManager {

        static final String PERSIST_FIELD = "prst";

        final MetastoreTable persistTable;

        MsTopicPersistenceManagerImpl(MetastoreTable persistTable) {
            this.persistTable = persistTable;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public void readTopicPersistenceInfo(final ByteString topic, final Callback<Versioned<LedgerRanges>> callback,
                Object ctx) {
            persistTable.get(topic.toStringUtf8(), new MetastoreCallback<Versioned<Value>>() {
                @Override
                public void complete(int rc, Versioned<Value> value, Object ctx) {
                    if (MSException.Code.OK.getCode() == rc) {
                        byte[] data = value.getValue().getField(PERSIST_FIELD);
                        if (data != null) {
                            parseAndReturnTopicLedgerRanges(topic, data, value.getVersion(), callback, ctx);
                        } else { // null data is same as NoKey
                            callback.operationFinished(ctx, null);
                        }
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        callback.operationFinished(ctx, null);
                    } else {
                        logErrorAndFinishOperation("Could not read ledgers node for topic " + topic.toStringUtf8(),
                                callback, ctx, rc);
                    }
                }
            }, ctx);
        }

        /**
         * Parse ledger ranges data and return it thru callback.
         *
         * @param topic
         *            Topic name
         * @param data
         *            Topic Ledger Ranges data
         * @param version
         *            Version of the topic ledger ranges data
         * @param callback
         *            Callback to return ledger ranges
         * @param ctx
         *            Context of the callback
         */
        private void parseAndReturnTopicLedgerRanges(ByteString topic, byte[] data, Version version,
                Callback<Versioned<LedgerRanges>> callback, Object ctx) {
            try {
                LedgerRanges.Builder rangesBuilder = LedgerRanges.newBuilder();
                TextFormat.merge(new String(data, UTF8), rangesBuilder);
                LedgerRanges lr = rangesBuilder.build();
                Versioned<LedgerRanges> ranges = new Versioned<LedgerRanges>(lr, version);
                callback.operationFinished(ctx, ranges);
            } catch (ParseException e) {
                StringBuilder sb = new StringBuilder();
                sb.append("Ledger ranges for topic ").append(topic.toStringUtf8())
                        .append(" could not be deserialized.");
                String msg = sb.toString();
                logger.error(msg, e);
                callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
            } catch (UnsupportedEncodingException uee) {
                StringBuilder sb = new StringBuilder();
                sb.append("Ledger ranges for topic ").append(topic.toStringUtf8()).append(" is not UTF-8 encoded.");
                String msg = sb.toString();
                logger.error(msg, uee);
                callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
            }
        }

        @Override
        public void writeTopicPersistenceInfo(final ByteString topic, LedgerRanges ranges, final Version version,
                final Callback<Version> callback, Object ctx) {
            Value value = new Value();
            value.setField(PERSIST_FIELD, TextFormat.printToString(ranges).getBytes());

            persistTable.put(topic.toStringUtf8(), value, version, new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version ver, Object ctx) {
                    if (MSException.Code.OK.getCode() == rc) {
                        callback.operationFinished(ctx, ver);
                        return;
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_TOPIC_PERSISTENCE_INFO,
                                "No persistence info found for topic " + topic.toStringUtf8()));
                        return;
                    } else if (MSException.Code.KeyExists.getCode() == rc) {
                        // key exists
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.TOPIC_PERSISTENCE_INFO_EXISTS,
                                "Persistence info of topic " + topic.toStringUtf8() + " existed."));
                        return;
                    } else if (MSException.Code.BadVersion.getCode() == rc) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                "Bad version provided to update persistence info of topic " + topic.toStringUtf8()));
                        return;
                    } else {
                        logErrorAndFinishOperation("Could not write ledgers node for topic " + topic.toStringUtf8(),
                                callback, ctx, rc);
                    }
                }
            }, ctx);
        }

        @Override
        public void deleteTopicPersistenceInfo(final ByteString topic, final Version version,
                final Callback<Void> callback, Object ctx) {
            persistTable.remove(topic.toStringUtf8(), version, new MetastoreCallback<Void>() {
                @Override
                public void complete(int rc, Void value, Object ctx) {
                    if (MSException.Code.OK.getCode() == rc) {
                        logger.debug("Successfully deleted persistence info for topic {}.", topic.toStringUtf8());
                        callback.operationFinished(ctx, null);
                        return;
                    } else if (MSException.Code.NoKey.getCode() == rc) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_TOPIC_PERSISTENCE_INFO,
                                "No persistence info found for topic " + topic.toStringUtf8()));
                        return;
                    } else if (MSException.Code.BadVersion.getCode() == rc) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                "Bad version provided to delete persistence info of topic " + topic.toStringUtf8()));
                        return;
                    } else {
                        logErrorAndFinishOperation("Failed to delete persistence info topic: " + topic.toStringUtf8()
                                + ", version: " + version, callback, ctx, rc, StatusCode.SERVICE_DOWN);
                        return;
                    }
                }
            }, ctx);
        }
    }

    @Override
    public SubscriptionDataManager newSubscriptionDataManager() {
        return new MsSubscriptionDataManagerImpl(cfg, subTable);
    }

    static class MsSubscriptionDataManagerImpl implements SubscriptionDataManager {

        static final String SUB_STATE_FIELD = "sub_state";
        static final String SUB_PREFS_FIELD = "sub_preferences";

        static final char TOPIC_SUB_FIRST_SEPARATOR = '\001';
        static final char TOPIC_SUB_LAST_SEPARATOR = '\002';

        final ServerConfiguration cfg;
        final MetastoreScannableTable subTable;

        MsSubscriptionDataManagerImpl(ServerConfiguration cfg, MetastoreScannableTable subTable) {
            this.cfg = cfg;
            this.subTable = subTable;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        private String getSubscriptionKey(ByteString topic, ByteString subscriberId) {
            return new StringBuilder(topic.toStringUtf8()).append(TOPIC_SUB_FIRST_SEPARATOR)
                    .append(subscriberId.toStringUtf8()).toString();
        }

        private Value subscriptionData2Value(SubscriptionData subData) {
            Value value = new Value();
            if (subData.hasState()) {
                value.setField(SUB_STATE_FIELD, TextFormat.printToString(subData.getState()).getBytes());
            }
            if (subData.hasPreferences()) {
                value.setField(SUB_PREFS_FIELD, TextFormat.printToString(subData.getPreferences()).getBytes());
            }
            return value;
        }

        @Override
        public void createSubscriptionData(final ByteString topic, final ByteString subscriberId,
                final SubscriptionData subData, final Callback<Version> callback, Object ctx) {
            String key = getSubscriptionKey(topic, subscriberId);
            Value value = subscriptionData2Value(subData);

            subTable.put(key, value, Version.NEW, new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version ver, Object ctx) {
                    if (rc == MSException.Code.OK.getCode()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully create subscription for topic: " + topic.toStringUtf8()
                                    + ", subscriberId: " + subscriberId.toStringUtf8() + ", data: "
                                    + SubscriptionStateUtils.toString(subData));
                        }
                        callback.operationFinished(ctx, ver);
                    } else if (rc == MSException.Code.KeyExists.getCode()) {
                        callback.operationFailed(ctx, PubSubException.create(
                                StatusCode.SUBSCRIPTION_STATE_EXISTS,
                                "Subscription data for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                        + subscriberId.toStringUtf8() + ") existed."));
                        return;
                    } else {
                        logErrorAndFinishOperation("Failed to create topic: " + topic.toStringUtf8()
                                + ", subscriberId: " + subscriberId.toStringUtf8() + ", data: "
                                + SubscriptionStateUtils.toString(subData), callback, ctx, rc);
                    }
                }
            }, ctx);
        }

        @Override
        public boolean isPartialUpdateSupported() {
            // TODO: Here we assume Metastore support partial update, but this
            // maybe incorrect.
            return true;
        }

        @Override
        public void replaceSubscriptionData(final ByteString topic, final ByteString subscriberId,
                final SubscriptionData subData, final Version version, final Callback<Version> callback,
                final Object ctx) {
            updateSubscriptionData(topic, subscriberId, subData, version, callback, ctx);
        }

        @Override
        public void updateSubscriptionData(final ByteString topic, final ByteString subscriberId,
                final SubscriptionData subData, final Version version, final Callback<Version> callback,
                final Object ctx) {
            String key = getSubscriptionKey(topic, subscriberId);
            Value value = subscriptionData2Value(subData);

            subTable.put(key, value, version, new MetastoreCallback<Version>() {
                @Override
                public void complete(int rc, Version version, Object ctx) {
                    if (rc == MSException.Code.OK.getCode()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully updated subscription data for topic: " + topic.toStringUtf8()
                                    + ", subscriberId: " + subscriberId.toStringUtf8() + ", data: "
                                    + SubscriptionStateUtils.toString(subData) + ", version: " + version);
                        }
                        callback.operationFinished(ctx, version);
                    } else if (rc == MSException.Code.NoKey.getCode()) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_SUBSCRIPTION_STATE,
                                "No subscription data found for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                        + subscriberId.toStringUtf8() + ")."));
                        return;
                    } else if (rc == MSException.Code.BadVersion.getCode()) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                "Bad version provided to update subscription data of topic " + topic.toStringUtf8()
                                        + " subscriberId " + subscriberId));
                        return;
                    } else {
                        logErrorAndFinishOperation(
                                "Failed to update subscription data for topic: " + topic.toStringUtf8()
                                        + ", subscriberId: " + subscriberId.toStringUtf8() + ", data: "
                                        + SubscriptionStateUtils.toString(subData) + ", version: " + version, callback,
                                ctx, rc);
                    }
                }
            }, ctx);
        }

        @Override
        public void deleteSubscriptionData(final ByteString topic, final ByteString subscriberId, Version version,
                final Callback<Void> callback, Object ctx) {
            String key = getSubscriptionKey(topic, subscriberId);
            subTable.remove(key, version, new MetastoreCallback<Void>() {
                @Override
                public void complete(int rc, Void value, Object ctx) {
                    if (rc == MSException.Code.OK.getCode()) {
                        logger.debug("Successfully delete subscription for topic: {}, subscriberId: {}.",
                                topic.toStringUtf8(), subscriberId.toStringUtf8());
                        callback.operationFinished(ctx, null);
                        return;
                    } else if (rc == MSException.Code.BadVersion.getCode()) {
                        // bad version
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.BAD_VERSION,
                                "Bad version provided to delete subscriptoin data of topic " + topic.toStringUtf8()
                                        + " subscriberId " + subscriberId));
                        return;
                    } else if (rc == MSException.Code.NoKey.getCode()) {
                        // no node
                        callback.operationFailed(ctx, PubSubException.create(StatusCode.NO_SUBSCRIPTION_STATE,
                                "No subscription data found for (topic:" + topic.toStringUtf8() + ", subscriber:"
                                        + subscriberId.toStringUtf8() + ")."));
                        return;
                    } else {
                        logErrorAndFinishOperation("Failed to delete subscription topic: " + topic.toStringUtf8()
                                + ", subscriberId: " + subscriberId.toStringUtf8(), callback, ctx, rc,
                                StatusCode.SERVICE_DOWN);
                    }
                }
            }, ctx);
        }

        private SubscriptionData value2SubscriptionData(Value value) throws ParseException,
                UnsupportedEncodingException {
            SubscriptionData.Builder builder = SubscriptionData.newBuilder();

            byte[] stateData = value.getField(SUB_STATE_FIELD);
            if (null != stateData) {
                SubscriptionState.Builder stateBuilder = SubscriptionState.newBuilder();
                TextFormat.merge(new String(stateData, UTF8), stateBuilder);
                SubscriptionState state = stateBuilder.build();
                builder.setState(state);
            }

            byte[] prefsData = value.getField(SUB_PREFS_FIELD);
            if (null != prefsData) {
                SubscriptionPreferences.Builder preferencesBuilder = SubscriptionPreferences.newBuilder();
                TextFormat.merge(new String(prefsData, UTF8), preferencesBuilder);
                SubscriptionPreferences preferences = preferencesBuilder.build();
                builder.setPreferences(preferences);
            }

            return builder.build();
        }

        @Override
        public void readSubscriptionData(final ByteString topic, final ByteString subscriberId,
                final Callback<Versioned<SubscriptionData>> callback, Object ctx) {
            String key = getSubscriptionKey(topic, subscriberId);
            subTable.get(key, new MetastoreCallback<Versioned<Value>>() {
                @Override
                public void complete(int rc, Versioned<Value> value, Object ctx) {
                    if (rc == MSException.Code.NoKey.getCode()) {
                        callback.operationFinished(ctx, null);
                        return;
                    }

                    if (rc != MSException.Code.OK.getCode()) {
                        logErrorAndFinishOperation(
                                "Could not read subscription data for topic: " + topic.toStringUtf8()
                                        + ", subscriberId: " + subscriberId.toStringUtf8(), callback, ctx, rc);
                        return;
                    }

                    try {
                        Versioned<SubscriptionData> subData = new Versioned<SubscriptionData>(
                                value2SubscriptionData(value.getValue()), value.getVersion());
                        if (logger.isDebugEnabled()) {
                            logger.debug("Found subscription while acquiring topic: " + topic.toStringUtf8()
                                    + ", subscriberId: " + subscriberId.toStringUtf8() + ", data: "
                                    + SubscriptionStateUtils.toString(subData.getValue()) + ", version: "
                                    + subData.getVersion());
                        }
                        callback.operationFinished(ctx, subData);
                    } catch (ParseException e) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Failed to deserialize subscription data for topic:").append(topic.toStringUtf8())
                                .append(", subscriberId: ").append(subscriberId.toStringUtf8());
                        String msg = sb.toString();
                        logger.error(msg, e);
                        callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                    } catch (UnsupportedEncodingException uee) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Subscription data for topic: ").append(topic.toStringUtf8())
                                .append(", subscriberId: ").append(subscriberId.toStringUtf8())
                                .append(" is not UFT-8 encoded");
                        String msg = sb.toString();
                        logger.error(msg, uee);
                        callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                    }
                }
            }, ctx);
        }

        private String getSubscriptionPrefix(ByteString topic, char sep) {
            return new StringBuilder(topic.toStringUtf8()).append(sep).toString();
        }

        private void readSubscriptions(final ByteString topic, final int keyLength, final MetastoreCursor cursor,
                final Map<ByteString, Versioned<SubscriptionData>> topicSubs,
                final Callback<Map<ByteString, Versioned<SubscriptionData>>> callback, Object ctx) {
            if (!cursor.hasMoreEntries()) {
                callback.operationFinished(ctx, topicSubs);
                return;
            }
            ReadEntriesCallback readCb = new ReadEntriesCallback() {
                @Override
                public void complete(int rc, Iterator<MetastoreTableItem> items, Object ctx) {
                    if (rc != MSException.Code.OK.getCode()) {
                        logErrorAndFinishOperation("Could not read subscribers for cursor " + cursor,
                                callback, ctx, rc);
                        return;
                    }
                    while (items.hasNext()) {
                        MetastoreTableItem item = items.next();
                        final ByteString subscriberId = ByteString.copyFromUtf8(item.getKey().substring(keyLength));
                        try {
                            Versioned<Value> vv = item.getValue();
                            Versioned<SubscriptionData> subData = new Versioned<SubscriptionData>(
                                    value2SubscriptionData(vv.getValue()), vv.getVersion());
                            topicSubs.put(subscriberId, subData);
                        } catch (ParseException e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("Failed to deserialize subscription data for topic: ")
                                    .append(topic.toStringUtf8()).append(", subscriberId: ")
                                    .append(subscriberId.toStringUtf8());
                            String msg = sb.toString();
                            logger.error(msg, e);
                            callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                            return;
                        } catch (UnsupportedEncodingException e) {
                            StringBuilder sb = new StringBuilder();
                            sb.append("Subscription data for topic: ").append(topic.toStringUtf8())
                                    .append(", subscriberId: ").append(subscriberId.toStringUtf8())
                                    .append(" is not UTF-8 encoded.");
                            String msg = sb.toString();
                            logger.error(msg, e);
                            callback.operationFailed(ctx, new PubSubException.UnexpectedConditionException(msg));
                            return;
                        }
                    }
                    readSubscriptions(topic, keyLength, cursor, topicSubs, callback, ctx);
                }
            };
            cursor.asyncReadEntries(cfg.getMetastoreMaxEntriesPerScan(), readCb, ctx);
        }

        @Override
        public void readSubscriptions(final ByteString topic,
                final Callback<Map<ByteString, Versioned<SubscriptionData>>> callback, Object ctx) {
            final String firstKey = getSubscriptionPrefix(topic, TOPIC_SUB_FIRST_SEPARATOR);
            String lastKey = getSubscriptionPrefix(topic, TOPIC_SUB_LAST_SEPARATOR);
            subTable.openCursor(firstKey, true, lastKey, true, Order.ASC, ALL_FIELDS,
                    new MetastoreCallback<MetastoreCursor>() {
                        @Override
                        public void complete(int rc, MetastoreCursor cursor, Object ctx) {
                            if (rc != MSException.Code.OK.getCode()) {
                                logErrorAndFinishOperation(
                                        "Could not read subscribers for topic " + topic.toStringUtf8(), callback, ctx,
                                        rc);
                                return;
                            }

                            final Map<ByteString, Versioned<SubscriptionData>> topicSubs =
                                    new ConcurrentHashMap<ByteString, Versioned<SubscriptionData>>();
                            readSubscriptions(topic, firstKey.length(), cursor, topicSubs, callback, ctx);
                        }
                    }, ctx);
        }
    }

    /**
     * callback finish operation with exception specify by code, regardless of
     * the value of return code rc.
     */
    private static <T> void logErrorAndFinishOperation(String msg, Callback<T> callback, Object ctx, int rc,
            StatusCode code) {
        logger.error(msg, MSException.create(MSException.Code.get(rc), ""));
        callback.operationFailed(ctx, PubSubException.create(code, msg));
    }

    /**
     * callback finish operation with corresponding PubSubException converted
     * from return code rc.
     */
    private static <T> void logErrorAndFinishOperation(String msg, Callback<T> callback, Object ctx, int rc) {
        StatusCode code;

        if (rc == MSException.Code.NoKey.getCode()) {
            code = StatusCode.NO_SUCH_TOPIC;
        } else if (rc == MSException.Code.ServiceDown.getCode()) {
            code = StatusCode.SERVICE_DOWN;
        } else {
            code = StatusCode.UNEXPECTED_CONDITION;
        }

        logErrorAndFinishOperation(msg, callback, ctx, rc, code);
    }

    @Override
    public void format(ServerConfiguration cfg, ZooKeeper zk) throws IOException {
        try {
            int maxEntriesPerScan = cfg.getMetastoreMaxEntriesPerScan();

            // clean topic ownership table.
            logger.info("Cleaning topic ownership table ...");
            MetastoreUtils.cleanTable(ownerTable, maxEntriesPerScan);
            logger.info("Cleaned topic ownership table successfully.");

            // clean topic subscription table.
            logger.info("Cleaning topic subscription table ...");
            MetastoreUtils.cleanTable(subTable, maxEntriesPerScan);
            logger.info("Cleaned topic subscription table successfully.");

            // clean topic persistence info table.
            logger.info("Cleaning topic persistence info table ...");
            MetastoreUtils.cleanTable(persistTable, maxEntriesPerScan);
            logger.info("Cleaned topic persistence info table successfully.");
        } catch (MSException mse) {
            throw new IOException("Exception when formatting hedwig metastore : ", mse);
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted when formatting hedwig metastore : ", ie);
        }
    }

}
