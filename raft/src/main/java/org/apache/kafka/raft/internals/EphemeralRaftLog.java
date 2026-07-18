/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.Isolation;
import org.apache.kafka.raft.LogAppendInfo;
import org.apache.kafka.raft.LogFetchInfo;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.RaftLog;
import org.apache.kafka.raft.ValidOffsetAndEpoch;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.storage.log.UnexpectedAppendOffsetException;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.storage.internals.log.LeaderHwChange;
import org.apache.kafka.storage.internals.log.OffsetsOutOfOrderException;
import org.apache.kafka.storage.internals.log.RecordValidationStats;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH;
import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;
import static org.apache.kafka.storage.internals.log.LocalLog.UNKNOWN_OFFSET;


public class EphemeralRaftLog implements RaftLog {

    // ── Record storage ──────────────────────────────────────────────
    // baseOffset → batch (NavigableMap for range queries)
    private final TreeMap<Long, MutableRecordBatch> batches = new TreeMap<>();

    // ── Epoch tracking ──────────────────────────────────────────────
    // leaderEpoch → firstOffsetOfEpoch
    private final InMemoryEpochCache epochCache = new InMemoryEpochCache();

    // Snapshot tracking ──────────────────────────────────────────────
    private Optional<OffsetAndEpoch> latestSnapshotId = Optional.empty();

    // ── Offset bookkeeping ──────────────────────────────────────────
    private long startOffset = 0L;
    private long endOffset   = 0L;   // exclusive (next offset to write)

    // ── High watermark ──────────────────────────────────────────────
    private LogOffsetMetadata highWatermark = new LogOffsetMetadata(0L);

    // ── Metadata ────────────────────────────────────────────────────
    private final TopicPartition topicPartition;
    private final Uuid topicId;
    
    private final Logger logger;
    private final String logIdent;
    
    private final boolean autoclean;

    private static final int MAX_BATCHES_BELOW_HWM = 1000;


    private EphemeralRaftLog(
            TopicPartition topicPartition,
            Uuid topicId,
            int nodeId,
            long restartOffset,
            boolean autoclean) {
        this.topicPartition = topicPartition;
        this.topicId = topicId;
        this.logIdent = "[RaftLog (Observer) nodeId=" + nodeId + "] ";
        this.logger = new LogContext(logIdent).logger(EphemeralRaftLog.class);
        this.autoclean = autoclean;
        this.startOffset = restartOffset;
        this.endOffset = restartOffset;
        this.highWatermark = new LogOffsetMetadata(restartOffset);
    }

    @Override
    public LogFetchInfo read(long fetchStartOffset, Isolation isolation, int maxBytes) {
        
        long fetchEndOffset = isolation == Isolation.COMMITTED
                ? highWatermark.offset()
                : endOffset;

        if (fetchStartOffset >= fetchEndOffset || fetchStartOffset < startOffset) {
            return new LogFetchInfo(MemoryRecords.EMPTY, new LogOffsetMetadata(fetchStartOffset));
        }

        // Pass 1 : collect eligible batches
        List<MutableRecordBatch> collected = new ArrayList<>();
        int totalSize = eligiblesBatches(collected, fetchStartOffset, fetchEndOffset, maxBytes);

        if (collected.isEmpty()) {
            return new LogFetchInfo(MemoryRecords.EMPTY, new LogOffsetMetadata(startOffset));
        }

        // Pass 2 : allocate exactly the real size
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (MutableRecordBatch batch : collected) {
            batch.writeTo(buffer);
        }
        buffer.flip();

        long firstOffset = collected.get(0).baseOffset();
        return new LogFetchInfo(
            MemoryRecords.readableRecords(buffer),
            new LogOffsetMetadata(firstOffset)
        );
    }

    private int eligiblesBatches(List<MutableRecordBatch> collected, long fetchStartOffset, long fetchEndOffset, int maxBytes) {
        int totalSize = 0;

        Long floorKey = batches.floorKey(fetchStartOffset);
        NavigableMap<Long, MutableRecordBatch> view =
                batches.tailMap(floorKey != null ? floorKey : fetchStartOffset, true);
        for (MutableRecordBatch batch : view.values()) {
            if (batch.lastOffset() < fetchStartOffset) continue;
            if (batch.baseOffset() >= fetchEndOffset) break;

            int batchSize = batch.sizeInBytes();
            if (!collected.isEmpty() && (totalSize + batchSize) > maxBytes) break;

            // At least 1 batch (standard Kafka behavior)
            collected.add(batch);
            totalSize += batchSize;
        }

        return totalSize;
    }

    @Override
    public LogAppendInfo appendAsLeader(Records records, int leaderEpoch) {
        if (records.sizeInBytes() == 0) {
            throw new IllegalArgumentException("Attempt to append an empty record set");
        }
        throw new UnexpectedAppendOffsetException("appendAsLeader() not supported by EphemeralRaftLog", 0L, 0L);
    }

    @Override
    public LogAppendInfo appendAsFollower(Records records, int leaderEpoch) {
        if (records.sizeInBytes() == 0) {
            throw new IllegalArgumentException("Attempt to append an empty record set");
        }
        
        org.apache.kafka.storage.internals.log.LogAppendInfo appendInfo = analyzeAndValidateRecords((MemoryRecords) records, leaderEpoch);
        if (appendInfo.firstOffset() == UNKNOWN_OFFSET) {
            throw new CorruptRecordException("Append failed unexpectedly " + appendInfo);
        }

        // return if we have no valid messages or if this is a duplicate of the last appended entry
        if (appendInfo.validBytes() <= 0) {
            return new LogAppendInfo(appendInfo.firstOffset(), appendInfo.lastOffset());
        }

        // trim any invalid bytes or partial messages before appending it to the on-disk log
        final MemoryRecords trimmedRecords = trimInvalidBytes((MemoryRecords) records, appendInfo);
        MemoryRecords validRecords = trimmedRecords;

        // we are taking the offsets we are given
        if (appendInfo.firstOrLastOffsetOfFirstBatch() < endOffset) {
            // we may still be able to recover if the log is empty
            // one example: fetching from log start offset on the leader which is not batch aligned,
            // which may happen as a result of AdminClient#deleteRecords()
            boolean hasFirstOffset = appendInfo.firstOffset() != UNKNOWN_OFFSET;
            long firstOffset = hasFirstOffset ? appendInfo.firstOffset() : records.batches().iterator().next().baseOffset();

            String firstOrLast = hasFirstOffset ? "First offset" : "Last offset of the first batch";
            List<String> offsets = new ArrayList<>();
            for (Record record : records.records()) {
                offsets.add(String.valueOf(record.offset()));
                if (offsets.size() == 10) break;
            }
            throw new UnexpectedAppendOffsetException(
                    "Unexpected offset in append to " + topicPartition() + ". " + firstOrLast + " " +
                            appendInfo.firstOrLastOffsetOfFirstBatch() + " is less than the next offset " + endOffset() + ". " +
                            "First 10 offsets in append: " + String.join(", ", offsets) + ", last offset in" +
                            " append: " + appendInfo.lastOffset() + ". Log start offset = " + startOffset,
                    firstOffset, appendInfo.lastOffset());
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        validRecords.batches().forEach(batch -> {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                epochCache.assign(batch.partitionLeaderEpoch(), batch.baseOffset());
            } else {
                // In partial upgrade scenarios, we may get a temporary regression to the message format. In
                // order to ensure the safety of leader election, we clear the epoch cache so that we revert
                // to truncation by high watermark after the next leader election.
                if (epochCache.latestEpoch().isPresent()) {
                    logger.warn("Clearing leader epoch cache after unexpected append with message format v{}", batch.magic());
                    epochCache.clear();
                }
            }
        });

        // Append the records, and increment the local log end offset immediately after the append
        append(validRecords, appendInfo.lastOffset());

        logger.trace("Appended message set with last offset: {}, first offset: {}, next offset: {}, and messages: {}",
                appendInfo.lastOffset(), appendInfo.firstOffset(), endOffset, validRecords);

        return new LogAppendInfo(appendInfo.firstOffset(), appendInfo.lastOffset());
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other
     * </ol>
     *
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private org.apache.kafka.storage.internals.log.LogAppendInfo analyzeAndValidateRecords(MemoryRecords records,
                                                    int leaderEpoch) {
        int validBytesCount = 0;
        long firstOffset = UNKNOWN_OFFSET;
        long lastOffset = -1L;
        int lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        CompressionType sourceCompression = CompressionType.NONE;
        boolean monotonic = true;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        boolean readFirstMessage = false;
        long lastOffsetOfFirstBatch = -1L;
        boolean skipRemainingBatches = false;

        for (MutableRecordBatch batch : records.batches()) {

            /* During replication of uncommitted data it is possible for the remote replica to send record batches after it lost
             * leadership. This can happen if sending FETCH responses is slow. There is a race between sending the FETCH
             * response and the replica truncating and appending to the log. The replicating replica resolves this issue by only
             * persisting up to the current leader epoch used in the fetch request. See KAFKA-18723 for more details.
             */
            skipRemainingBatches = skipRemainingBatches || hasHigherPartitionLeaderEpoch(batch, leaderEpoch);
            if (skipRemainingBatches) {
                logger.info("Skipping batch {} because its partition leader epoch {} is higher than the replica's current leader epoch {}",
                        batch, batch.partitionLeaderEpoch(), leaderEpoch);
            } else {
                // update the first offset if on the first message. For magic versions older than 2, we use the last offset
                // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
                // For magic version 2, we can get the first offset directly from the batch header.
                // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
                // case, validation will be more lenient.
                // Also indicate whether we have the accurate first offset or not
                if (!readFirstMessage) {
                    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                        firstOffset = batch.baseOffset();
                    }
                    lastOffsetOfFirstBatch = batch.lastOffset();
                    readFirstMessage = true;
                }

                // check that offsets are monotonically increasing
                if (lastOffset >= batch.lastOffset()) {
                    monotonic = false;
                }

                // update the last offset seen
                lastOffset = batch.lastOffset();
                lastLeaderEpoch = batch.partitionLeaderEpoch();

                // check the validity of the message by checking CRC
                if (!batch.isValid()) {
                    throw new CorruptRecordException("Record is corrupt (stored crc = " + batch.checksum() + ") in topic partition " + topicPartition() + ".");
                }

                if (batch.maxTimestamp() > maxTimestamp) {
                    maxTimestamp = batch.maxTimestamp();
                }

                int batchSize = batch.sizeInBytes();
                validBytesCount += batchSize;

                CompressionType batchCompression = CompressionType.forId(batch.compressionType().id);
                // sourceCompression is only used on the leader path, which only contains one batch if version is v2 or messages are compressed
                if (batchCompression != CompressionType.NONE) {
                    sourceCompression = batchCompression;
                }
            }

            if (!monotonic) {
                throw new OffsetsOutOfOrderException("Out of order offsets found in append to " + topicPartition() + ": " +
                        StreamSupport.stream(records.records().spliterator(), false)
                            .map(Record::offset)
                            .map(String::valueOf)
                            .collect(Collectors.joining(",")));
            }
        }
        Optional<Integer> lastLeaderEpochOpt = (lastLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH)
                ? Optional.of(lastLeaderEpoch)
                : Optional.empty();

        return new org.apache.kafka.storage.internals.log.LogAppendInfo(firstOffset, lastOffset, lastLeaderEpochOpt, maxTimestamp,
                RecordBatch.NO_TIMESTAMP, startOffset, RecordValidationStats.EMPTY, sourceCompression,
                validBytesCount, lastOffsetOfFirstBatch, List.of(), LeaderHwChange.NONE);
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param records The records to trim
     * @param info The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in, or it may not.
     */
    private MemoryRecords trimInvalidBytes(MemoryRecords records, org.apache.kafka.storage.internals.log.LogAppendInfo info) {
        int validBytes = info.validBytes();
        if (validBytes < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length " + validBytes + " to " +
                    "log for " + topicPartition() + ". A possible cause is a corrupted produce request.");
        }
        if (validBytes == records.sizeInBytes()) {
            return records;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytes);
            return MemoryRecords.readableRecords(validByteBuffer);
        }
    }

    /**
     * Return true if the record batch has a higher leader epoch than the specified leader epoch
     *
     * @param batch the batch to validate
     * @param leaderEpoch the epoch to compare
     * @return true if the append reason is replication and the batch's partition leader epoch is
     *         greater than the specified leaderEpoch, otherwise false
     */
    private static boolean hasHigherPartitionLeaderEpoch(RecordBatch batch, int leaderEpoch) {
        return batch.partitionLeaderEpoch() != RecordBatch.NO_PARTITION_LEADER_EPOCH
                && batch.partitionLeaderEpoch() > leaderEpoch;
    }

    private void append(MemoryRecords records, long lastOffset) {

        // Loop on batches
        for (MutableRecordBatch batch : records.batches()) {
            batches.put(batch.baseOffset(), batch);
        }

        endOffset = lastOffset + 1;
    }
    
    @Override
    public int lastFetchedEpoch() {
        return epochCache.latestEpoch().orElse(0);
    }

    @Override
    public OffsetAndEpoch endOffsetForEpoch(int epoch) {
        Optional<OffsetAndEpoch> endOffsetEpochOpt = epochCache.endOffsetForEpoch(epoch, endOffset);
        if (endOffsetEpochOpt.isPresent()) {
            OffsetAndEpoch endOffsetEpoch = endOffsetEpochOpt.get();
            return new OffsetAndEpoch(endOffsetEpoch.offset(), endOffsetEpoch.epoch());
        } else {
            return new OffsetAndEpoch(endOffset, lastFetchedEpoch());
        }
    }

    @Override
    public LogOffsetMetadata endOffset() {
        return new LogOffsetMetadata(endOffset);
    }

    @Override
    public long startOffset() {
        return startOffset;
    }

    @Override
    public void truncateTo(long offset) {
        if (offset >= endOffset) return;
        // if (offset < startOffset) {
        //    throw new OffsetOutOfRangeException("Cannot truncate below startOffset");
        // }

        // Delete all batches with baseOffset ≥ offset
        batches.tailMap(offset).clear();

        // Delete batches as well partially beyond offset
        // (batch with baseOffset < offset but lastOffset >= offset)
        Map.Entry<Long, MutableRecordBatch> floor = batches.floorEntry(offset - 1);
        if (floor != null && floor.getValue().lastOffset() >= offset) {
            // This batch overlaps the troncation point — rare case with Raft
            // we delete it too (we don't cut a batch)
            batches.remove(floor.getKey());
        }

        endOffset = offset;
        epochCache.truncateFromEnd(offset);
    }

    @Override
    public boolean truncateToLatestSnapshot() {
        if (latestSnapshotId.isPresent()) {
            startOffset = latestSnapshotId.get().offset();
            if (startOffset >= endOffset) endOffset = startOffset;
        }
        highWatermark = new LogOffsetMetadata(endOffset);
        if (autoclean) trimMemory();
        return false;
    }

    @Override
    public void initializeLeaderEpoch(int epoch) {
        // Anchor the new epoch to the current endOffset in the in-memory cache
        // An observer never becomes leader, but this method may be called 
        // during a transition of internal state of the RaftClient
        epochCache.assign(epoch, endOffset);
    }

    @Override
    public void updateHighWatermark(LogOffsetMetadata offsetMetadata) {
        if (offsetMetadata.offset() > highWatermark.offset()) {
            highWatermark = new LogOffsetMetadata(offsetMetadata.offset());
            // Free memory of batches far below HWM
            if (autoclean) trimMemory();
        }
    }

    private boolean trimMemory() {
        boolean cleaned = false;
        long trimUpTo = highWatermark.offset();

        // Look at only what is necessary above the current startOffset
        // We can delete everything below < HWM
        // as an observer does not need to replay
        NavigableMap<Long, MutableRecordBatch> committed = batches.headMap(trimUpTo, false);

        if (committed.size() > MAX_BATCHES_BELOW_HWM) {
            // Move forward the startOffset and purge
            long newStart = committed.lastKey();
            batches.headMap(newStart, false).clear();
            startOffset = newStart;
            epochCache.truncateFromStart(newStart);
            cleaned = true;
        }
        
        return cleaned;
    }
    
    @Override
    public LogOffsetMetadata highWatermark() {
        return highWatermark;
    }

    @Override
    public void flush(boolean forceFlushActiveSegment) {
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public Uuid topicId() {
        return topicId;
    }

    @Override
    public Optional<RawSnapshotWriter> createNewSnapshot(OffsetAndEpoch snapshotId) {
        long startOffset = startOffset();
        if (snapshotId.offset() < startOffset) {
            logger.info("Cannot create a snapshot with an id ({}) less than the log start offset ({})", snapshotId, startOffset);
            return Optional.empty();
        }

        long highWatermarkOffset = highWatermark().offset();
        if (snapshotId.offset() > highWatermarkOffset) {
            throw new IllegalArgumentException(
                    "Cannot create a snapshot with an id (" + snapshotId + ") greater than the high-watermark (" + highWatermarkOffset + ")"
            );
        }

        ValidOffsetAndEpoch validOffsetAndEpoch = validateOffsetAndEpoch(snapshotId.offset(), snapshotId.epoch());
        if (validOffsetAndEpoch.kind() != ValidOffsetAndEpoch.Kind.VALID) {
            throw new IllegalArgumentException(
                    "Snapshot id (" + snapshotId + ") is not valid according to the log: " + validOffsetAndEpoch
            );
        }

        /*
          Perform a check that the requested snapshot offset is batch aligned via a log read, which
          returns the base offset of the batch that contains the requested offset. A snapshot offset
          is one greater than the last offset contained in the snapshot, and cannot go past the high
          watermark.

          This check is necessary because Raft replication code assumes the snapshot offset is the
          start of a batch. If a follower applies a non-batch aligned snapshot at offset (X) and
          fetches from this offset, the returned batch will start at offset (X - M), and the
          follower will be unable to append it since (X - M) < (X).
         */
        long baseOffset = read(
            snapshotId.offset(),
            Isolation.COMMITTED,
            1 // maxTotalBatchBytes - ensures that we only fetch one batch.
        ).startOffsetMetadata.offset();

        if (snapshotId.offset() != baseOffset) {
            throw new IllegalArgumentException(
                    "Cannot create snapshot at offset (" + snapshotId.offset() + ") because it is not batch aligned. " +
                    "The batch containing the requested offset has a base offset of (" + baseOffset + ")"
            );
        }
        return createNewSnapshotUnchecked(snapshotId);
    }

    @Override
    public Optional<RawSnapshotWriter> createNewSnapshotUnchecked(OffsetAndEpoch snapshotId) {
        epochCache.assign(snapshotId.epoch(), snapshotId.offset());
        latestSnapshotId = Optional.of(snapshotId);
        return Optional.empty();
    }

    @Override
    public Optional<RawSnapshotReader> readSnapshot(OffsetAndEpoch snapshotId) {
        return Optional.empty();
    }

    @Override
    public Optional<RawSnapshotReader> latestSnapshot() {
        return Optional.empty();
    }

    @Override
    public Optional<OffsetAndEpoch> latestSnapshotId() {
        // return latestSnapshotId;
        // last Snapshot Id is only kept for internal purposes
        // but from user, this implementation does not handle any snapshot
        return Optional.empty();
    }

    @Override
    public Optional<OffsetAndEpoch> earliestSnapshotId() {
        return Optional.empty();
    }

    @Override
    public void onSnapshotFrozen(OffsetAndEpoch snapshotId) {
    }

    @Override
    public boolean deleteBeforeSnapshot(OffsetAndEpoch snapshotId) {
        return false;
    }

    @Override
    public boolean maybeClean() {
        return trimMemory();
    }

    @Override
    public void close() {
        batches.clear();
        epochCache.clear();
        highWatermark = new LogOffsetMetadata(0L);
        startOffset = 0L;
        endOffset   = 0L;

    }

    public static EphemeralRaftLog createLog(
            TopicPartition topicPartition,
            Uuid topicId,
            int nodeId,
            long restartOffset) {

        return createLog(
                topicPartition,
                topicId,
                nodeId,
                restartOffset,
                true);
    }

    public static EphemeralRaftLog createLog(
            TopicPartition topicPartition,
            Uuid topicId,
            int nodeId,
            long restartOffset,
            boolean autoClean) {

        EphemeralRaftLog metadataLog = new EphemeralRaftLog(
                topicPartition,
                topicId,
                nodeId,
                restartOffset,
                autoClean
        );

        return metadataLog;
    }

    private static class InMemoryEpochCache {

        // epoch → inclusive start offset of this epoch
        private final TreeMap<Integer, Long> epochStartOffsets = new TreeMap<>();

        public Optional<Integer> latestEpoch() {
            if (epochStartOffsets.isEmpty()) return Optional.empty();
            return Optional.of(epochStartOffsets.lastKey());
        }

        public void assign(int epoch, long startOffset) {
            // Clean futur epochs when we truncate as follower
            epochStartOffsets.tailMap(epoch, false).clear();
            epochStartOffsets.put(epoch, startOffset);
        }

        public Optional<OffsetAndEpoch> endOffsetForEpoch(int leaderEpoch, long logEndOffset) {
            Map.Entry<Integer, Long> entry = endOffsetFor(leaderEpoch, logEndOffset);
            int foundEpoch = entry.getKey();
            long foundOffset = entry.getValue();
            if (foundOffset == UNDEFINED_EPOCH_OFFSET) {
                return Optional.empty();
            } else {
                return Optional.of(new OffsetAndEpoch(foundOffset, foundEpoch));
            }
        }

        private Map.Entry<Integer, Long> endOffsetFor(int requestedEpoch, long logEndOffset) {
            Map.Entry<Integer, Long> epochAndOffset;
            if (requestedEpoch == UNDEFINED_EPOCH) {
                // This may happen if a bootstrapping follower sends a request with undefined epoch or
                // a follower is on the older message format where leader epochs are not recorded
                epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
            } else if (latestEpoch().isPresent() && latestEpoch().get() == requestedEpoch) {
                // For the leader, the latest epoch is always the current leader epoch that is still being written to.
                // Followers should not have any reason to query for the end offset of the current epoch, but a consumer
                // might if it is verifying its committed offset following a group rebalance. In this case, we return
                // the current log end offset which makes the truncation check work as expected.
                epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(requestedEpoch, logEndOffset);
            } else {
                Map.Entry<Integer, Long> higherEntry = epochStartOffsets.higherEntry(requestedEpoch);
                if (higherEntry == null) {
                    // The requested epoch is larger than any known epoch. This case should never be hit because
                    // the latest cached epoch is always the largest.
                    epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
                } else {
                    Map.Entry<Integer, Long> floorEntry = epochStartOffsets.floorEntry(requestedEpoch);
                    if (floorEntry == null) {
                        // The requested epoch is smaller than any known epoch, so we return the start offset of the first
                        // known epoch which is larger than it. This may be inaccurate as there could have been
                        // epochs in between, but the point is that the data has already been removed from the log
                        // and we want to ensure that the follower can replicate correctly beginning from the leader's
                        // start offset.
                        epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(requestedEpoch, higherEntry.getValue());
                    } else {
                        // We have at least one previous epoch and one subsequent epoch. The result is the first
                        // prior epoch and the starting offset of the first subsequent epoch.
                        epochAndOffset = new AbstractMap.SimpleImmutableEntry<>(floorEntry.getKey(), higherEntry.getValue());
                    }
                }
            }

            return epochAndOffset;
        }

        /*
        public Optional<OffsetAndEpoch> endOffsetFor(int requestedEpoch, long logEndOffset) {

            if (latestEpoch().isPresent() && latestEpoch().get() == requestedEpoch) {
                return Optional.of(new OffsetAndEpoch(logEndOffset, requestedEpoch));
            }

            OffsetAndEpoch epochAndOffset;
            Map.Entry<Integer, Long> higherEntry = epochStartOffsets.higherEntry(requestedEpoch);
            if (higherEntry == null) {
                // The requested epoch is larger than any known epoch. This case should never be hit because
                // the latest cached epoch is always the largest.
                epochAndOffset = new OffsetAndEpoch(UNDEFINED_EPOCH_OFFSET, UNDEFINED_EPOCH);
            } else {
                Map.Entry<Integer, Long> floorEntry = epochStartOffsets.floorEntry(requestedEpoch);
                if (floorEntry == null) {
                    // The requested epoch is smaller than any known epoch, so we return the start offset of the first
                    // known epoch which is larger than it. This may be inaccurate as there could have been
                    // epochs in between, but the point is that the data has already been removed from the log
                    // and we want to ensure that the follower can replicate correctly beginning from the leader's
                    // start offset.
                    epochAndOffset = new OffsetAndEpoch(higherEntry.getValue(), requestedEpoch);
                } else {
                    // We have at least one previous epoch and one subsequent epoch. The result is the first
                    // prior epoch and the starting offset of the first subsequent epoch.
                    epochAndOffset = new OffsetAndEpoch(higherEntry.getValue(), floorEntry.getKey());
                }
            }

            return Optional.of(epochAndOffset);
        }*/

        public void truncateFromEnd(long endOffset) {
            // Delete all epochs with startOffset ≥ endOffset
            epochStartOffsets.entrySet().removeIf(e -> e.getValue() >= endOffset);
            // Adjust current epoch if its start is over endOffset
            if (!epochStartOffsets.isEmpty()) {
                // the last epoch survive, but its endOffset is now endOffset
                // no modification necessary, it's implicit
            }
        }

        public void truncateFromStart(long startOffset) {
            // Delete epochs fully before startOffset
            epochStartOffsets.entrySet().removeIf(e -> {
                Map.Entry<Integer, Long> next = epochStartOffsets.higherEntry(e.getKey());
                return next != null && next.getValue() <= startOffset;
            });
        }

        public void clear() {
            epochStartOffsets.clear();
        }
    }
}
