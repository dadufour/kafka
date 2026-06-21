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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.internal.ArbitraryMemoryRecords;
import org.apache.kafka.common.record.internal.InvalidMemoryRecordsProvider;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.raft.Isolation;
import org.apache.kafka.raft.LogAppendInfo;
import org.apache.kafka.raft.LogFetchInfo;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.RaftLog;
import org.apache.kafka.raft.ValidOffsetAndEpoch;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.snapshot.RawSnapshotWriter;

import net.jqwik.api.AfterFailureMode;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EphemeralRaftLogTest {

    @Test
    public void testInitialState() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        
        assertEquals(0, log.lastFetchedEpoch());
        assertEquals(0L, log.startOffset());
        assertEquals(0L, log.endOffset().offset());
        assertEquals(0L, log.highWatermark().offset());
        assertTrue(log.latestSnapshotId().isEmpty());
        assertEquals(0L, log.endOffsetForEpoch(1).offset());
    }
    
    @Test
    public void testLeaderEpoch() throws IOException {
        int numberOfRecordsPerBatch = 5;
        int epoch = 2;
        EphemeralRaftLog log = buildMetadataLog();

        append(log, numberOfRecordsPerBatch, epoch);
        epoch++;

        assertEquals(0L, log.endOffsetForEpoch(1).offset());

        append(log, numberOfRecordsPerBatch, epoch);
        epoch++;
        append(log, numberOfRecordsPerBatch, epoch);
        epoch++;
        append(log, numberOfRecordsPerBatch, epoch);
        epoch++;
        append(log, numberOfRecordsPerBatch, epoch);

        assertEquals(0L, log.endOffsetForEpoch(1).offset());
        assertEquals(1L * numberOfRecordsPerBatch, log.endOffsetForEpoch(2).offset());
        assertEquals(2L * numberOfRecordsPerBatch, log.endOffsetForEpoch(3).offset());
        assertEquals(3L * numberOfRecordsPerBatch, log.endOffsetForEpoch(4).offset());
        assertEquals(4L * numberOfRecordsPerBatch, log.endOffsetForEpoch(5).offset());
        assertEquals(5L * numberOfRecordsPerBatch, log.endOffsetForEpoch(6).offset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(5L * numberOfRecordsPerBatch, log.endOffset().offset());
        assertEquals(0L, log.highWatermark().offset());
        
        // Trunc future epoch as follower
        epoch = 4;
        log.initializeLeaderEpoch(epoch);
        
        // End offset is not changed
        assertEquals(5L * numberOfRecordsPerBatch, log.endOffset().offset());
        // Missing epoch returns end offset
        assertEquals(log.endOffset().offset(), log.endOffsetForEpoch(5).offset());
        // But epochs are purged
        assertEquals(epoch, log.lastFetchedEpoch());
        
        append(log, numberOfRecordsPerBatch, epoch);

        assertEquals(6L * numberOfRecordsPerBatch, log.endOffsetForEpoch(4).offset());
        
        // purge batches
        log.truncateTo(2L * numberOfRecordsPerBatch + 2);
        
        assertEquals(2L * numberOfRecordsPerBatch + 2, log.endOffset().offset());
        assertEquals(log.endOffset().offset(), log.endOffsetForEpoch(4).offset());
        // Missing epoch returns end offset
        assertEquals(log.endOffset().offset(), log.endOffsetForEpoch(5).offset());
    }

    @Test
    public void testRead() throws IOException {
        int numberOfRecordsPerBatch = 5;
        int epoch = 2;
        EphemeralRaftLog log = buildMetadataLog();

        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        
        epoch++;
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);
        append(log, numberOfRecordsPerBatch, epoch);

        LogFetchInfo info = log.read(
                0,
                Isolation.UNCOMMITTED,
                200 * 10
            );
        assertTrue(200 * 10 > info.records.sizeInBytes());
        assertEquals(10L * numberOfRecordsPerBatch - 1, info.records.lastBatch().get().lastOffset());
        assertEquals(0L, info.startOffsetMetadata.offset());
        
        info = log.read(
                42,
                Isolation.UNCOMMITTED,
                200 * 10
            );
        assertEquals(10L * numberOfRecordsPerBatch - 1, info.records.lastBatch().get().lastOffset());
        assertEquals(40L, info.startOffsetMetadata.offset());
        
        info = log.read(
                60,
                Isolation.UNCOMMITTED,
                200 * 10
            );
        assertTrue(info.records.lastBatch().isEmpty());

        info = log.read(
                12,
                Isolation.UNCOMMITTED,
                150
            );
        assertEquals(3L * numberOfRecordsPerBatch - 1, info.records.lastBatch().get().lastOffset());
    }
    
    @Test
    public void testUnexpectedAppendOffset() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();

        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        int currentEpoch = 3;
        long initialOffset = log.endOffset().offset();

        log.appendAsFollower(
                MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo),
                currentEpoch
        );

        assertThrows(
                RuntimeException.class,
                () -> log.appendAsFollower(MemoryRecords.withRecords(initialOffset, Compression.NONE, currentEpoch, recordFoo), currentEpoch)
        );
    }

    @Test
    public void testEmptyAppendNotAllowed() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();

        assertThrows(IllegalArgumentException.class, () -> log.appendAsFollower(MemoryRecords.EMPTY, 1));
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidMemoryRecordsProvider.class)
    public void testInvalidMemoryRecords(MemoryRecords records, Optional<Class<Exception>> expectedException) throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        long previousEndOffset = log.endOffset().offset();

        Executable action = () -> log.appendAsFollower(records, Integer.MAX_VALUE);
        if (expectedException.isPresent()) {
            assertThrows(expectedException.get(), action);
        } else {
            assertThrows(CorruptRecordException.class, action);
        }

        assertEquals(previousEndOffset, log.endOffset().offset());
    }

    @Property(tries = 100, afterFailure = AfterFailureMode.SAMPLE_ONLY)
    public void testRandomRecords(@ForAll(supplier = ArbitraryMemoryRecords.class) MemoryRecords records) throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        long previousEndOffset = log.endOffset().offset();

        assertThrows(
                CorruptRecordException.class,
                () -> log.appendAsFollower(records, Integer.MAX_VALUE)
        );

        assertEquals(previousEndOffset, log.endOffset().offset());
    }

    @Test
    public void testInvalidLeaderEpoch() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        long previousEndOffset = log.endOffset().offset();
        int epoch = log.lastFetchedEpoch() + 1;
        int numberOfRecords = 10;

        SimpleRecord[] simpleRecords = new SimpleRecord[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            simpleRecords[i] = new SimpleRecord(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        MemoryRecords batchWithValidEpoch = MemoryRecords.withRecords(
                previousEndOffset,
                Compression.NONE,
                epoch,
                simpleRecords
        );
        MemoryRecords batchWithInvalidEpoch = MemoryRecords.withRecords(
                previousEndOffset + numberOfRecords,
                Compression.NONE,
                epoch + 1,
                simpleRecords
        );

        ByteBuffer buffer = ByteBuffer.allocate(batchWithValidEpoch.sizeInBytes() + batchWithInvalidEpoch.sizeInBytes());
        buffer.put(batchWithValidEpoch.buffer());
        buffer.put(batchWithInvalidEpoch.buffer());
        buffer.flip();

        MemoryRecords records = MemoryRecords.readableRecords(buffer);
        log.appendAsFollower(records, epoch);

        // Check that only the first batch was appended
        assertEquals(previousEndOffset + numberOfRecords, log.endOffset().offset());
        // Check that the last fetched epoch matches the first batch
        assertEquals(epoch, log.lastFetchedEpoch());
    }

    @Test
    public void testHighWatermarkOffsetMetadata() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;
        EphemeralRaftLog log = buildMetadataLog();

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        LogOffsetMetadata highWatermarkMetadata = log.highWatermark();
        assertEquals(numberOfRecords, highWatermarkMetadata.offset());
    }

    @Test
    public void testTopicId() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        assertEquals(Uuid.METADATA_TOPIC_ID, log.topicId());
    }

    @Test
    public void testReadMissingSnapshot() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        assertEquals(Optional.empty(), log.readSnapshot(new OffsetAndEpoch(10, 0)));
    }

    @Test
    public void testReadMissingSnapshotFile() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10, 0);
        log.onSnapshotFrozen(offsetAndEpoch);
        assertEquals(Optional.empty(), log.readSnapshot(offsetAndEpoch));
    }

    @Test
    public void testDeleteNonExistentSnapshot() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        int offset = 10;
        int epoch = 0;

        append(log, offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(2L, epoch)));
        assertEquals(0, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(offset, log.endOffset().offset());
        assertEquals(offset, log.highWatermark().offset());
    }

    @Test
    public void testAppend() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        assertEquals(0L, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(Long.valueOf((long) numberOfRecords), log.endOffset().offset());
        assertEquals(0L, log.highWatermark().offset());
        
        epoch++;

        append(log, numberOfRecords, epoch);

        assertEquals(0L, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(Long.valueOf(2L * numberOfRecords), log.endOffset().offset());
        assertEquals(0L, log.highWatermark().offset());
    }

    @Test
    public void testTruncateFullyToLatestSnapshot() throws IOException {
        EphemeralRaftLog log = buildMetadataLog(false);
        int numberOfRecords = 10;
        int epoch = 0;
        OffsetAndEpoch sameEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, sameEpochSnapshotId);

        assertFalse(log.truncateToLatestSnapshot());
        assertEquals(sameEpochSnapshotId.offset(), log.startOffset());
        assertEquals(sameEpochSnapshotId.epoch(), log.lastFetchedEpoch());
        assertEquals(sameEpochSnapshotId.offset(), log.endOffset().offset());
        assertEquals(sameEpochSnapshotId.offset(), log.highWatermark().offset());

        OffsetAndEpoch greaterEpochSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch + 1);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, greaterEpochSnapshotId);

        assertFalse(log.truncateToLatestSnapshot());
        assertEquals(greaterEpochSnapshotId.offset(), log.startOffset());
        assertEquals(greaterEpochSnapshotId.epoch(), log.lastFetchedEpoch());
        assertEquals(greaterEpochSnapshotId.offset(), log.endOffset().offset());
        assertEquals(greaterEpochSnapshotId.offset(), log.highWatermark().offset());
    }

    @Test
    public void testDoesntTruncateFully() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch - 1);
        createNewSnapshotUnchecked(log, olderEpochSnapshotId);
        assertFalse(log.truncateToLatestSnapshot());

        append(log, numberOfRecords, epoch);

        OffsetAndEpoch olderOffsetSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, olderOffsetSnapshotId);

        assertFalse(log.truncateToLatestSnapshot());
    }

    @Test
    public void testCreateRaftLogTruncatesFully() throws IOException {
        EphemeralRaftLog log = buildMetadataLog(false);
        int numberOfRecords = 10;
        int epoch = 1;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords + 1, epoch + 1);

        append(log, numberOfRecords, epoch);
        createNewSnapshotUnchecked(log, snapshotId);

        log.close();

        assertTrue(log.latestSnapshotId().isEmpty());
        assertEquals(0, log.lastFetchedEpoch());
        assertEquals(0L, log.startOffset());
        assertEquals(0L, log.endOffset().offset());
        assertEquals(0L, log.highWatermark().offset());
    }

    @Test
    public void testTruncateBelowHighWatermark() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        int numRecords = 10;
        int epoch = 5;

        append(log, numRecords, epoch);
        assertEquals(numRecords, log.endOffset().offset());

        log.updateHighWatermark(new LogOffsetMetadata(numRecords));
        assertEquals(numRecords, log.highWatermark().offset());

        log.truncateTo(5L);
        assertEquals(numRecords, log.highWatermark().offset());
    }

    @Test
    public void testValidateEpochGreaterThanLastKnownEpoch() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        int numberOfRecords = 1;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch + 1);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(log.endOffset().offset(), epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateEpochLessThanOldestSnapshotEpoch() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();
        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        createNewSnapshot(log, snapshotId);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch - 1);
        // Snapshot does actually not exist
        // assertEquals(ValidOffsetAndEpoch.Kind.SNAPSHOT, resultOffsetAndEpoch.kind());
        assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind());
        // assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
        assertEquals(new OffsetAndEpoch((long) numberOfRecords, epoch - 1), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetEqualToOldestSnapshotOffset() throws IOException {
        int offset = 2;
        int epoch = 1;
        EphemeralRaftLog log = buildMetadataLog();

        append(log, offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, epoch);
        createNewSnapshot(log, snapshotId);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind());
        assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateUnknownEpochLessThanLastKnownGreaterThanOldestSnapshot() throws IOException {
        long offset = 10;
        int numOfRecords = 5;
        EphemeralRaftLog log = buildMetadataLog();

        log.updateHighWatermark(new LogOffsetMetadata(offset));
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, 1);
        createNewSnapshotUnchecked(log, snapshotId);
        log.truncateToLatestSnapshot();
        
        assertEquals(10L, log.startOffset());
        assertEquals(10L, log.endOffset().offset());

        LogAppendInfo info = append(log, numOfRecords, 1);
        assertEquals(10L, info.firstOffset());
        assertEquals(14L, info.lastOffset());
        assertEquals(new OffsetAndEpoch(15L, 1), log.endOffsetForEpoch(3));

        append(log, numOfRecords, 2);
        assertEquals(new OffsetAndEpoch(20L, 2), log.endOffsetForEpoch(2));
        
        append(log, numOfRecords, 4);
        assertEquals(new OffsetAndEpoch(20L, 2), log.endOffsetForEpoch(3));

        // offset is not equal to the oldest snapshot's offset
        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 3);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(20, 2), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateEpochLessThanFirstEpochInLog() throws IOException {
        long offset = 10;
        int numOfRecords = 5;
        EphemeralRaftLog log = buildMetadataLog();

        log.updateHighWatermark(new LogOffsetMetadata(offset));
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, 1);
        createNewSnapshotUnchecked(log, snapshotId);
        log.truncateToLatestSnapshot();

        append(log, numOfRecords, 3);

        // offset is not equal to the oldest snapshot's offset
        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 2);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(snapshotId, resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetGreatThanEndOffset() throws IOException {
        int numberOfRecords = 1;
        int epoch = 1;
        EphemeralRaftLog log = buildMetadataLog();

        append(log, numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords + 1, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(log.endOffset().offset(), epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateOffsetLessThanLEO() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();

        int numberOfRecords = 10;
        int epoch = 1;

        append(log, numberOfRecords, epoch);
        append(log, numberOfRecords, epoch + 1);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(11, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(10, epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testValidateValidEpochAndOffset() throws IOException {
        EphemeralRaftLog log = buildMetadataLog();

        int numberOfRecords = 5;
        int epoch = 1;

        append(log, numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords - 1, epoch);
        assertEquals(ValidOffsetAndEpoch.Kind.VALID, resultOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(numberOfRecords - 1, epoch), resultOffsetAndEpoch.offsetAndEpoch());
    }

    @Test
    public void testAdvanceLogStartOffsetAfterCleaning() throws IOException {
        EphemeralRaftLog log = buildMetadataLog(false);
        int numberOfRecords = 5;

        // Generate some batches
        for (int i = 0; i < 1000; i++) {
            append(log, numberOfRecords, 1);
        }
        assertFalse(log.maybeClean(), "Should not clean");

        log.updateHighWatermark(new LogOffsetMetadata(1000 * numberOfRecords));
        assertFalse(log.maybeClean(), "Should not clean");

        assertEquals(0L, log.startOffset());

        // 1 more batch
        append(log, numberOfRecords, 1);

        log.updateHighWatermark(new LogOffsetMetadata(1001 * numberOfRecords));

        assertTrue(log.maybeClean(), "Should clean");
        long lsoAfter = log.startOffset();
        
        assertEquals(lsoAfter, 1000 * numberOfRecords, "Expected the Log Start Offset to be less than or equal to the snapshot offset");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testReadRespectsMaxSizeInBytes(int expectedBatches) throws IOException {
        // 5 records are written in batches of 101 bytes each (at time of writing).
        int magicMaxBatchSizeBytes = 101;
        EphemeralRaftLog log = buildMetadataLog();
        int recordsPerBatch = 5;
        append(log, recordsPerBatch, 1);
        append(log, recordsPerBatch, 1);
        append(log, recordsPerBatch, 1);
        append(log, recordsPerBatch, 1);

        LogFetchInfo info = log.read(
            0,
            Isolation.UNCOMMITTED,
            magicMaxBatchSizeBytes * expectedBatches
        );
        assertEquals(expectedBatches * magicMaxBatchSizeBytes, info.records.sizeInBytes());
        // Asserts that we have exactly B * R records. Further there must be B batches of SimpleRecords each with a value of
        // [0..R-1] converted to an utf-8 string with empty keys and headers.
        int count = 0;
        for (Record record : info.records.records()) {
            byte[] expectedValue = String.valueOf(count % recordsPerBatch).getBytes(StandardCharsets.UTF_8);
            assertEquals(ByteBuffer.wrap(expectedValue), record.value());
            count += 1;
        }
        assertEquals(recordsPerBatch * expectedBatches, count);
    }

    @Test
    public void testLogLimitsReturnsLessThanMaxBytes() throws IOException {
        // 5 records are written in batches of 141 bytes each (at time of writing).
        // int magicMaxBatchSizeBytes = 141;
        EphemeralRaftLog log = buildMetadataLog();
        int numberOfRecordsPerBatch = 10;
        append(log, numberOfRecordsPerBatch, 5);
        append(log, numberOfRecordsPerBatch, 5);
        append(log, numberOfRecordsPerBatch, 5);
        // Set to be larger than 1 batch but smaller than 2.
        int magicMaxTotalBytes = 200;
        LogFetchInfo fetchInfo = log.read(
                0,
                Isolation.UNCOMMITTED,
                magicMaxTotalBytes
            );
        assertEquals(fetchInfo.startOffsetMetadata.offset(), 0);
        int[] nbBatches = {0};
        int[] nbRecords = {0};
        fetchInfo.records.batches().forEach(batch -> nbBatches[0]++);
        fetchInfo.records.records().forEach(rec -> nbRecords[0]++);
        assertEquals(nbBatches[0], 1);
        assertEquals(nbRecords[0], numberOfRecordsPerBatch);
    }

    @Test
    public void testLogLimitsReturnsAtLeastOne() throws IOException {
        int numberOfRecordsPerBatch = 10;
        // 5 records are written in batches of 141 bytes each (at time of writing).
        // int magicMaxBatchSizeBytes = 141;
        EphemeralRaftLog log = buildMetadataLog();
        append(log, numberOfRecordsPerBatch, 5);
        append(log, numberOfRecordsPerBatch, 5);
        // magicMaxTotalBytes are smaller than 10 simple records in a batch.
        // Meaning we will read only the first batch and not the second.
        int magicMaxTotalBytes = 1;
        Records records = log.read(
            0,
            Isolation.UNCOMMITTED,
            magicMaxTotalBytes
        ).records;
        assertTrue(
            records.sizeInBytes() > magicMaxTotalBytes,
            String.format(
                "Expected records size (%d) > maxTotalBytes (%d) since one whole batch must be returned",
                records.sizeInBytes(),
                magicMaxTotalBytes
            )
        );
        int recordCount = 0;
        var iterator = records.records().iterator();
        while (iterator.hasNext()) {
            recordCount++;
            iterator.next();
        }
        assertEquals(numberOfRecordsPerBatch, recordCount);
    }

    private static EphemeralRaftLog buildMetadataLog() {
        return EphemeralRaftLog.createLog(
                Topic.CLUSTER_METADATA_TOPIC_PARTITION,
                Uuid.METADATA_TOPIC_ID,
                1
        );
    }

    private static EphemeralRaftLog buildMetadataLog(boolean autoClean) {
        return EphemeralRaftLog.createLog(
                Topic.CLUSTER_METADATA_TOPIC_PARTITION,
                Uuid.METADATA_TOPIC_ID,
                1,
                autoClean
        );
    }

    private void createNewSnapshot(EphemeralRaftLog log, OffsetAndEpoch snapshotId) {
        Optional<RawSnapshotWriter> snapshot = log.createNewSnapshot(snapshotId);
        if (snapshot.isPresent()) {
            snapshot.get().freeze();
            snapshot.get().close();
        }
    }

    private static void createNewSnapshotUnchecked(EphemeralRaftLog log, OffsetAndEpoch snapshotId) {
        Optional<RawSnapshotWriter> snapshot = log.createNewSnapshotUnchecked(snapshotId);
        if (snapshot.isPresent()) {
            snapshot.get().freeze();
            snapshot.get().close();
        }
    }

    private static LogAppendInfo append(RaftLog log, int numberOfRecords, int epoch) {
        SimpleRecord[] records = new SimpleRecord[numberOfRecords];
        for (int i = 0; i < numberOfRecords; i++) {
            records[i] = new SimpleRecord(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        return log.appendAsFollower(
            MemoryRecords.withRecords(
                    log.endOffset().offset(),
                    Compression.NONE,
                    epoch,
                    records
            ),
            epoch
        );
    }

}
