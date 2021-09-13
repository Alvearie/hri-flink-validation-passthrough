/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.passthrough.jobtest;

import org.alvearie.hri.api.BatchNotification;
import org.alvearie.hri.flink.core.jobtest.sources.TestRecordHeaders;
import org.alvearie.hri.flink.core.serialization.NotificationRecord;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.junit.Test;
import scala.util.Try;

import static org.junit.Assert.assertTrue;

public class MapBatchLookupJavaTest {
    private static int TestJobParallelism = 2;
    private static String TestBatchId = "batch-42";
    private static String InvalidTestBatchId = "batch-fake";
    private static String TestNotificationKey = "testNotification01";

    @Test
    /** Test class MapBatchLookupJava to ensure it returns success for existing test batchId's. */
    public void testValidMapBatchLookupJava() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(TestJobParallelism);

        int numRecordsToSend = 1;
        TestRecordHeaders testHeaders = PassthroughJobTestHelper.getTestHeaders(TestBatchId);

        NotificationRecord[] testNotifications = PassthroughJobTestHelper.getStartCompleteNotifications(
                testHeaders, TestNotificationKey, TestBatchId, numRecordsToSend, 5);
        MapBatchLookupJava batchLookup = new MapBatchLookupJava(testNotifications);

        Try<BatchNotification> result = batchLookup.getBatchId("", TestBatchId);
        assertTrue(result.isSuccess());
    }

    @Test
    /** Test class MapBatchLookupJava to ensure it returns failure for nonexistent test batchId's. */
    public void testInvalidMapBatchLookupJava () {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(TestJobParallelism);

        int numRecordsToSend = 1;
        TestRecordHeaders testHeaders = PassthroughJobTestHelper.getTestHeaders(TestBatchId);

        NotificationRecord[] testNotifications = PassthroughJobTestHelper.getStartCompleteNotifications(
                testHeaders, TestNotificationKey, TestBatchId, numRecordsToSend, 5);
        MapBatchLookupJava batchLookup = new MapBatchLookupJava(testNotifications);

        Try<BatchNotification> result = batchLookup.getBatchId("", InvalidTestBatchId);
        assertTrue(result.isFailure());
    }
}
