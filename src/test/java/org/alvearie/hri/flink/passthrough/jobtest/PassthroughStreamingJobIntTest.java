/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.passthrough.jobtest;

import org.alvearie.hri.api.BatchNotification;
import org.alvearie.hri.flink.PassthroughStreamingJob;
import org.alvearie.hri.flink.core.jobtest.sources.HriTestRecsSourceFunction;
import org.alvearie.hri.flink.core.jobtest.sources.NotificationSourceFunction;
import org.alvearie.hri.flink.core.jobtest.sources.TestRecordHeaders;
import org.alvearie.hri.flink.core.serialization.HriRecord;
import org.alvearie.hri.flink.core.serialization.NotificationRecord;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.test.streaming.runtime.util.TestListResultSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.common.header.Header;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import scala.Option;
import scala.collection.Seq;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PassthroughStreamingJobIntTest {
    private static int SlotsPerTaskMgr = 2;
    private static int TestJobParallelism = 2;
    private static int DefaultTestInvalidThreshold = 5;

    private static String TestBatchId = "batch-42";
    private static String TestNotificationKey = "testNotification01";
    private static String TestHriRecordKey = "hriRec01";

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(SlotsPerTaskMgr)
                .setNumberTaskManagers(1)
                .build());

    @Before
    public void clusterBefore() throws Exception {
        flinkCluster.before();
    }

    @After
    public void clusterAfter() {
        flinkCluster.after();
    }

    @Test
    /**
     * End to End Pipeline Test.  First sent a notification with a status of "started".  Pause, then send one valid
     * HriRecord.  Pause again after NotificationSourceFunction.run(), then send a notification with status
     * "sendCompleted".
     */
    public void endToEndPipelineTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(TestJobParallelism);

        // Create Test Sinks to send to Job
        TestListResultSink validRecsSink = new TestListResultSink<HriRecord>();
        TestListResultSink invalidRecsSink = new TestListResultSink<HriRecord>();
        TestListResultSink countRecsSink = new TestListResultSink<HriRecord>();

        int numRecordsToSend = 1;
        int expectedRecordCount = numRecordsToSend * SlotsPerTaskMgr;

        TestRecordHeaders testHeaders = PassthroughJobTestHelper.getTestHeaders(TestBatchId);
        NotificationRecord[] testNotifications = PassthroughJobTestHelper.getStartCompleteNotifications(
                testHeaders, TestNotificationKey, TestBatchId, expectedRecordCount, DefaultTestInvalidThreshold);
        Option<Seq<NotificationRecord>> testNotificationsOptSeq = PassthroughJobTestHelper.toScalaOptionSeq(testNotifications);

        MapBatchLookupJava batchLookup = new MapBatchLookupJava(testNotifications);

        NotificationSourceFunction notificationSrcFunc = new NotificationSourceFunction(testNotificationsOptSeq, 400, 0);
        Option<Seq<HriRecord>> hriRecord = PassthroughJobTestHelper.getHriRecord(testHeaders, TestHriRecordKey);
        HriTestRecsSourceFunction testRecsSrcFunc = new HriTestRecsSourceFunction(hriRecord, 200, 1000);

        PassthroughStreamingJob validationJob = new PassthroughStreamingJob(
                notificationSrcFunc, testRecsSrcFunc, validRecsSink, invalidRecsSink, countRecsSink, batchLookup, 100L);
        validationJob.call();

        List<HriRecord> validRecsList = validRecsSink.getResult();
        verifyDefaultValidRecOutput(testHeaders, TestHriRecordKey, validRecsList, expectedRecordCount);

        // Verify recCount is updated
        List<NotificationRecord> countResultList = countRecsSink.getResult();
        assertEquals(countResultList.size(), 1);
        BatchNotification recCountNotificationVal = countResultList.get(0).value();
        assertEquals(Optional.ofNullable(recCountNotificationVal.getExpectedRecordCount()), Optional.of(expectedRecordCount));
        assertTrue(recCountNotificationVal.getStatus().equals(BatchNotification.Status.COMPLETED));
        assertTrue(recCountNotificationVal.getId().equals(TestBatchId));

        // Confirm there are no invalid records
        List<HriRecord> invalidRecsList = invalidRecsSink.getResult();
        assertEquals(invalidRecsList.size(), 0);
    }

    @Test
    /**
     * Test handling a SendCompleted notification with NoOp when no HriRecords are received/processed.  First, sent a
     * notification with its status set to "started".  Pause, then send a notification record with a status set to
     * "sendComplete".  Verify there are 0 records in valid HRI Recs output, invalid HRI Recs output, and notification
     * Recs output.
     */
    public void noSentRecordsTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(TestJobParallelism);

        // Create Test Sinks to send to Job
        TestListResultSink validRecsSink = new TestListResultSink<HriRecord>();
        TestListResultSink invalidRecsSink = new TestListResultSink<HriRecord>();
        TestListResultSink countRecsSink = new TestListResultSink<HriRecord>();

        int expectedRecordCount = 1;
        // We must have an expectedRecordCount to send as part of the batchNotification message, but we do not actually
        // send a record.

        TestRecordHeaders testHeaders = PassthroughJobTestHelper.getTestHeaders(TestBatchId);
        NotificationRecord[] testNotifications = PassthroughJobTestHelper.getStartCompleteNotifications(
                testHeaders, TestNotificationKey, TestBatchId, expectedRecordCount, DefaultTestInvalidThreshold);
        Option<Seq<NotificationRecord>> testNotificationsOptSeq = PassthroughJobTestHelper.toScalaOptionSeq(testNotifications);

        MapBatchLookupJava batchLookup = new MapBatchLookupJava(testNotifications);

        NotificationSourceFunction notificationSrcFunc = new NotificationSourceFunction(testNotificationsOptSeq, 200, 0);
        Option hriRecord = scala.None$.MODULE$;
        HriTestRecsSourceFunction testRecsSrcFunc = new HriTestRecsSourceFunction(hriRecord, 200, 1000);

        PassthroughStreamingJob validationJob = new PassthroughStreamingJob(
                notificationSrcFunc, testRecsSrcFunc, validRecsSink, invalidRecsSink, countRecsSink, batchLookup, 100L);
        validationJob.call();

        List<HriRecord> validRecsList = validRecsSink.getResult();
        verifyDefaultValidRecOutput(testHeaders, TestHriRecordKey, validRecsList, 0);

        // Verify recCount indicates no messages
        List<NotificationRecord> countResultList = countRecsSink.getResult();
        assertEquals(countResultList.size(), 0);

        // Confirm there are no invalid records
        List<HriRecord> invalidRecsList = invalidRecsSink.getResult();
        assertEquals(invalidRecsList.size(), 0);
    }

    static void verifyDefaultValidRecOutput(TestRecordHeaders testHeaders, String testHriRecordKey, List<HriRecord> validRecsList, int totalRecsExpectedCount) {
        assertTrue("The size of the recs was " + validRecsList.size() + " and not " + totalRecsExpectedCount, validRecsList.size() == totalRecsExpectedCount);
        if (totalRecsExpectedCount == 0) return;
        String outputKey = new String(validRecsList.get(0).key(), StandardCharsets.UTF_8);
        assertTrue(outputKey.equals(testHriRecordKey));
        Header[] headersArr = validRecsList.get(0).headers().toArray();
        assertTrue(defaultHeadersDoMatch(testHeaders.toArray().length, headersArr));
    }

    static boolean defaultHeadersDoMatch(int expectedHeaderLen, Header[] headers) {
        int headerLen = headers.length;
        return (headerLen == expectedHeaderLen && headers[headerLen - 1].key().equals("passThru"));
    }
}
