/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.passthrough.jobtest;

import org.alvearie.hri.api.BatchNotification;
import org.alvearie.hri.flink.core.jobtest.sources.TestBatchNotification;
import org.alvearie.hri.flink.core.jobtest.sources.TestRecordHeaders;
import org.alvearie.hri.flink.core.serialization.HriRecord;
import org.alvearie.hri.flink.core.serialization.NotificationRecord;
import scala.Option;
import scala.collection.Seq;
import scala.collection.immutable.Set;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;

public class PassthroughJobTestHelper {

    private static String PassThruHeaderVal = "PassThruHeaderValue";
    private static String DefaultBatchName = "TestBatchName";
    private static String DefaultDataType = "procedure";
    private static String DefaultStartDate = "2020-04-08T03:02:23Z";
    private static String DefaultEndDate = "2020-04-11T16:02:44Z";
    private static String DefaultTopic = "ingest.porcupine.data-int1.in";
    private static int DefaultPartition = 1;
    private static long DefaultOffset = 1234L;

    static TestRecordHeaders getTestHeaders(String batchId) {
        TestRecordHeaders headers = new TestRecordHeaders();
        headers.add("batchId", batchId.getBytes(StandardCharsets.UTF_8));
        headers.add("passThru", PassThruHeaderVal.getBytes(StandardCharsets.UTF_8));
        return headers;
    }

    static Option<Seq<HriRecord>> getHriRecord(TestRecordHeaders headers, String testHriRecordKey) {
        HriRecord record = new HriRecord(headers, testHriRecordKey.getBytes(),
                "message body".getBytes(StandardCharsets.UTF_8), DefaultTopic, DefaultPartition, DefaultOffset);
        return toScalaOptionSeq(new HriRecord[]{record});
    }

    static NotificationRecord[] getStartCompleteNotifications(TestRecordHeaders testHeaders, String testNotificationKey,
                                                              String testBatchId, int expectedRecCount,
                                                              int testInvalidThreshold) {
        ArrayList<NotificationRecord> recordsList = new ArrayList(0);
        BatchNotification.Status[] statuses = {BatchNotification.Status.STARTED, BatchNotification.Status.SEND_COMPLETED};
        for (BatchNotification.Status status: statuses) {
            NotificationRecord record = getTestNotification(
                    testHeaders, testNotificationKey.getBytes(), testBatchId, expectedRecCount, status, testInvalidThreshold);
            recordsList.add(record);
        }

        NotificationRecord[] records = new NotificationRecord[recordsList.size()];
        records = recordsList.toArray(records);
        return records;
    }

    private static NotificationRecord getTestNotification(TestRecordHeaders headers, byte[] key,
            String batchId, int expectedRecCount, BatchNotification.Status status, int invalidThreshold) {
        TestBatchNotification batchNotification = (TestBatchNotification) new TestBatchNotification()
                .withId(batchId)
                .withName(DefaultBatchName)
                .withStatus(status)
                .withDataType(DefaultDataType)
                .withStartDate(OffsetDateTime.parse(DefaultStartDate))
                .withEndDate(OffsetDateTime.parse(DefaultEndDate))
                .withExpectedRecordCount(expectedRecCount)
                .withTopic(DefaultTopic)
                .withInvalidThreshold(invalidThreshold);
        return new NotificationRecord(headers, key, batchNotification);
    }

    public static <A> Option<Seq<A>> toScalaOptionSeq(A[] a) {
        Seq<A> sequence;
        if (a.length == 1) {
            sequence = new Set.Set1<>(a[0]).toSeq();
        } else if (a.length == 2) {
            sequence = new Set.Set2<>(a[0], a[1]).toSeq();
        } else {
            return null;
        }
        Option<Seq<A>> wrappedA = Option.apply(sequence);
        return wrappedA;
    }
}
