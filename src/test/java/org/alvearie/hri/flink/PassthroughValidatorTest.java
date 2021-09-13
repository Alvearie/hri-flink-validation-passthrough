/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink;

import org.alvearie.hri.flink.core.serialization.HriRecord;
import org.alvearie.hri.flink.PassthroughValidator;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import scala.Tuple2;
import scala.runtime.RichBoolean;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/*
Example Test for a Java class that implements the Validator interface
 */
public class PassthroughValidatorTest {

    private static String DefaultTopic = "ingest.porcupine.data-int1.in";
    private static int DefaultPartition = 1;
    private static long DefaultOffset = 1234L;

    @Test
    public void isValidTest() {
        PassthroughValidator validator = new PassthroughValidator();

        RecordHeaders headers = new RecordHeaders();
        headers.add("batchId", "mybatchid".getBytes(StandardCharsets.UTF_8));

        HriRecord record = new HriRecord(headers, null, "message body".getBytes(StandardCharsets.UTF_8),
                DefaultTopic, DefaultPartition, DefaultOffset);

        Tuple2<RichBoolean, String> tuple = validator.isValid(record);
        assertTrue(tuple._1.self());
    }
}