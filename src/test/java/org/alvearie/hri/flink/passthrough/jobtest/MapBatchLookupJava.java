/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink.passthrough.jobtest;

import org.alvearie.hri.api.BatchLookup;
import org.alvearie.hri.api.BatchNotification;
import org.alvearie.hri.api.RequestException;
import org.alvearie.hri.flink.core.serialization.NotificationRecord;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import java.util.HashMap;

public class MapBatchLookupJava implements BatchLookup, Serializable {

    HashMap batchNotificationHashMap = new HashMap<String, BatchNotification>();
    private Logger log = LoggerFactory.getLogger(this.getClass());

    MapBatchLookupJava(NotificationRecord[] batchNotificationArr) {
        for (NotificationRecord n : batchNotificationArr) {
            batchNotificationHashMap.put(n.value().getId(), n.value());
        }
    }

    @Override
    public Try<BatchNotification> getBatchId(String tenantId, String batchId) {
        BatchNotification notification = (BatchNotification) batchNotificationHashMap.get(batchId);
        if (notification != null) {
            log.info("found batch " + batchId);
            return new Success(notification);
        } else {
            log.info("did not find batch " + batchId);
            return new Failure(new RequestException("Not found", HttpStatus.SC_NOT_FOUND));
        }
    }
}
