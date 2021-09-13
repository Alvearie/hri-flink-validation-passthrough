/**
 * (C) Copyright IBM Corp. 2021
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.alvearie.hri.flink;

import org.alvearie.hri.flink.core.Validator;
import org.alvearie.hri.flink.core.serialization.HriRecord;
import scala.Tuple2;
import scala.runtime.RichBoolean;

/*
Example Java class that implements the Validator interface. This is a 'passthrough' validator that does not
inspect the contents of the record and for all records, responds that they are valid.
 */
public class PassthroughValidator implements Validator {
    @Override
    public Tuple2<RichBoolean, String> isValid(HriRecord record) {
        // The first tuple value indicates if the record is valid
        // The second tuple value is the error message and is only used when the first value is 'false'.
        return Tuple2.apply(new RichBoolean(true), null);
    }
}
