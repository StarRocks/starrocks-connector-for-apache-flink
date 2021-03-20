/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dorisdb.connector.flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.dorisdb.connector.flink.DorisSinkBaseTest;
import com.dorisdb.connector.flink.manager.DorisStreamLoadVisitor;

public class DorisStreamLoadVisitorTest extends DorisSinkBaseTest {

    @Test
    public void testNoAvailableHost() throws IOException {
        stopHttpServer();
        DorisStreamLoadVisitor visitor = new DorisStreamLoadVisitor(OPTIONS, TABLE_SCHEMA.getFieldNames());
        // test failed
        String exMsg = "";
        try {
            visitor.doStreamLoad(new Tuple3<>(mockFailedResponse(), 0l, Lists.newArrayList("aaaa")));
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertEquals(0, exMsg.indexOf("None of the host"));
    }

    @Test
    public void testDoStreamLoad() throws IOException {
        DorisStreamLoadVisitor visitor = new DorisStreamLoadVisitor(OPTIONS, TABLE_SCHEMA.getFieldNames());
        // test failed
        String exMsg = "";
        try {
            visitor.doStreamLoad(new Tuple3<>(mockFailedResponse(), 0l, Lists.newArrayList("aaaa")));
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertTrue(0 < exMsg.length());
        // test suucess
        exMsg = "";
        try {
            visitor.doStreamLoad(new Tuple3<>(mockSuccessResponse(), 0l, Lists.newArrayList("aaaa")));
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertEquals(0, exMsg.length());
    }
}
