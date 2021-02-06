package com.dorisdb.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.dorisdb.DorisSinkBaseTest;
import com.dorisdb.manager.DorisStreamLoadVisitor;

public class DorisStreamLoadVisitorTest extends DorisSinkBaseTest {

	@Test
	public void testNoAvailableHost() throws IOException {
		stopHttpServer();
		DorisStreamLoadVisitor visitor = new DorisStreamLoadVisitor(OPTIONS);
		// test failed
		String exMsg = "";
		try {
			visitor.doStreamLoad(new Tuple2<>(mockFailedResponse(), Lists.newArrayList("aaaa")));
		} catch (Exception e) {
			exMsg = e.getLocalizedMessage();
		}
		assertEquals(0, exMsg.indexOf("None of the host"));
	}

	@Test
	public void testDoStreamLoad() throws IOException {
		DorisStreamLoadVisitor visitor = new DorisStreamLoadVisitor(OPTIONS);
		// test failed
		String exMsg = "";
		try {
			visitor.doStreamLoad(new Tuple2<>(mockFailedResponse(), Lists.newArrayList("aaaa")));
		} catch (Exception e) {
			exMsg = e.getLocalizedMessage();
		}
		assertTrue(0 < exMsg.length());
		// test suucess
		exMsg = "";
		try {
			visitor.doStreamLoad(new Tuple2<>(mockSuccessResponse(), Lists.newArrayList("aaaa")));
		} catch (Exception e) {
			exMsg = e.getLocalizedMessage();
		}
		assertEquals(0, exMsg.length());
	}
}
