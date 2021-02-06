package com.dorisdb;

import com.dorisdb.manager.DorisQueryVisitor;
import com.dorisdb.table.DorisSinkOptions;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.junit.Test;

import mockit.Mocked;

import static org.junit.Assert.assertFalse;

import java.io.Serializable;

public class DorisGenericSinkITTest extends DorisSinkBaseTest {

	@Mocked
	private transient DorisQueryVisitor v;

	class TestEntry implements Serializable {

		private static final long serialVersionUID = 1L;

		public final Integer score;
		public final String name;

		public TestEntry(Integer score, String name) {
			this.score = score;
			this.name = name;
		}
	}

	private final TestEntry[] TEST_DATA = {
		new TestEntry(99, "lebron"),
		new TestEntry(99, "stephen")
	};

	@Test
	public void testBatchSink() {
		mockSuccessResponse();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.setParallelism(1);
		env.fromElements(TEST_DATA)
			.addSink(DorisSink.sink(
				TableSchema.builder()
					.field("score", DataTypes.INT())
					.field("name", DataTypes.VARCHAR(20))
					.build(),
				DorisSinkOptions.builder()
					.withProperty("jdbc-url", "jdbc:mysql://ip:port,ip:port?xxxxx")
					.withProperty("load-url", "ip:port;ip:port")
					.withProperty("username", "xxx")
					.withProperty("password", "xxx")
					.withProperty("table-name", "xxx")
					.withProperty("database-name", "xxx")
					.build(),
				(slots, te) -> {
					slots[0] = te.score;
					slots[1] = te.name;
				}));

		String exMsg = "";
		try {
			env.execute();
		} catch (Exception e) {
			exMsg = e.getMessage();
		}
		assertFalse(exMsg, exMsg.length() > 0);
	}
}
