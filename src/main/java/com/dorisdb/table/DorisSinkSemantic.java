package com.dorisdb.table;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.annotation.Internal;

/**
 * Doris sink semantic Enum.
 * */
@Internal
public enum DorisSinkSemantic {
	EXACTLY_ONCE("exactly-once"),
	AT_LEAST_ONCE("at-least-once");

	private String name;

	private DorisSinkSemantic(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public static DorisSinkSemantic fromName(String n) {
		List<DorisSinkSemantic> rs = Arrays.stream(DorisSinkSemantic.values()).filter(v -> v.getName().equals(n)).collect(Collectors.toList());
		return rs.isEmpty() ? null : rs.get(0);
	}
}
