package com.dorisdb.connection;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * JDBC connection options.
 */
@PublicEvolving
public class DorisJdbcConnectionOptions  implements Serializable {
	
	private static final long serialVersionUID = 1L;

	protected final String url;
	protected final String driverName;
	@Nullable
	protected final String username;
	@Nullable
	protected final String password;

	public DorisJdbcConnectionOptions(String url, String username, String password) {
		this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
		this.driverName = "com.mysql.jdbc.Driver";
		this.username = username;
		this.password = password;
	}

	public String getDbURL() {
		return url;
	}

	public String getDriverName() {
		return driverName;
	}

	public Optional<String> getUsername() {
		return Optional.ofNullable(username);
	}

	public Optional<String> getPassword() {
		return Optional.ofNullable(password);
	}
}
