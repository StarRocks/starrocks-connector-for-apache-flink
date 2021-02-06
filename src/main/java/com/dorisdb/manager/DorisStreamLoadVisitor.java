package com.dorisdb.manager;


import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import com.dorisdb.table.DorisSinkOptions;
import com.alibaba.fastjson.JSON;

import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

 
public class DorisStreamLoadVisitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadVisitor.class);

	private final DorisSinkOptions sinkOptions;
	private int pos;

    public DorisStreamLoadVisitor(DorisSinkOptions sinkOptions) {
		this.sinkOptions = sinkOptions;
	}

	public void doStreamLoad(Tuple2<String, List<String>> labeledJson) throws IOException {
		String host = getAvailableHost();
		if (null == host) {
			throw new IOException("None of the host in `load_url` could be connected.");
		}
		Map<String, Object> loadResult = doHttpPut(host, labeledJson);
		final String keyStatus = "Status";
		if (null == loadResult || !loadResult.containsKey(keyStatus)) {
			throw new IOException("Unable to flush data to doris: unknown result status.");
		}
		if (loadResult.get(keyStatus).equals("Fail")) {
			throw new IOException(
				new StringBuilder("Failed to flush data to doris.").append(loadResult.get("Message").toString()).toString()
			);
		}
	}

	private String getAvailableHost() {
		List<String> hostList = sinkOptions.getLoadUrlList();
		if (pos >= hostList.size()) {
			pos = 0;
		}
		for (; pos < hostList.size(); pos++) {
			String host = new StringBuilder("http://").append(hostList.get(pos)).toString();
			if (tryHttpConnection(host)) {
				return host;
			}
		}
		return null;
	}

	private boolean tryHttpConnection(String host) {
		try {  
			URL url = new URL(host);
			HttpURLConnection co =  (HttpURLConnection) url.openConnection();
			co.setConnectTimeout(100);
			co.connect();
			co.disconnect();
			return true;
		} catch (Exception e1) {
			LOG.warn("Failed to connect to address:{}", host, e1);
			return false;
		}
	}

    @SuppressWarnings("unchecked")
    private Map<String, Object> doHttpPut(String host, Tuple2<String, List<String>> labeledJson) throws IOException {
		URL url = null;
		HttpURLConnection httpurlconnection = null;
		HttpURLConnection.setFollowRedirects(true);
		StringBuilder loadUrl = new StringBuilder(host)
			.append("/api/")
			.append(sinkOptions.getDatabaseName())
			.append("/")
			.append(sinkOptions.getTableName())
			.append("/_stream_load");
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Executing stream load to: '%s', rows: '%s'", loadUrl.toString(), labeledJson.f1.size()));
		}
		try {
			url = new URL(loadUrl.toString());
			httpurlconnection = (HttpURLConnection) url.openConnection();
			httpurlconnection.setDoInput(true);
			httpurlconnection.setDoOutput(true);
			httpurlconnection.setConnectTimeout(1000);
			httpurlconnection.setReadTimeout(300000);
			httpurlconnection.setRequestMethod("PUT");
			Map<String, String> props = sinkOptions.getSinkStreamLoadProperties();
			for (Map.Entry<String,String> entry : props.entrySet()) {
				httpurlconnection.setRequestProperty(entry.getKey(), entry.getValue());
			}
			httpurlconnection.setRequestProperty("label", "");
			httpurlconnection.setRequestProperty("Expect", "100-continue");
			httpurlconnection.setRequestProperty("strip_outer_array", "true");
			httpurlconnection.setRequestProperty("label", labeledJson.f0);
			httpurlconnection.setRequestProperty("Authorization", getBasicAuthHeader(sinkOptions.getUsername(), sinkOptions.getPassword()));
			httpurlconnection.getOutputStream().write(new StringBuilder("[").append(String.join(",", labeledJson.f1)).append("]").toString().getBytes(StandardCharsets.UTF_8));
			httpurlconnection.getOutputStream().flush();
			httpurlconnection.getOutputStream().close();
			int code = httpurlconnection.getResponseCode();
 
			if (200 != code) {
				LOG.warn("Request failed with code:{}", code);
				return null;
			}
			try(DataInputStream in = new DataInputStream(httpurlconnection.getInputStream())) {
				int len = in.available();
				byte[] by = new byte[len];
				in.readFully(by);
				String result = new String(by);
				return (Map<String, Object>)JSON.parse(result);
			}
		} catch (MalformedURLException e) {
			LOG.warn("Unable to parse url:{}", loadUrl.toString(), e);
		} catch (ParseException e) {
            throw new IOException("Unable to convert stream load result to string.", e);
        } catch (IOException e) {
            throw e;
        } finally {
			url = null;
			if (httpurlconnection != null) {
				httpurlconnection.disconnect();
			}
		}
		return null;
	}
	
	private String getBasicAuthHeader(String username, String password) {
		String auth = username + ":" + password;
		byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.US_ASCII));
		return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
	}

}
