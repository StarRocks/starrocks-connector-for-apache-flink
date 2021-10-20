package com.starrocks.connector.flink.manager;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.related.QueryPlan;
import com.starrocks.connector.flink.related.Tablet;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.*;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class StarRocksSourceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkManager.class);

    private final StarRocksSourceOptions sourceOptions;

    private final StarRocksJdbcConnectionProvider jdbcConnProvider;

    private LinkedBlockingDeque blockingDeque;

    public StarRocksSourceReader sourceReader;

    public StarRocksSourceManager(StarRocksSourceOptions sourceOptions) {

        this.sourceOptions = sourceOptions;
        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(sourceOptions.getJdbcUrl(), sourceOptions.getUsername(), sourceOptions.getPassword());
        this.jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
    }

    public void startToRead() throws SQLException, ClassNotFoundException, IOException, HttpException {

        String columns = sourceOptions.getColums() == null ? "*" : sourceOptions.getColums();
        String filter = sourceOptions.getFilter() == null ? "" : " where " + sourceOptions.getFilter();
        String querySQL = "select " + columns + " from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName() + filter;
        LOG.info("query sql [{}]", querySQL);
        String[] httpNodes = sourceOptions.getHttpNodes().split(",");

        QueryPlan plan = getQueryPlan(querySQL,
                sourceOptions.getDatabaseName(),
                sourceOptions.getTableName(),
                httpNodes[new Random().nextInt(httpNodes.length)]);
        Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(plan);
        sourceReader = new StarRocksSourceReader(beXTablets, plan.getOpaqued_query_plan());
        sourceReader.startRead();
    }

    private static Map<String, Set<Long>> transferQueryPlanToBeXTablet(QueryPlan queryPlan) {
        Map<String, Set<Long>> beXTablets = new HashMap<>();
        queryPlan.getPartitions().forEach((tabletId, routingList) -> {
            int tabletCount = Integer.MAX_VALUE;
            String candidateBe = "";
            for (String beNode : routingList.getRoutings()) {
                if (!beXTablets.containsKey(beNode)) {
                    beXTablets.put(beNode, new HashSet<>());
                    candidateBe = beNode;
                    break;
                } else {
                    if (beXTablets.get(beNode).size() < tabletCount) {
                        candidateBe = beNode;
                        tabletCount = beXTablets.get(beNode).size();
                    }
                }
            }
            beXTablets.get(candidateBe).add(Long.valueOf(tabletId));
        });
        return beXTablets;
    }

    private static QueryPlan getQueryPlan(String querySQL, String dbName, String tableName, String httpNode) throws IOException, HttpException {

        OkHttpClient client = new OkHttpClient.Builder().authenticator((route, response) -> {
            String credential = Credentials.basic("root", "");
            return response.request().newBuilder().header("Authorization", credential).build();
        }).build();

        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        MediaType JSON = MediaType.parse("application/json;charset=utf-8");
        RequestBody requestBody = RequestBody.create(JSON, new JSONObject(bodyMap).toString());
        String url = "http://" + httpNode + "/api/" + dbName + "/" + tableName + "/_query_plan";
        Request request = new Request.Builder().url(url).post(requestBody).build();
        Response response = client.newCall(request).execute();
        String responseStr = Objects.requireNonNull(response.body()).string();
        Map<String, Object> responseMap =  JSONObject.parseObject(responseStr);
        String status = responseMap.get("status").toString();
        if (!status.equals("200")) {
            throw new HttpException(responseStr);
        }

        JSONObject jsonObject = JSONObject.parseObject(responseStr);
        QueryPlan queryPlan = JSONObject.toJavaObject(jsonObject, QueryPlan.class);
        return queryPlan;
    }
}
