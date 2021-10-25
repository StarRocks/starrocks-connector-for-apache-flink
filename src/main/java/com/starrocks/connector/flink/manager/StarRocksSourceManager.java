package com.starrocks.connector.flink.manager;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.related.QueryBeXTablets;
import com.starrocks.connector.flink.related.QueryInfo;
import com.starrocks.connector.flink.related.QueryPlan;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;



public class StarRocksSourceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkManager.class);

    private final StarRocksSourceOptions sourceOptions;


    public StarRocksSourceManager(StarRocksSourceOptions sourceOptions) {

        this.sourceOptions = sourceOptions;
    }

    public QueryInfo getQueryInfo() throws IOException, HttpException {

        String columns = sourceOptions.getColums() == null ? "*" : sourceOptions.getColums();
        String filter = sourceOptions.getFilter() == null ? "" : " where " + sourceOptions.getFilter();
        String querySQL = "select " + columns + " from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName() + filter;
        LOG.info("query sql [{}]", querySQL);
        String[] httpNodes = sourceOptions.getHttpNodes().split(",");

        QueryPlan plan = getQueryPlan(querySQL, httpNodes[new Random().nextInt(httpNodes.length)], sourceOptions);
        Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(plan);
        List<QueryBeXTablets> queryBeXTabletsList = new ArrayList<>();
        beXTablets.forEach((be, tablets) -> {
            QueryBeXTablets queryBeXTablets = new QueryBeXTablets(be, new ArrayList<>(tablets));
            queryBeXTabletsList.add(queryBeXTablets);
        });
        return new QueryInfo(plan, queryBeXTabletsList);
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

    private static QueryPlan getQueryPlan(String querySQL, String httpNode, StarRocksSourceOptions sourceOptions) throws IOException, HttpException {
        
        String url = "http://" + httpNode + "/api/" + sourceOptions.getDatabaseName() + "/" + sourceOptions.getTableName() + "/_query_plan";
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = new JSONObject(bodyMap).toString();

        try(CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.setHeader("Content-Type", "application/json;charset=UTF-8");
            post.setHeader("Authorization", getBasicAuthHeader(sourceOptions.getUsername(), sourceOptions.getPassword()));
            post.setEntity(new ByteArrayEntity(body.getBytes()));
            try(CloseableHttpResponse response = httpClient.execute(post)) {
                int code = response.getStatusLine().getStatusCode();
                if (200 != code) {
                    LOG.warn("Request failed with code:{}", code);
                    return null;
                }
                HttpEntity respEntity = response.getEntity();
                if (null == respEntity) {
                    LOG.warn("Request failed with empty response.");
                    return null;
                }
                JSONObject jsonObject = JSONObject.parseObject(EntityUtils.toString(respEntity));
                return JSONObject.toJavaObject(jsonObject, QueryPlan.class);
            }
        }
    }

    private static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }
}
