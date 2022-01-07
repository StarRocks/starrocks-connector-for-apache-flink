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

package com.starrocks.connector.flink.manager;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;

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



public class StarRocksQueryPlanVisitor implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksQueryPlanVisitor.class);

    private final StarRocksSourceOptions sourceOptions;


    public StarRocksQueryPlanVisitor(StarRocksSourceOptions sourceOptions) {

        this.sourceOptions = sourceOptions;
    }

    public QueryInfo getQueryInfo(String SQL) throws IOException {
        
        LOG.info("query sql [{}]", SQL);
        String[] httpNodes = sourceOptions.getScanUrl().split(",");
        QueryPlan plan = getQueryPlan(SQL, httpNodes[new Random().nextInt(httpNodes.length)], sourceOptions);
        Map<String, Set<Long>> beXTablets = transferQueryPlanToBeXTablet(plan);
        List<QueryBeXTablets> queryBeXTabletsList = new ArrayList<>();
        beXTablets.entrySet().stream().forEach(entry -> {
            QueryBeXTablets queryBeXTablets = new QueryBeXTablets(entry.getKey(), new ArrayList<>(entry.getValue()));
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
                } 
                if (beXTablets.get(beNode).size() < tabletCount) {
                    candidateBe = beNode;
                    tabletCount = beXTablets.get(beNode).size();
                }
            }
            beXTablets.get(candidateBe).add(Long.valueOf(tabletId));
        });
        return beXTablets;
    }

    private static QueryPlan getQueryPlan(String querySQL, String httpNode, StarRocksSourceOptions sourceOptions) throws IOException {
        
        String url = new StringBuilder("http://")
            .append(httpNode)
            .append("/api/")
            .append(sourceOptions.getDatabaseName())
            .append("/")
            .append(sourceOptions.getTableName())
            .append("/_query_plan")
            .toString();

        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = new JSONObject(bodyMap).toString();
        int requsetCode = 0;
        String respString = "";
        for (int i = 0; i < sourceOptions.getScanMaxRetries(); i ++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json;charset=UTF-8");
                post.setHeader("Authorization", getBasicAuthHeader(sourceOptions.getUsername(), sourceOptions.getPassword()));
                post.setEntity(new ByteArrayEntity(body.getBytes()));
                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    requsetCode = response.getStatusLine().getStatusCode();
                    HttpEntity respEntity = response.getEntity();
                    respString = EntityUtils.toString(respEntity, "UTF-8");
                }
            }
            if (200 == requsetCode || i == sourceOptions.getScanMaxRetries() - 1) {
                break;
            }
            LOG.warn("Request failed with code:{}", requsetCode);
            try {
                Thread.sleep(1000l * (i + 1));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Unable to get query plan, interrupted while doing another attempt", ex);
            }
        }
        if (200 != requsetCode) {
            throw new RuntimeException("Request of get queryPlan failed with code " + requsetCode + " " + respString);
        }
        if (respString == "") {
            LOG.warn("Request failed with empty response.");
            throw new RuntimeException("Request failed with empty response." + requsetCode);
        }
        JSONObject jsonObject = JSONObject.parseObject(respString);
        return JSONObject.toJavaObject(jsonObject, QueryPlan.class);
    }

    private static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }
}
