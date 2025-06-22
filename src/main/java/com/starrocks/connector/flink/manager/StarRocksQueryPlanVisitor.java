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
import com.starrocks.connector.flink.tools.JsonWrapper;
import com.starrocks.streamload.shade.org.apache.http.HttpEntity;
import com.starrocks.streamload.shade.org.apache.http.client.methods.CloseableHttpResponse;
import com.starrocks.streamload.shade.org.apache.http.client.methods.HttpPost;
import com.starrocks.streamload.shade.org.apache.http.entity.ByteArrayEntity;
import com.starrocks.streamload.shade.org.apache.http.impl.client.CloseableHttpClient;
import com.starrocks.streamload.shade.org.apache.http.impl.client.HttpClients;
import com.starrocks.streamload.shade.org.apache.http.util.EntityUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


public class StarRocksQueryPlanVisitor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksQueryPlanVisitor.class);
    private StarRocksSourceOptions sourceOptions;

    public StarRocksQueryPlanVisitor(StarRocksSourceOptions sourceOptions) {
        this.sourceOptions = sourceOptions;
    }

    public void setSourceOptions(StarRocksSourceOptions sourceOptions) {
        this.sourceOptions = sourceOptions;
    }

    public QueryInfo getQueryInfo(String SQL) throws IOException {
        LOG.info("query sql [{}]", SQL);
        QueryPlan plan = getQueryPlan(SQL, sourceOptions);
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

    private static QueryPlan getQueryPlan(String querySQL, StarRocksSourceOptions sourceOptions) throws IOException {

        List<String> httpNodes = Arrays.asList(sourceOptions.getScanUrl().split(","));
        // shuffle scan urls to ensure support for both random fe node selection and high availability
        Collections.shuffle(httpNodes);
        int requestCode = 0;
        String respString = "";
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("sql", querySQL);
        String body = new JSONObject(bodyMap).toString();
        for (String httpNode : httpNodes) {
            respString = executeOnNodeWithMaxRetries(constructUrl(httpNode, sourceOptions),
                    body,
                    sourceOptions
            );
            if (StringUtils.isNotEmpty(respString)) {
                break;
            }
            LOG.warn("Request to get query plan failed on node: {}", httpNode);
        }
        if (respString.isEmpty()) {
            LOG.warn("Request failed with empty response.");
            throw new RuntimeException("Request failed with empty response." + requestCode);
        }
        return new JsonWrapper().parseObject(respString, QueryPlan.class);
    }

    private static String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return new StringBuilder("Basic ").append(new String(encodedAuth)).toString();
    }

    private static String constructUrl(String httpNode, StarRocksSourceOptions sourceOptions) {
        return new StringBuilder("http://")
                .append(httpNode)
                .append("/api/")
                .append(sourceOptions.getDatabaseName())
                .append("/")
                .append(sourceOptions.getTableName())
                .append("/_query_plan")
                .toString();
    }

    private static String executeOnNodeWithMaxRetries(String url, String body,
                                                      StarRocksSourceOptions sourceOptions) throws IOException {

        String responseString = "";
        for (int attempt = 0; attempt < sourceOptions.getScanMaxRetries(); attempt++) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json;charset=UTF-8");
                post.setHeader("Authorization", getBasicAuthHeader(sourceOptions.getUsername(), sourceOptions.getPassword()));
                post.setEntity(new ByteArrayEntity(body.getBytes()));
                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    int requestCode = response.getStatusLine().getStatusCode();
                    responseString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    if (200 == requestCode || attempt == sourceOptions.getScanMaxRetries() - 1) {
                        break;
                    }
                    LOG.warn("Request to get query plan failed with status code: {}", requestCode);
                }
            } catch (IOException e) {
                LOG.error("Error while executing query plan request: {}", e.getMessage(), e);
            }
            if (attempt < sourceOptions.getScanMaxRetries() - 1) {
                try {
                    Thread.sleep(1000l * (attempt + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while retrying query plan request", ex);
                }
            }
        }
        return responseString;
    }
}
