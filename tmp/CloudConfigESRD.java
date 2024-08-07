import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;
import scouter.config.CommonConfigure;

import java.io.IOException;
import java.util.*;

import scouter.server.Logger;
import tuna.server.db.common.elastic.ConnectionManager;
import tuna.server.db.rd.ICloudConfigRD;

public class CloudConfigESRD implements ICloudConfigRD {

    private CommonConfigure conf;

    public CloudConfigESRD() {
        this.conf = CommonConfigure.getInstance();
    }

    @Override
    public Map<String, Object> getAgent(int id) {
        SearchRequest searchRequest = new SearchRequest("cloud-agent");

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("objectId", id));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(new String[]{"objectId","active","account","ipAddress","port"}, new String[]{"wakeupTime","updateDate"});
        sourceBuilder.size(conf.es_query_fetch_size);

        if (conf.print_es_query) {
            Logger.println(queryBuilder.toString());
        }

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        try {
            SearchResponse searchResponse = ConnectionManager.getInstance().getReadClient().search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse.getHits().getHits()[0].getSourceAsMap();
        } catch (IOException e) {
            Logger.println("cloud-config-001", e);
        }
        return null;
    }

    @Override
    public JSONArray getCredential(String cspName, String name) {
        JSONArray jsonArray = new JSONArray();
        SearchRequest searchRequest = new SearchRequest("cloud-credential");

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("csp", cspName))
                .filter(QueryBuilders.termQuery("name", name));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        switch (cspName) {
            case "AWS" :
                sourceBuilder.fetchSource(new String[]{"name", "csp", "accessKeyId", "secretAccessKey"}, new String[]{});
                break;
            case "GCP" :
                sourceBuilder.fetchSource(new String[]{"name", "csp", "projectId", "region", "keyFile"}, new String[]{});
                break;
            case "Azure" :
                sourceBuilder.fetchSource(new String[]{"name", "csp", "subscriptionId", "tenantId", "clientId", "clientSecret", "resourceGroupName", "location"}, new String[]{});
                break;
            default :
                break;
        }
        sourceBuilder.size(conf.es_query_fetch_size);

        if (conf.print_es_query) {
            Logger.println(queryBuilder.toString());
        }

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        try {
            SearchResponse searchResponse = ConnectionManager.getInstance().getReadClient().search(searchRequest, RequestOptions.DEFAULT);
            jsonArray.add(searchResponse.getHits().getHits()[0].getSourceAsMap());
        } catch (IOException e) {
            Logger.println("cloud-config-002", e);
        }
        return jsonArray;
    }

    @Override
    public JSONArray getCredentialListJson(String[] names) {
        JSONArray jsonArray = new JSONArray();
        try {
            SearchResponse searchResponse = getCredentialList(names);
            for (SearchHit hit: searchResponse.getHits().getHits()) {
                jsonArray.add(hit.getSourceAsMap());
            }
        } catch (IOException e) {
            Logger.println("cloud-config-003", e);
        }
        return jsonArray;
    }

    @Override
    public List<Map<String, Object>> getCredentialListMap(String[] names) {
        List<Map<String, Object>> result =  new ArrayList<>();
        try {
            SearchResponse searchResponse = getCredentialList(names);
            for (SearchHit hit: searchResponse.getHits().getHits()) {
                result.add(hit.getSourceAsMap());
            }
        } catch (IOException e) {
            Logger.println("cloud-config-004", e);
        }
        return result;
    }

    @Override
    public List<Map<String, Object>> getConfigurationListMap(int[] cloudAgentIds) {
        List<Map<String, Object>> result =  new ArrayList<>();
        try {
            SearchResponse searchResponse = getConfigurationList(cloudAgentIds);
            for (SearchHit hit: searchResponse.getHits().getHits()) {
                result.add(hit.getSourceAsMap());
            }
        } catch (IOException e) {
            Logger.println("cloud-config-005", e);
        }
        return result;
    }

    @Override
    public List<Map<String, Object>> getNamespaceMetricListMap(String csp, List<String> namespaces) {
        List<Map<String, Object>> result =  new ArrayList<>();
        try {
            String esIndex = "cloud-metric-meta";

            Map<String, Object> queryConditions = new HashMap<>();
            queryConditions.put("csp", csp);
            queryConditions.put("namespace", namespaces);
            queryConditions.put("collectFlag", true);

            LinkedHashMap<String, String> order = new LinkedHashMap<>();
            order.put("namespace", "asc");
            order.put("metricName", "asc");

            String[] includeFields = new String[]{
                    "metricMetaId",
                    "csp",
                    "namespace",
                    "metricName",
                    "aggTypes",
                    "includeDimensions",
                    "excludeDimensions"};

            result = retrieveScrolledList(esIndex, queryConditions, order, includeFields, conf.es_query_fetch_size);
        } catch (IOException e) {
            Logger.println("cloud-config-006", e);
        }
        return result;
    }

    @Override
    public JSONArray getMetricListJson(String[] ids) {
        JSONArray jsonArray = new JSONArray();
        try {
            SearchResponse searchResponse = getMetricList(ids);
            for (SearchHit hit: searchResponse.getHits().getHits()) {
                jsonArray.add(hit.getSourceAsMap());
            }
        } catch (IOException e) {
            Logger.println("cloud-config-007", e);
        }
        return jsonArray;
    }

    @Override
    public List<Map<String, Object>> getMetricListMap(String[] ids) {
        List<Map<String, Object>> result =  new ArrayList<>();
        try {
            SearchResponse searchResponse = getMetricList(ids);
            for (SearchHit hit: searchResponse.getHits().getHits()) {
                result.add(hit.getSourceAsMap());
            }
        } catch (IOException e) {
            Logger.println("cloud-config-008", e);
        }
        return result;
    }

    /**
     * 현재 설정된 클라우드 메트릭 갯수(CSP Metric Count)
     * @return: The count of metrics set to collect in the all configurations
     */
    @Override
    public int getCollectMetricCount() {

        SearchResponse searchResponse;

        // 1. Get list of namespaces from [cloud-configuration]
        QueryBuilder queryBuilder = QueryBuilders
                .boolQuery()
                .filter(
                        QueryBuilders.termQuery("use", "Y")
                );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .size(conf.es_query_fetch_size)
                .fetchSource("cspService.namespace", null)
                .query(queryBuilder);
        SearchRequest searchRequest = new SearchRequest()
                .indices("cloud-configuration")
                .source(sourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        if (conf.print_es_query) {
            Logger.println(searchRequest.source().toString());
        }

        try {
            searchResponse = ConnectionManager
                    .getInstance()
                    .getReadClient()
                    .search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            Logger.println("cloud-config-009", e);
            return 0;
        }

        ArrayList<String> namespaces = new ArrayList<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            for (HashMap<String, String> cspService : (ArrayList<HashMap<String, String>>) searchHit.getSourceAsMap().get("cspService")) {
                namespaces.add(cspService.get("namespace"));
            }
        }

        // 2. get the metric count from [cloud-metric-meta] using namespaces
        queryBuilder = QueryBuilders
                .boolQuery()
                .filter(
                        QueryBuilders.termQuery("collectFlag", true)
                )
                .filter(
                        QueryBuilders.termsQuery("namespace", namespaces)
                );
        sourceBuilder = new SearchSourceBuilder()
                .size(0)
                .fetchSource(false)
                .trackTotalHits(true)
                .query(queryBuilder);
        searchRequest = new SearchRequest()
                .indices("cloud-metric-meta")
                .source(sourceBuilder)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        if (conf.print_es_query) {
            Logger.println(searchRequest.source().toString());
        }

        try {
            searchResponse = ConnectionManager
                    .getInstance()
                    .getReadClient()
                    .search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            Logger.println("cloud-config-010", e);
            return 0;
        }

        return (int) searchResponse.getHits().getTotalHits().value;
    }

    private SearchResponse getCredentialList(String[] names) throws IOException {
        SearchRequest searchRequest = new SearchRequest("cloud-credential");

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("name", names));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(
                new String[]{"name", "csp",
                        "accessKeyId", "secretAccessKey",
                        "projectId", "region", "keyFile",
                        "subscriptionId", "tenantId", "clientId", "clientSecret", "resourceGroupName", "location"
                },
                new String[]{});
        sourceBuilder.sort("name", SortOrder.ASC);
        sourceBuilder.size(conf.es_query_fetch_size);

        if (conf.print_es_query) {
            Logger.println(queryBuilder.toString());
        }

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        return ConnectionManager.getInstance().getReadClient().search(searchRequest, RequestOptions.DEFAULT);
    }

    private SearchResponse getConfigurationList(int[] cloudAgentIds) throws IOException {
        SearchRequest searchRequest = new SearchRequest("cloud-configuration");

        QueryBuilder queryBuilder = QueryBuilders.termsQuery("agentId", cloudAgentIds);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(
                new String[]{"name", "agentId", "agentName", "csp", "credentials", "use", "collectInterval", "cspService", "updateDate"},
                new String[]{});
        searchSourceBuilder.size(conf.es_query_fetch_size);

        if (conf.print_es_query) {
            Logger.println(queryBuilder.toString());
        }

        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        return ConnectionManager.getInstance().getReadClient().search(searchRequest, RequestOptions.DEFAULT);
    }

    private SearchResponse getMetricList(String[] ids) throws IOException {
        SearchRequest searchRequest = new SearchRequest("cloud-manage-metrics");

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termsQuery("accountId", ids));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(new String[]{"accountId", "namespace", "dimensions", "metricName", "csp", "collectInterval", "alignmentPeriod"}, new String[]{});
        sourceBuilder.sort("accountId", SortOrder.ASC);
        sourceBuilder.sort("namespace", SortOrder.ASC);

        sourceBuilder.size(conf.es_query_fetch_size);

        if (conf.print_es_query) {
            Logger.println(queryBuilder.toString());
        }

        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        return ConnectionManager.getInstance().getReadClient().search(searchRequest, RequestOptions.DEFAULT);
    }

    private List<Map<String, Object>> retrieveScrolledList(
            Object index,
            Map<String, Object> queryConditions,
            LinkedHashMap<String, String> order,
            String[] includeFields,
            int fetchSize
    ) throws IOException {
        RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        List<Map<String, Object >> resultList = new ArrayList<Map<String, Object>>();

        SearchRequest searchRequest = null;

        if (index instanceof String[]) {
            searchRequest = new SearchRequest((String[]) index);
        } else if (index instanceof String) {
            searchRequest = new SearchRequest((String) index);
        }

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.scroll(scroll);

        // make "query" for ElasticSearch
        if ((queryConditions != null) && (queryConditions.size() > 0)) {
            Set<String> keySet = queryConditions.keySet();
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            for (String key : keySet) {
                if (queryConditions.get(key) instanceof ArrayList) {
                    boolQuery.filter(QueryBuilders.termsQuery(key, (ArrayList<String>) queryConditions.get(key)));
                } else if (queryConditions.get(key) instanceof String[]) {
                    boolQuery.filter(QueryBuilders.termsQuery(key, (String[]) queryConditions.get(key)));
                } else if (queryConditions.get(key) instanceof Integer[]) {
                    boolQuery.filter(QueryBuilders.termsQuery(key, (Integer[]) queryConditions.get(key)));
                } else if (queryConditions.get(key) instanceof HashSet) {
                    boolQuery.filter(QueryBuilders.termsQuery(key, (HashSet) queryConditions.get(key)));
                } else {
                    boolQuery.filter(QueryBuilders.termsQuery(key, queryConditions.get(key)));
                }
            }
            searchSourceBuilder.query(boolQuery);
        }

        // make "sort" for ElasticSearch
        if ((order != null) && (order.size() > 0)) {
            Set<String> keySet = order.keySet();
            for (String key : keySet) {
                SortOrder sortOrder = null;
                if (order.get(key).equals("asc")) {
                    sortOrder = SortOrder.ASC;
                } else if (order.get(key).equals("desc")) {
                    sortOrder = SortOrder.DESC;
                }
                searchSourceBuilder.sort(key, sortOrder);
            }
        }

        // make "_source: { include Fields }"
        if ((includeFields != null) && (includeFields.length > 0)) {
            searchSourceBuilder.fetchSource(includeFields, null);
        }

        searchSourceBuilder.size(fetchSize);

        // search - first scroll data
        searchRequest.source(searchSourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();
        clearScrollRequest.addScrollId(scrollId);

        SearchHit[] searchHits = response.getHits().getHits();

        // Continue scrolling until end of data or data exceeds 50000
        int maxCount = 0;
        boolean readContinue = true;
        while (searchHits != null && searchHits.length > 0 && readContinue) {
            for (int i = 0; i < searchHits.length; i++) {
                if (maxCount++ > 50000) {
                    readContinue = false;
                    break;
                }
                resultList.add(searchHits[i].getSourceAsMap());
            }
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            clearScrollRequest.addScrollId(scrollId);
            searchHits = response.getHits().getHits();
        }

        client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

        return resultList;
    }
}
