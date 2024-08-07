import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import scouter.server.Logger;
import tuna.server.db.common.elastic.ConnectionManager;
import tuna.server.db.rd.ICounterKeyRD;
import tuna.server.model.CounterKeyInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CounterKeyESRD implements ICounterKeyRD {

    @Override
    public Map<String, CounterKeyInfo> getCounterKeyMap() {
        Map<String, CounterKeyInfo> counterKeyMap = new ConcurrentHashMap<>();

        RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();

        SearchRequest searchRequest = new SearchRequest("counterkey");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        String[] target = new String[]{"host", "javaee", "httpd", "k8sNode", "k8sNamespace", "k8sContainer", "k8sCluster"};
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("target", target));

        String[] inField = new String[]{"keyId", "target", "agg"};
        sourceBuilder.query(queryBuilder);
        sourceBuilder.fetchSource(inField, null);
        sourceBuilder.size(1000);

        searchRequest.source(sourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<SearchHit> searchHits = Arrays.asList(searchResponse.getHits().getHits());
            for (SearchHit hit : searchHits) {
                counterKeyMap.put(hit.getSourceAsMap().get("keyId").toString()
                        , new CounterKeyInfo(hit.getSourceAsMap().get("keyId").toString()
                                , hit.getSourceAsMap().get("target").toString()
                                , hit.getSourceAsMap().get("agg").toString()));
            }
            Logger.println("CounterKeyMap Retrieve Complete");
        } catch (IOException e) {
            Logger.println(e.getMessage());
        }

        return counterKeyMap;
    }
}
