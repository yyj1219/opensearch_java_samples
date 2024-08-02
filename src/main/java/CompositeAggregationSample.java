import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.CompositeAggregation;
import org.opensearch.client.opensearch._types.aggregations.CompositeAggregationSource;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeAggregationSample {

    private final OpenSearchClient client;

    public CompositeAggregationSample(OpenSearchClient client) {
        this.client = client;
    }

    public void search(String indexName) {

        System.out.println("= CompositeAggregationSample.search =");

        // grouping key 1
        Map<String, CompositeAggregationSource> comAggSrcMap1 = new HashMap<>();
        CompositeAggregationSource comAggSrc1 = new CompositeAggregationSource.Builder()
                .terms(
                        termsAggrBuilder1 -> termsAggrBuilder1.field("counter").missingBucket(false).order(SortOrder.Asc)
                ).build();
        comAggSrcMap1.put("COUNTER", comAggSrc1);

        // grouping key 2
        Map<String, CompositeAggregationSource> comAggSrcMap2 = new HashMap<>();
        CompositeAggregationSource comAggSrc2 = new CompositeAggregationSource.Builder()
                .terms(
                        termsAggrBuilder2 -> termsAggrBuilder2.field("objHash").missingBucket(false).order(SortOrder.Asc)
                ).build();
        comAggSrcMap2.put("OBJ_HASH", comAggSrc2);

        CompositeAggregation compAgg = new CompositeAggregation.Builder().sources(List.of(comAggSrcMap1, comAggSrcMap2)).build();
        Aggregation aggregation = new Aggregation.Builder().composite(compAgg).build();

        SearchRequest request = new SearchRequest.Builder()
                .index(indexName)
                .aggregations("my_buckets", aggregation).build();

        try {
            SearchResponse<ObjectNode> response = client.search(request, ObjectNode.class);
            for (Map.Entry<String, Aggregate> entry : response.aggregations().entrySet()) {
                System.out.println("key: "+ entry.getKey());
                entry.getValue().composite().buckets().array().forEach(
                        bucket -> System.out.printf("%s : %d\n", bucket.key(), bucket.docCount())
                );
            }
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
