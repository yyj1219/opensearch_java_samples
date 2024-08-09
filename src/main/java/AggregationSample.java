import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.LongTermsBucket;
import org.opensearch.client.opensearch._types.aggregations.StringTermsBucket;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;
import java.util.Map;

public class AggregationSample {

    private final OpenSearchClient client;

    public AggregationSample(OpenSearchClient client) {
        this.client = client;
    }

    public void search(String indexName) {

        System.out.println("= AggregationSample.search =");

        Query query = BoolQuery.of(bool -> bool
                .filter(filter -> filter
                        .term(term -> term
                                .field("counter")
                                .value(value -> value
                                        .stringValue("15U")
                                )
                        )
                )).toQuery();

        try {
            SearchRequest request = new SearchRequest.Builder()
                    .index(indexName)
                    .query(query)
                    .aggregations("MainAgg_term",
                            new Aggregation.Builder()
                                    .terms(t -> t.field("counter"))
                                    .aggregations("SubAgg_term",
                                            new Aggregation.Builder()
                                                    .terms(t -> t.field("objHash"))
                                                    .aggregations("SubSubAgg_max",
                                                            new Aggregation.Builder()
                                                                    .max(m -> m.field("value")
                                                                    ).build()
                                                    ).build()
                                    ).build()
                    ).build();

            SearchResponse<ObjectNode> response = client.search(request, ObjectNode.class);

            for (Map.Entry<String, Aggregate> mainEntry : response.aggregations().entrySet()) {
                System.out.println("Agg - " + mainEntry.getKey());

                for (StringTermsBucket mainBucket : mainEntry.getValue().sterms().buckets().array()) { // String Term
                    System.out.println("    key: "+ mainBucket.key());

                    for (Map.Entry<String, Aggregate> subEntry : mainBucket.aggregations().entrySet()) {
                        System.out.println("    Agg - " + subEntry.getKey());

                        for (LongTermsBucket subBucket : subEntry.getValue().lterms().buckets().array()) { // Long Term
                            System.out.printf("        key: %s (count=%d)\n",
                                    subBucket.key(),
                                    subBucket.docCount()
                            );
                            System.out.printf("            max=%f\n",
                                    subBucket.aggregations().get("SubSubAgg_max").max().value()
                            );
                        }
                    }
                }
            }
        } catch (OpenSearchClientException | IOException e) {
            e.printStackTrace();
        }
    }
}
