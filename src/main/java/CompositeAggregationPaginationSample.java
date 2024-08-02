import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.*;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeAggregationPaginationSample {

    private final OpenSearchClient client;

    public CompositeAggregationPaginationSample(OpenSearchClient client) {
        this.client = client;
    }


    public void search(String indexName) {

        System.out.println("= CompositeAggregationPaginationSample.search =");

        // grouping key 1
        Map<String, CompositeAggregationSource> comAggSrcMap1 = new HashMap<>();
        CompositeAggregationSource comAggSrc1 = new CompositeAggregationSource.Builder()
                .terms(
                        termsAggrBuilder1 -> termsAggrBuilder1
                                .field("counter")
                                .missingBucket(false)
                                .order(SortOrder.Asc)
                ).build();
        comAggSrcMap1.put("COUNTER", comAggSrc1);

        // grouping key 2
        Map<String, CompositeAggregationSource> comAggSrcMap2 = new HashMap<>();
        CompositeAggregationSource comAggSrc2 = new CompositeAggregationSource.Builder()
                .terms(
                        termsAggrBuilder2 -> termsAggrBuilder2
                                .field("objHash")
                                .missingBucket(false)
                                .order(SortOrder.Asc)
                ).build();
        comAggSrcMap2.put("OBJ_HASH", comAggSrc2);

        // merge grouping keys into list
        List<Map<String, CompositeAggregationSource>> sources = List.of(comAggSrcMap1, comAggSrcMap2);

        String mainBucketName = "my_buckets";
        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .aggregations(mainBucketName, agg -> agg
                                .composite(comp -> comp
                                        .size(2) // grouping한 데이터를 1건씩 가쟈오기
                                        .sources(sources)
                                )
                        )
        );

        try {
            boolean isFirstRun = true;
            int bucketSize;
            Map<String, String> afterKey = new HashMap<>();
            SearchResponse<ObjectNode> response;

            do {
                // 첫 실행이 아니면 request 에 after 추가해서 조회
                if (!isFirstRun) {
                    request = SearchRequest.of(s -> s
                            .index(indexName)
                            .aggregations(mainBucketName, agg -> agg
                                    .composite(comp -> comp
                                            .size(2)
                                            .after(afterKey) // after key
                                            .sources(sources)
                                    )
                            )
                    );
                } else {
                    isFirstRun = false;
                }

                // 요청 실행
                response = client.search(request, ObjectNode.class);

                // 집계 데이터 가져오기
                if (response.aggregations().isEmpty()) {
                    break;
                }
                CompositeAggregate compAgg = response.aggregations().get(mainBucketName).composite();

                // 집계 데이터의 bucket 크기
                bucketSize = compAgg.buckets().array().size();

                // 집계 데이터를 콘솔에 출력
                compAgg.buckets().array().forEach(
                        bucket -> System.out.printf("%s : %d\n", bucket.key(), bucket.docCount())
                );

                // 다음 페이지 검색을 위한 키 설정
                compAgg.afterKey().forEach((key, value) -> afterKey.put(key, value.toString().replace("\"", "")));

            } while (bucketSize > 0);

        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

}
