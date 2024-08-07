import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.FieldAndFormat;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ScrollSample {

    private final OpenSearchClient client;

    public ScrollSample(OpenSearchClient client) {
        this.client = client;
    }

    public void search(String indexName) {

        System.out.println("= ScrollSample.search =");

        Query query = BoolQuery.of(bool -> bool
                .filter(filter -> filter
                        .term(term -> term
                                .field("counter")
                                .value(value -> value
                                        .stringValue("15U")
                                )
                        )
                )).toQuery();

        Time keepAlive = Time.of(t -> t.time("1m"));
        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .scroll(keepAlive) // 스크롤 설정, keepAlive 시간 동안만 유효함
                .query(query)
                .size(5) /// 5건 반환
                .docvalueFields(Arrays.asList(
                        FieldAndFormat.of(f -> f.field("counter")),
                        FieldAndFormat.of(f -> f.field("ctime")),
                        FieldAndFormat.of(f -> f.field("objHash")),
                        FieldAndFormat.of(f -> f.field("value"))
                ))
        );

        // OpenSearch/ElasticSearch에서 scroll ID
        // 초기 검색 요청에서 scroll ID가 생성됩니다. 이 scroll ID는 검색 컨텍스트를 나타냅니다.
        // scrolling을 할 때마다 scroll ID가 달라지지는 않습니다.
        String scrollId = "";
        try {
            // search API 첫번째 실행 (요청 안에 스크롤해달라는 내용 포함)
            SearchResponse<ObjectNode> response = client.search(request, ObjectNode.class);
            printDocument(response.hits().hits());
            long documentsCount = response.hits().hits().size();

            scrollId = response.scrollId(); // 초기 검색 요청에서 생성된 scroll ID 변수에 담기

            // 다음 데이터를 모두 받아올 때까지 계속 scrolling
            while (true) {
                ScrollRequest scrollRequest = new ScrollRequest.Builder().scrollId(scrollId).scroll(keepAlive).build();
                ScrollResponse<ObjectNode> scrollResponse = client.scroll(scrollRequest, ObjectNode.class);
                printDocument(scrollResponse.hits().hits());
                documentsCount += scrollResponse.hits().hits().size();

                // 모든 데이터를 가져 왔으면 while 문에서 빠져나감
                if (scrollResponse.hits().hits().isEmpty()) {
                    System.out.println("Total Documents: " + documentsCount);
                    break;
                }
            }
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                // 검색이 종료되었으므로 명시적으로 scroll 삭제
                String finalScrollId = scrollId;
                client.clearScroll(cs -> cs.scrollId(finalScrollId));
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }

    }

    public void printDocument(List<Hit<ObjectNode>> hits) {
        for (Hit<ObjectNode> hit:hits) {

            if (hit.source() == null) {
                System.out.println("Document has no source.");
            } else {
                System.out.println("Document source: \n" + hit.source().toPrettyString());
            }

            if (hit.fields() != null && !hit.fields().isEmpty()) {
                System.out.print("Document has docvalue_fields: \n{");
                hit.fields().keySet().forEach(key -> {
                    ArrayNode v = hit.fields().get(key).to(ArrayNode.class);
                    System.out.print(key + ":" + v.get(0) + " ");
                });
                System.out.println("}");
            }
        }
    }
}
