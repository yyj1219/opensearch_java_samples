import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.*;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.TotalHits;
import org.opensearch.client.opensearch.core.search.TotalHitsRelation;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SearchDocumentsSample {

    private final OpenSearchClient client;

    public SearchDocumentsSample(OpenSearchClient client) {
        this.client = client;
    }

    /**
     * index에서 search API에 단순 term 조건 하나만 이용해서 데이터 검색
     * @param indexName
     */
    public void search(String indexName) {

        System.out.println("= SearchDocumentsSample.search =");

        Query query = BoolQuery.of(bool -> bool
                .filter(filter -> filter
                        .term(term -> term
                                .field("counter")
                                .value(value -> value
                                        .stringValue("15U")
                                )
                        )
                )).toQuery();

        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(query)
                .size(10) /// 10건 반환
                .docvalueFields(Arrays.asList(
                        FieldAndFormat.of(f -> f.field("counter")),
                        FieldAndFormat.of(f -> f.field("ctime")),
                        FieldAndFormat.of(f -> f.field("objHash")),
                        FieldAndFormat.of(f -> f.field("value"))
                ))
        );

        /* Request 내용을 문자열로 출력하고 싶은데... 안 되네;;
        try {
            JsonFactory factory = new JsonFactory();
            StringWriter jsonObjectWriter = new StringWriter();
            JacksonJsonpGenerator generator =
                    new JacksonJsonpGenerator(factory.createGenerator(jsonObjectWriter));
            JsonpMapper mapper = client._transport().jsonpMapper();
            query.serialize(generator, mapper);
            mapper.toString();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
         */

        try {
            SearchResponse<ObjectNode> response = client.search(request, ObjectNode.class);

            // 모든 데이터가 반환됐는지, 더 받아와야 할 데이터가 있는지 체크
            TotalHits total = response.hits().total();
            boolean isExtractResult = total.relation() == TotalHitsRelation.Eq;
            if (isExtractResult) {
                System.out.println("There are " + total.value() + " results.");
            } else {
                System.out.println("There are more then " + total.value() + " results.");
            }

            List<Hit<ObjectNode>> hits = response.hits().hits();
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
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 매개변수로 받은 필터링 조건들을을 사용하여 index에서 search API에 term 조건 여러개를 적용해서 데이터 검색
     * @param indexName
     * @param filterMap
     */
    public void search(String indexName, Map<String,Object> filterMap) {

        // 데이터 필터에 사용할 조건 리스트
        List<Query> conditions = new ArrayList<>();
        filterMap.forEach((filedName, fieldValue) -> {
            if (fieldValue instanceof Double) {
                conditions.add(TermQuery.of(t -> t
                        .field(filedName)
                        .value(v -> v.doubleValue((Double) fieldValue))
                ).toQuery());
            } else if (fieldValue instanceof Integer) {
                conditions.add(TermQuery.of(t -> t
                        .field(filedName)
                        .value(v -> v.longValue((Integer) fieldValue))
                ).toQuery());
            } else if (fieldValue instanceof Long) {
                conditions.add(TermQuery.of(t -> t
                        .field(filedName)
                        .value(v -> v.longValue((Long) fieldValue))
                ).toQuery());
            } else if (fieldValue instanceof Boolean) {
                conditions.add(TermQuery.of(t -> t
                        .field(filedName)
                        .value(v -> v.booleanValue((Boolean) fieldValue))
                ).toQuery());
            } else {
                conditions.add(TermQuery.of(t -> t
                        .field(filedName)
                        .value(v -> v.stringValue(fieldValue.toString()))
                ).toQuery());
            }
        });

        // search query 작성
        Query query = Query.of(
                q -> q.bool(
                        bool -> bool
                                .filter(conditions)
                )
        ).bool().toQuery();

        // search request 작성
        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(query)
                .size(10) // 10 건 반환
                .docvalueFields(Arrays.asList(
                        FieldAndFormat.of(f -> f.field("counter")),
                        FieldAndFormat.of(f -> f.field("ctime")),
                        FieldAndFormat.of(f -> f.field("objHash")),
                        FieldAndFormat.of(f -> f.field("value"))
                ))
        );

        try {
            // search API 실행
            SearchResponse<ObjectNode> response = client.search(request, ObjectNode.class);

            // 모든 데이터가 반환됐는지, 더 받아와야 할 데이터가 있는지 체크
            TotalHits total = response.hits().total();
            boolean isExtractResult = total.relation() == TotalHitsRelation.Eq;
            if (isExtractResult) {
                System.out.println("There are " + total.value() + " results.");
            } else {
                System.out.println("There are more then " + total.value() + " results.");
            }

            List<Hit<ObjectNode>> hits = response.hits().hits();
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
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }

    }

}
