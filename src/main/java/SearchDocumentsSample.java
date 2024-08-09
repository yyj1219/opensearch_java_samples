import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.FieldAndFormat;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.TermQuery;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.*;
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
     * search API - terms 쿼리 하나만 이용해서 데이터 검색
     * @param indexName
     */
    public void searchWithTerms(String indexName) {
        System.out.println("= SearchDocumentsSample.searchWithTerms =");

        String[] filedValues = {"15U", "16U"};
        List<String> filedValuesList = Arrays.asList(filedValues);

        BoolQuery boolQuery = BoolQuery.of(bool -> bool
                .filter(filter -> filter
                        .terms(terms -> terms
                                .field("counter")
                                .terms(v -> v
                                        .value(
                                                //filedValuesList.stream().map(FieldValue::of).collect(Collectors.toList())
                                                OpenSearchUtil.toFieldValueList(filedValues)
                                        )
                                )
                        )
                )
        );

        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(boolQuery.toQuery())
                .size(10) // 10건 반환
                .source(SourceConfig.of(sc -> sc
                        .filter(SourceFilter.of(sf -> sf
                                .includes("counter", "objHash", "value")
                                .excludes("ctime")
                                )
                        ))
                )
                .docvalueFields(Arrays.asList(
                        FieldAndFormat.of(f -> f.field("counter")),
                        FieldAndFormat.of(f -> f.field("ctime")),
                        FieldAndFormat.of(f -> f.field("objHash")),
                        FieldAndFormat.of(f -> f.field("value"))
                ))
        );

        System.out.println(OpenSearchUtil.convertToJson(request));

        try {
            SearchResponse<Map> response = client.search(request, Map.class);

            // 모든 데이터가 반환됐는지, 더 받아와야 할 데이터가 있는지 체크
            TotalHits total = response.hits().total();
            boolean isExtractResult = total.relation() == TotalHitsRelation.Eq;
            if (isExtractResult) {
                System.out.println("There are " + total.value() + " results.");
            } else {
                System.out.println("There are more then " + total.value() + " results.");
            }

            List<Hit<Map>> hits = response.hits().hits();
            for (Hit<Map> hit:hits) {

                if (hit.source() == null) {
                    System.out.println("Document has no source.");
                } else {
                    System.out.println("Document source: \n" + hit.source());
                }

                // docvalue_fields 처리
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
     * search API - 단순 term 쿼리 하나만 이용해서 데이터 검색
     * @param indexName
     */
    public void searchWithTerm(String indexName) {

        System.out.println("= SearchDocumentsSample.searchWithTerm =");

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
                .source(SourceConfig.of(sc -> sc
                        .filter(SourceFilter.of(sf -> sf
                                        .includes("counter", "objHash", "value")
                                        .excludes("ctime")
                                )
                        ))
                )
                .docvalueFields(Arrays.asList(
                        FieldAndFormat.of(f -> f.field("counter")),
                        FieldAndFormat.of(f -> f.field("ctime")),
                        FieldAndFormat.of(f -> f.field("objHash")),
                        FieldAndFormat.of(f -> f.field("value"))
                ))
        );

        System.out.println(OpenSearchUtil.convertToJson(request));

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
     * search API - 단순 term 쿼리 여러 개를 적용해서 데이터 검색
     * @param indexName
     * @param filterMap
     */
    public void searchWithTerm(String indexName, Map<String,Object> filterMap) {

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

        // searchWithTerm query 작성
        Query query = Query.of(
                q -> q.bool(
                        bool -> bool
                                .filter(conditions)
                )
        ).bool().toQuery();

        // searchWithTerm request 작성
        SearchRequest request = SearchRequest.of(s -> s
                .index(indexName)
                .query(query)
                .size(10) // 10 건 반환
                .source(SourceConfig.of(sc -> sc
                        .filter(SourceFilter.of(sf -> sf
                                        .includes("counter", "objHash", "value")
                                        .excludes("ctime")
                                )
                        ))
                )
                .docvalueFields(Arrays.asList(
                        FieldAndFormat.of(f -> f.field("counter")),
                        FieldAndFormat.of(f -> f.field("ctime")),
                        FieldAndFormat.of(f -> f.field("objHash")),
                        FieldAndFormat.of(f -> f.field("value"))
                ))
        );

        System.out.println(OpenSearchUtil.convertToJson(request));

        try {
            // searchWithTerm API 실행
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
