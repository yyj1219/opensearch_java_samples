import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpStatus;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.mapping.*;
import org.opensearch.client.opensearch.generic.*;
import org.opensearch.client.opensearch.indices.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * OpenSearchClient(aka.high level Client)를 이용해서 index 생성과 삭제를 테스트하는 샘플 클래스
 */
public class IndexSample {

    private final OpenSearchClient client;

    public IndexSample(OpenSearchClient client) {
        this.client = client;
    }

    /**
     * 샘플 메소드 - Fluent DSL 방식 (OpenSearch에서 추천하는 방식) <br>
     * Fluent API는 무엇인가? <br>
     * 소프트웨어 개발에서 메서드 호출을 체인으로 연결하여, 더 읽기 쉽고 유창한(플루언트) 코드를 작성할 수 있도록 하는 프로그래밍 기법입니다. <br>
     * 주로 메서드 체이닝을 통해 객체의 상태를 설정하거나 동작을 구성하는 데 사용됩니다.
     * @param indexName 생성할 index 이름
     */
    public void createIndexFluentDsl(String indexName) {

        System.out.println("= IndexSample.createIndexFluentDsl =");

        // custom index mappings 정의
        /* 아래와 같은 index의 mappings를 TypeMapping 클래스로 정의함
        {
          "mappings": {
            "dynamic" : "strict",
            "_source": {
              "enabled": true
            },
            "properties": {
              "counter": {
                "type": "keyword",
                "ignore_above": 256
              },
              "ctime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss || epoch_millis"
              },
              "objHash": {
                "type": "integer"
              },
              "value": {
                "type": "scaled_float",
                "scaling_factor": 100
              }
            }
          }
        }
        */
        Map<String, Property> properties = new HashMap<>();
        properties.put("counter", new Property.Builder().keyword(new KeywordProperty.Builder().ignoreAbove(256).build()).build());
        properties.put("ctime", new Property.Builder().date(new DateProperty.Builder().format("yyyy-MM-dd HH:mm:ss || epoch_millis").build()).build());
        properties.put("objHash", new Property.Builder().integer(new IntegerNumberProperty.Builder().build()).build());
        properties.put("value", new Property.Builder().scaledFloat(new ScaledFloatNumberProperty.Builder().scalingFactor(100D).build()).build());

        try {
            // index 생성 요청과 응답
            CreateIndexRequest request = CreateIndexRequest.of(c -> c
                    .index(indexName)
                    .settings(settings -> settings
                            .numberOfShards("1")
                            .numberOfReplicas("1")
                    )
                    .mappings(mappings -> mappings
                            .dynamic(DynamicMapping.Strict)
                            .source(_source -> _source.enabled(true))
                            .properties(properties)
                    ));

            System.out.println(OpenSearchUtil.convertToJson(request));

            CreateIndexResponse response = client.indices().create(request);

            if (Boolean.TRUE.equals(response.acknowledged())) {
                System.out.println("Index creation successful");
            } else {
                System.out.println("Index creation failed");
            }
        } catch (OpenSearchException | IOException e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * 샘플 메소드 - 클래식 빌더 방식
     * @param indexName 생성할 index 이름
     */
    public void createIndex(String indexName) {

        System.out.println("= IndexSample.createIndex (like elasticsearch 7.10) =");

        // custom index setting 정의
        IndexSettings indexSettings = new IndexSettings.Builder()
                .numberOfShards("1")
                .numberOfReplicas("1")
                .build();

        // custom index mappings 정의
        /* 아래와 같은 index의 mappings를 TypeMapping 클래스로 정의함
        {
          "mappings": {
            "dynamic" : "strict",
            "_source": {
              "enabled": true
            },
            "properties": {
              "counter": {
                "type": "keyword",
                "ignore_above": 256
              }, 
              "ctime": {
                "type": "date",
                "format": "yyyy-MM-dd HH:mm:ss || epoch_millis"
              },
              "objHash": {
                "type": "integer"
              }, 
              "value": {
                "type": "scaled_float",
                "scaling_factor": 100
              }
            }
          }
        }
        */
        Map<String, Property> propertyMap = new HashMap<>();
        propertyMap.put("counter", new Property.Builder().keyword(new KeywordProperty.Builder().ignoreAbove(256).build()).build());
        propertyMap.put("ctime", new Property.Builder().date(new DateProperty.Builder().format("yyyy-MM-dd HH:mm:ss || epoch_millis").build()).build());
        propertyMap.put("objHash", new Property.Builder().integer(new IntegerNumberProperty.Builder().build()).build());
        propertyMap.put("value", new Property.Builder().scaledFloat(new ScaledFloatNumberProperty.Builder().scalingFactor(100D).build()).build());
        TypeMapping mapping = new TypeMapping.Builder()
                .dynamic(DynamicMapping.Strict)
                .source(new SourceField.Builder().enabled(true).build())
                .properties(propertyMap)
                .build();

        // index request 작성 - 람다식 사용
        CreateIndexRequest request = CreateIndexRequest.of(b -> b
                .index(indexName)
                .settings(indexSettings)
                .mappings(mapping)
        );

        try {
            // index 생성 요청과 응답
            CreateIndexResponse response = client.indices().create(request);

            if (Boolean.TRUE.equals(response.acknowledged())) {
                System.out.println("Index creation successful");
            } else {
                System.out.println("Index creation failed");
            }
        } catch (OpenSearchException | IOException e) {
            System.out.println(e.getMessage());
        }

    }

    /**
     * 샘플 메소드 - index 생성 - JSON 문자열 방식
     * @param indexName 생성할 index 이름
     */
    public void createIndexWithJsonString(OpenSearchGenericClient genericClient, String indexName) {
        System.out.println("= IndexSample.createIndexWithJsonString =");

        String jsonString = """
                {
                          "mappings": {
                            "dynamic" : "strict",
                            "_source": {
                              "enabled": true
                            },
                            "properties": {
                              "counter": {
                                "type": "keyword",
                                "ignore_above": 256
                              },
                              "ctime": {
                                "type": "date",
                                "format": "yyyy-MM-dd HH:mm:ss || epoch_millis"
                              },
                              "objHash": {
                                "type": "integer"
                              },
                              "value": {
                                "type": "scaled_float",
                                "scaling_factor": 100
                              }
                            }
                          }
                        }
                """;

        try {
            Response genericResponse = genericClient.execute(
                    Requests.builder()
                            .endpoint("/" + indexName)
                            .method("PUT")
                            .json(jsonString)
                            .build()
            );

            if (genericResponse.getStatus() == HttpStatus.SC_OK) {
                System.out.println("Index creation successful");
            } else {
                System.out.println("Index creation failed");
            }
            ObjectNode jsonObject = genericResponse.getBody()
                    .map(body -> Bodies.json(body, ObjectNode.class, genericClient._transport().jsonpMapper()))
                    .orElse(null);

            System.out.println(jsonObject.toString());

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 샘플 메소드 - index 삭제
     * @param indexName 삭제할 index 이름
     */
    public void deleteIndex(String indexName) {

        System.out.println("= IndexSample.deleteIndex =");

        DeleteIndexRequest request = new DeleteIndexRequest.Builder().index(indexName).build();
        try {
            DeleteIndexResponse response = client.indices().delete(request);

            if (response.acknowledged()) {
                System.out.println("Index deletion successful");
            } else {
                System.out.println("Index deletion failed");
            }
        } catch (OpenSearchException | IOException e) {
            System.out.println(e.getMessage());
        }

    }

}
