import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;

public class SingleDocumentSample {

    private final OpenSearchClient client;
    private final OpenSearchAsyncClient asyncClient;

    public SingleDocumentSample(OpenSearchClient client, OpenSearchAsyncClient asyncClient) {
        this.client = client;
        this.asyncClient = asyncClient;
    }

    /**
     * 단일 도큐먼트를 인덱싱합니다. (단순 DSL)
     * 도큐먼트가 없으면 추가되고, 있으면 갱신됩니다.
     * @param indexName 인덱스 이름
     * @param documentId 도큐먼트 아이디
     */
    public void singleDocumentDSL(String indexName, String documentId) {

        System.out.println("= SingleDocumentSample.singleDocumentDSL =");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put("counter", "15U");
        objectNode.put("ctime", 1717558901000L); // timestamp in milliseconds 2024/06/05 03:41:41 GMT+09:00
        objectNode.put("objHash", 1113030459);
        objectNode.put("value", 0);

        try {
            IndexResponse response = client.index(i-> i
                    .index(indexName)
                    .id(documentId)
                    .document(objectNode));

            System.out.println("Indexed document with version " + response.version());
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 단일 도큐먼트를 인덱싱합니다. (of 패턴 사용)
     * 도큐먼트가 없으면 추가되고, 있으면 갱신됩니다.
     * @param indexName 인덱스 이름
     * @param documentId 도큐먼트 아이디
     */
    public void singleDocumentDSLwithOf(String indexName, String documentId) {

        System.out.println("= SingleDocumentSample.singleDocumentDSLwithOf =");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put("counter", "15U");
        objectNode.put("ctime", 1717558901000L); // timestamp in milliseconds 2024/06/05 03:41:41 GMT+09:00
        objectNode.put("objHash", 1113030459);
        objectNode.put("value", 0);

        try {
            IndexRequest<ObjectNode> request = IndexRequest.of(i -> i
                    .index(indexName)
                    .id(documentId)
                    .document(objectNode)
                    .refresh(Refresh.True) // 변경 사항을 즉시 반영
            );

            System.out.println("Request body: \n" +
                    new ObjectMapper().writeValueAsString(request.document()));

            IndexResponse response = client.index(request);

            System.out.println("Indexed document with version " + response.version());
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 단일 도큐먼트를 인덱싱합니다. (Builder 패턴 사용)
     * 도큐먼트가 없으면 추가되고, 있으면 갱신됩니다.
     * @param indexName 인덱스 이름
     * @param documentId 도큐먼트 아이디
     */
    public void singleDocumentBuilder(String indexName, String documentId) {

        System.out.println("= SingleDocumentSample.singleDocumentBuilder =");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put("counter", "15U");
        objectNode.put("ctime", 1717558901000L); // timestamp in milliseconds 2024/06/05 03:41:41 GMT+09:00
        objectNode.put("objHash", 1113030459);
        objectNode.put("value", 0);

        try {
            IndexRequest.Builder<ObjectNode> requestBuilder = new IndexRequest.Builder<>();
            requestBuilder.index(indexName);
            requestBuilder.id(documentId);
            requestBuilder.document(objectNode);
            requestBuilder.refresh(Refresh.True); // 변경 사항을 즉시 반영

            IndexResponse response = client.index(requestBuilder.build());

            System.out.println("Indexed document with version " + response.version());
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 단일 도큐먼트를 인덱싱합니다. (비동기 Async)
     * 도큐먼트가 없으면 추가되고, 있으면 갱신됩니다.
     * @param indexName 인덱스 이름
     * @param documentId 도큐먼트 아이디
     */
    public void singleDocumentDSLAsync(String indexName, String documentId) {

        System.out.println("= SingleDocumentSample.singleDocumentDSLAsync =");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put("counter", "15U");
        objectNode.put("ctime", 1717558901000L); // timestamp in milliseconds 2024/06/05 03:41:41 GMT+09:00
        objectNode.put("objHash", 1113030459);
        objectNode.put("value", 0);

        try {
            asyncClient.index(i -> i
                    .index(indexName)
                    .id(documentId)
                    .document(objectNode)
                    .refresh(Refresh.True)
            ).whenComplete((response, exception) -> {
                if (exception != null) {
                    System.out.println("Failed to index: " + exception.getMessage());
                } else {
                    System.out.println("Indexed document with version " + response.version());
                }
            });

        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 도큐먼트 아이디를 이용해서 단일 도큐먼트를 읽어 옵니다.
     * @param indexName 인덱스 이름
     * @param documentId 도큐먼트 아이디
     */
    public void retrieveSingleDocument(String indexName, String documentId) {

        System.out.println("= SingleDocumentSample.retrieveSingleDocument =");

        GetRequest request = GetRequest.of(i -> i
                .index(indexName)
                .id(documentId)
        );
        try {
            GetResponse<ObjectNode> response = client.get(request, ObjectNode.class);

            if (response.found()) {
                System.out.println("Document found");

                // 도큐먼트가 있더라도 `_source`를 disabled 했었으면 `source` 필드는 null 임
                if (response.source() != null) {
                    System.out.println("Document: \n" + response.source().toPrettyString());
                }
            } else {
                System.out.println("Document not found");
            }
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 도큐먼트 아이디를 이용해서 단일 도큐먼트를 삭제합니다.
     * @param indexName 인덱스 이름
     * @param documentId 도큐먼트 아이디
     */
    public void deleteDocument(String indexName, String documentId) {

        System.out.println("= SingleDocumentSample.deleteDocument =");

        DeleteRequest request = DeleteRequest.of(d -> d
                .index(indexName)
                .id(documentId)
                .refresh(Refresh.True) // 변경 사항을 즉시 반영
        );

        try {
            DeleteResponse response = client.delete(request);

            if (response.result() == Result.Deleted) {
                System.out.println("Document deleted.");
            } else if (response.result() == Result.NotFound) {
                System.out.println("Document does not exist.");
            }

        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }

}
