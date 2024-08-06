import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchVersionInfo;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;
import org.opensearch.client.opensearch.generic.OpenSearchGenericClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        String host = "10.81.208.51";
        int port = 8200;
        // Client 초기화 (OpenSearch API 처리의 기본이 되는 client와 JSON 문자열 처리를 위한 generic client)
        OpenSearchClient client = OpenSearchConnectionManager.getInstance(host, port).getClient();
        OpenSearchAsyncClient asyncClient = OpenSearchConnectionManager.getInstance(host, port).getAsyncClient();
        OpenSearchGenericClient genericClient = OpenSearchConnectionManager.getInstance(host, port).getGenericClient();

        try {
            // OpenSearch 버전 확인
            OpenSearchVersionInfo version = client.info().version();
            System.out.printf("Server: %s@%s\n", version.distribution(), version.number());
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }

        // index 생성/삭제 테스트
        IndexSample indexSample = new IndexSample(client);
        indexSample.deleteIndex("sample-index");
        indexSample.createIndexWithJsonString(genericClient, "sample-index");
        indexSample.createIndex("sample-index");
        indexSample.createIndexFluentDsl("sample-index");

        // Bulk 도큐먼트 테스트
        BulkSample bulkSample = new BulkSample(client);
        bulkSample.bulkInsert("sample-index");
        bulkSample.bulkUpsert("sample-index");

        // 도큐먼트 아이디를 이용한 단일 도큐먼트 테스트
        SingleDocumentSample sdSample = new SingleDocumentSample(client, asyncClient);
        sdSample.deleteDocument("sample-index", "doc1"); // 도큐먼트 삭제하기
        sdSample.singleDocumentDSLwithOf("sample-index", "doc1"); // 도큐먼트 생성 혹은 갱신
        sdSample.retrieveSingleDocument("sample-index", "doc1"); // 도큐먼트 읽어오기

        // 도큐먼트 searchWithTerm 테스트
        SearchDocumentsSample searchSample = new SearchDocumentsSample(client);
        searchSample.searchWithTerm("sample-index");
        Map<String, Object> filterMap = new HashMap<>();
        filterMap.put("counter", "15U");
        filterMap.put("objHash", 1113030459);
        searchSample.searchWithTerm("sample-index", filterMap);

        // 도큐먼트 searchWithTerms 테스트
        searchSample.searchWithTerms("sample-index");

        // scroll 테스트
        ScrollSample scrollSample = new ScrollSample(client);
        scrollSample.search("sample-index");

        // Aggregation 테스트
        AggregationSample aggSample = new AggregationSample(client);
        aggSample.search("sample-index");

        // Composite Aggregation 테스트
        CompositeAggregationSample compAggSample = new CompositeAggregationSample(client);
        compAggSample.search("sample-index");

        // Composite Aggregation 해서 패이징하기 테스트
        CompositeAggregationPaginationSample compAggPageSample = new CompositeAggregationPaginationSample(client);
        compAggPageSample.search("sample-index");

        // 어플리케이션 종료시 client를 close하기
        Runtime.getRuntime().addShutdownHook(new Thread(() -> OpenSearchConnectionManager.getInstance(host, port).close()));
    }
}
