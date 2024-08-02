import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.client.opensearch.generic.OpenSearchClientException;

import java.io.IOException;
import java.util.ArrayList;

public class BulkSample {

    private final OpenSearchClient client;

    public BulkSample(OpenSearchClient client) {
        this.client = client;
    }

    public void bulkInsert(String indexName) {

        System.out.println("= BulkSample.bulkInsert =");

        ArrayList<BulkOperation> ops = new ArrayList<>();

        try {
            for (int i=0; i<20; i++) {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode doc = mapper.createObjectNode();
                String id = "id" + i;
                doc.put("counter", "15U");
                doc.put("ctime", System.currentTimeMillis());
                doc.put("objHash", 1113030459+(i%4));
                doc.put("value", i);
                ops.add(new BulkOperation.Builder().index(
                        IndexOperation.of(io -> io.index(indexName).id(id).document(doc))
                ).build());
            }

            BulkRequest.Builder bulkRequest
                    = new BulkRequest.Builder().operations(ops).refresh(Refresh.True);
            BulkResponse bulkResponse = client.bulk(bulkRequest.build());

            System.out.println("Bulk Insert response items: " + bulkResponse.items().size());
        } catch (OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }

    }

    public void bulkUpsert(String indexName) {

        System.out.println("= BulkSample.bulkUpsert =");
        System.out.println("= The index to be upsert must have _source enabled. =");

        ArrayList<BulkOperation> ops = new ArrayList<>();

        try {
            for (int i=0; i<20; i++) {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode doc = mapper.createObjectNode();
                String id = "id" + i;
                ///doc.put("counter", "AAA");
                ///doc.put("ctime", 1722326835000L);
                ///doc.put("objHash", 1113030459+(i%4));
                doc.put("value", 1000 + i*1000); // 업데이트할 필드, 나머지 필드는 기존 값 유지
                ops.add(new BulkOperation.Builder()
                        .update(UpdateOperation.of(u -> u
                                .index(indexName)
                                .id(id) // 도큐먼트 아이디
                                .document(doc) // 도큐먼트
                                .docAsUpsert(true)) // 도큐먼트를 upsert 대상으로 사용함 = 도큐먼트가 없으면 입력하고, 있으면 갱신함
                        ).build());
            }

            BulkRequest.Builder bulkRequest = new BulkRequest.Builder()
                    .operations(ops)
                    .refresh(Refresh.True); // 데이터 입력이 즉시 전체 노드에 반영되도록 설정
            BulkResponse bulkResponse = client.bulk(bulkRequest.build());

            System.out.println("Bulk Insert response items: " + bulkResponse.items().size());

        } catch (OpenSearchException | OpenSearchClientException | IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
