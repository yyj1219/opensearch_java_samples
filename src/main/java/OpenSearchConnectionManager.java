import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.generic.OpenSearchGenericClient;
import org.opensearch.client.opensearch.generic.OpenSearchGenericClient.ClientOptions;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.util.concurrent.TimeUnit;

public class OpenSearchConnectionManager {

    private static String hostIp = "localhost";
    private static int servicePort = 9200;
    private static OpenSearchConnectionManager instance;

    private final OpenSearchTransport transport;
    private final OpenSearchClient client;
    OpenSearchGenericClient genericClient;

    OpenSearchAsyncClient asyncClient;

    private OpenSearchConnectionManager() {

        HttpHost host = new HttpHost("http", hostIp, servicePort);

        /*
        *****************************************
        ****** ApacheHttpClient5 사용하기 전 ******
        * 1. RestClient 생성
        * 2. RestClientTransport 생성
        *****************************************/
        /*
        // Create the low-level client
        // 개별 요청의 구성 옵션
        // 전반적인 HTTP 클라이언트 구성 (커넥션 풀 크기, 커넥션 매니저 등)
        RestClient restClient = RestClient
                .builder("http://localhost:8200")
                .setRequestConfigCallback( // Apache HttpClient에 RequestConfig 객체를 사용자 정의할 수 있게 함
                        // new RestClientBuilder.RequestConfigCallback() 를 람다식으로 변경 - 개별 요청의 구성 옵션
                        builder -> {
                            return builder
                                    .setSocketTimeout(60000) // 데이터를 읽기 위한 타임아웃 (milliseconds)
                                    .setConnectTimeout(5000); // 서버에 연결하기 위한 타임아웃 (milliseconds)
                        }
                )
                .setHttpClientConfigCallback( // Apache HttpClient에 HttpAsyncClientBuilder 객체를 사용자 정의할 수 있게 함
                        // new RestClientBuilder.HttpClientConfigCallback() 를 람다식으로 변경 - 전반적인 HTTP 클라이언트 구성 (커넥션 풀 크기, 커넥션 매니저 등)
                        httpAsyncClientBuilder -> {
                            return httpAsyncClientBuilder
                                    .setMaxConnPerRoute(100) // 최대 라우트당 연결 수
                                    .setMaxConnTotal(100); // 최대 연결 수
                        }
                )
                .build();

        // Create the transport
        this.transport = new RestClientTransport(restClient, (JsonpMapper) new ObjectMapper().registerModule(new JavaTimeModule()));
        */

        /*
         *****************************************
         ****** ApacheHttpClient5 사용 후 ******
         * OpenSearch 3.0.0 릴리즈부터는 HTTP/2 지원됨
         * ApacheHttpClient5 사용을 권장하므로 아래와 같이 Transport 생성
         *****************************************
         */

        // Apache HTTP 5 를 위한 requestConfig
        RequestConfig requestConfig = RequestConfig.custom()
                .setResponseTimeout( 5000, TimeUnit.MILLISECONDS ) // Socket Timeout : Connection 생성 마다 속성이 다시 Override되므로 socketConfig에 둘 필요 없다
                .setConnectionRequestTimeout( 5000, TimeUnit.MILLISECONDS ) // Connection Request Timeout : the timeout in milliseconds used when requesting a connection from the connection manager.
                .setExpectContinueEnabled( false ) // Activates 'Expect: 100-Continue' handshake : Expect 헤더 포함 (사용안함)
                .setRedirectsEnabled( false ) // Redirect 금지
                .build();

        // Apache HTTP 5 를 위한 connectionManager
        PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnTotal(100)
                .setMaxConnPerRoute(100)
                .build();

        // Create the transport with requestConfig and connectionManager
        this.transport = ApacheHttpClient5TransportBuilder
                .builder(host)
                .setHttpClientConfigCallback(
                        httpAsyncClientBuilder -> httpAsyncClientBuilder
                                .setDefaultRequestConfig(requestConfig)
                                .setConnectionManager(connectionManager))
                .setMapper(new JacksonJsonpMapper())
                .build();

        // Create the API client
        this.client = new OpenSearchClient(transport);

        // Create the Async client
        this.asyncClient = new OpenSearchAsyncClient(transport);

        // Create the Generic client
        this.genericClient = this.client.generic().withClientOptions(ClientOptions.throwOnHttpErrors());
    }

    /**
     * 싱글톤 패턴을 적용한 OpenSearchClientManager의 인스턴스 반환
     * @return OpenSearchClientManager
     */
    public static synchronized OpenSearchConnectionManager getInstance(String ip, int port) {
        if (instance == null) {
            if (ip != null) {
                hostIp = ip;
            }
            if (port > 0) {
                servicePort = port;
            }
            instance = new OpenSearchConnectionManager();
        }
        return instance;
    }

    /**
     * OpenSearchClient 반환
     * @return OpenSearchClient
     */
    public OpenSearchClient getClient() {
        return client;
    }

    /**
     * JSON 문자열을 직접 처리할 수 있는 OpenSearchGenericClient 반환
     * @return
     */
    public OpenSearchGenericClient getGenericClient() {
        return genericClient;
    }

    public OpenSearchAsyncClient getAsyncClient() {
        return asyncClient;
    }

    /**
     * Transport 개체는 OpenSearch 클러스터와 연결되어 있으며, 네트워크 연결과 같은 기본 리소스를 해제하려면 명시적으로 닫혀야 합니다.
     */
    public void close() {
        try {
            if (transport != null) {
                transport.close();
            }
            System.out.println("OpenSearchTransport is closed");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
