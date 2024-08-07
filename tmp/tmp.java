import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import scouter.config.CommonConfigure;
import scouter.server.Logger;
import scouter.server.core.cache.TextCache;
import tuna.server.db.common.TextSearchArgs;
import tuna.server.db.common.TextSearchInfo;
import tuna.server.db.common.elastic.ConnectionManager;
import tuna.server.db.rd.ITextRD;
import tuna.server.text.cache.LocalTextCache;

import java.util.List;

public class TextESRD implements ITextRD {
    private CommonConfigure conf;
    private static TextESRD inst;

    public static TextESRD getInstance() {
        if (inst == null) {
            synchronized (TextESRD.class) {
                if (inst == null) {
                    inst = new TextESRD();

                }
            }
        }
        return inst;
    }

    private TextESRD() {
        conf = CommonConfigure.getInstance();
    }

    public String getString(long hash) {
        return null;
    }

    @Override
    public String getString(long from, long to, String div, long hash) {
        try {
            String val = TextCache.get(div, hash);
            if (val != null) {
                return val;
            }
            /*List<String> timeList = DateUtil.getSearchRangeTime("text-info",from,to);
            if(timeList == null) return null;

            String[] indexes = timeList.toArray(new String[timeList.size()]);*/

            SearchRequest searchRequest = new SearchRequest("text-info");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            String[] includeFields = new String[]{"text"};
            String[] excludeFields = new String[]{"div", "hash"};
            sourceBuilder.size(conf.es_query_fetch_size);
            sourceBuilder.fetchSource(includeFields, excludeFields);


            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery("div", div))
                    .filter(QueryBuilders.termsQuery("hash", new Long[]{hash}));
            if (conf.print_es_query) {
                Logger.println(queryBuilder.toString());
            }
            sourceBuilder.query(queryBuilder);
            searchRequest.source(sourceBuilder);
            RestHighLevelClient restHighLevelClient = ConnectionManager.getInstance().getReadClient();
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getHits().length > 0) {
                String txt = (String) searchResponse.getHits().getAt(0).getSourceAsMap().get("text");
                TextCache.put(div, hash, txt);
                return txt;
            }

        } catch (Exception ex) {
            Logger.println(ex);

        }
        return null;

    }

    @Override
    public LocalTextCache getString(TextSearchInfo searchInfo) throws Exception {
        if (searchInfo.getSize() == 0) return null;
        LocalTextCache localTextCache = new LocalTextCache();
        int execPerCount = 100;
        try {
            List<TextSearchArgs> argList = searchInfo.getArgsList();
            int execCount = (argList.size() / execPerCount) + 1;
            int argsIndx = 0;
            int remain = argList.size();
            int loopCount = 0;
            while (remain > 0) {
                SearchRequest searchRequest = new SearchRequest("text-info");
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                String[] includeFields = new String[]{"div", "hash", "text"};
                String[] excludeFields = new String[]{""};
                sourceBuilder.size(conf.es_query_fetch_size);
                sourceBuilder.fetchSource(includeFields, excludeFields);
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                List<QueryBuilder> queryBuildersList = boolQueryBuilder.should();
                for (int j = loopCount * execPerCount; j < loopCount * execPerCount + execPerCount; j++) {
                    if (argsIndx >= argList.size()) {
                        break;
                    }
                    TextSearchArgs args = argList.get(j);
                    queryBuildersList.add(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("div", args.div))
                            .filter(QueryBuilders.termQuery("hash", args.hash)));
                    argsIndx++;
                    remain--;
                }
                loopCount++;
                sourceBuilder.query(boolQueryBuilder);
                searchRequest.source(sourceBuilder);
                if (conf.print_es_query) {
                    Logger.println(boolQueryBuilder.toString());
                }
                RestHighLevelClient restHighLevelClient = ConnectionManager.getInstance().getReadClient();
                SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
                if (searchResponse.getHits().getHits().length > 0) {
                    for (int k = 0; k < searchResponse.getHits().getHits().length; k++) {
                        String txt = (String) searchResponse.getHits().getAt(k).getSourceAsMap().get("text");
                        String div = (String) searchResponse.getHits().getAt(k).getSourceAsMap().get("div");
                        int hash = (int) searchResponse.getHits().getAt(k).getSourceAsMap().get("hash");
                        TextCache.put(div, hash, txt);
                        localTextCache.put(div, hash, txt);
                    }
                }
            }

        } catch (Exception ex) {
            throw ex;
        }
        return localTextCache;
    }

    @Override
    public String getString(String div, long hash) {
        try {
            String val = TextCache.get(div, hash);
            if (val != null) {
                return val;
            }
            SearchRequest searchRequest = new SearchRequest("text-info");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            String[] includeFields = new String[]{"text"};
            String[] excludeFields = new String[]{"div", "hash"};
            sourceBuilder.size(conf.es_query_fetch_size);
            sourceBuilder.fetchSource(includeFields, excludeFields);

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery("div", div))
                    .filter(QueryBuilders.termsQuery("hash", new Long[]{hash}));
            if (conf.print_es_query) {
                Logger.println(queryBuilder.toString());
            }
            sourceBuilder.query(queryBuilder);
            searchRequest.source(sourceBuilder);
            RestHighLevelClient restHighLevelClient = ConnectionManager.getInstance().getReadClient();
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getHits().length > 0) {
                String txt = (String) searchResponse.getHits().getAt(0).getSourceAsMap().get("text");
                TextCache.put(div, hash, txt);
                return txt;
            }

        } catch (Exception ex) {
            Logger.println(ex);

        }
        return null;

    }

}
