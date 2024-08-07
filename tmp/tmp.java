import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import scouter.db.elastic.vo.EndUserProfileVo;
import scouter.io.DataInputX;
import scouter.lang.TextTypes;
import scouter.lang.args.EndUserXLogProfileSearchArgs;
import scouter.lang.step.ResourceStep;
import scouter.lang.step.Step;
import scouter.server.Logger;
import scouter.server.Configure;
import scouter.server.util.QueryUtil;
import scouter.util.Base64;
import scouter.util.DateUtil;
import tuna.server.db.common.TextSearchHelper;
import tuna.server.db.common.TextSearchInfo;
import tuna.server.db.common.elastic.ConnectionManager;
import tuna.server.db.rd.IEndUserProfileRD;
import tuna.server.db.rd.factory.TextRDFactory;
import tuna.server.text.cache.LocalTextCache;

import java.util.*;

public class EndUserProfileESRD implements IEndUserProfileRD {

    private Configure conf;
    private TextSearchHelper textSearchHelper;

    public EndUserProfileESRD()
    {
        conf = Configure.getInstance();
        textSearchHelper = TextSearchHelper.getInstance();
    }

    @Override
    public Map<String,Object> ReadEndUserProfile(EndUserXLogProfileSearchArgs args) {
        if(conf.support_old_enduser) {
            return ReadEndUserProfileOrg(args);
        }
        Map<String, Object> result = new HashMap<String, Object>();

        List<Map<String, Object>> profile = new ArrayList<Map<String, Object>>();

        try {
            List<String> timeList = DateUtil.getSearchRangeTimeForTx("enduser-profile-", args.startTime);
            if(timeList == null) return result;
            String[] indexes = timeList.toArray(new String[timeList.size()]);

            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.size(conf.es_query_fetch_size);
            String[] includeFields = new String[]{"blob"};
            String[] excludeFields = new String[]{"objHash"};
            builder.fetchSource(includeFields,excludeFields);
            builder.query(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termsQuery("uuid", new String[]{args.uuid})))
                    .size(10000);
            //builder.sort("start", SortOrder.ASC);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
            if (conf.print_es_query) {
                Logger.println(builder.toString());
            }

            if(args.error != 0L){
                SearchRequest textSearchRequest = new SearchRequest("text-info");
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termsQuery("hash", new Long[]{args.error}))
                        .filter(QueryBuilders.termsQuery("div", new String[]{TextTypes.ERROR})));
                searchSourceBuilder.fetchSource("text", null);
                textSearchRequest.source(searchSourceBuilder);

                SearchResponse textResponse = client.search(textSearchRequest, RequestOptions.DEFAULT);
                String errorText = "";
                if (textResponse.getHits().getHits().length > 0) {
                    errorText = textResponse.getHits().getHits()[0].getSourceAsMap().get("text").toString();
                }
                result.put("error", errorText);
            }

            SearchRequest searchRequest = new SearchRequest(indexes);
            searchRequest.source(builder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            //  조회결과가 없는 경우 Return
            if( searchHits.length == 0 ) return result;
            List<Step> stepList = new ArrayList<>();
            String strblob = (String) searchResponse.getHits().getAt(0).getSourceAsMap().get("blob");
            byte[] profileBuffer = Base64.decode(strblob);
            DataInputX in = new DataInputX(profileBuffer);
            while (in.available() > 0) {
                scouter.lang.step.Step step = in.readStep();
                stepList.add(step);
            }
            Collections.sort(stepList);
            TextSearchInfo textSearchInfo = new TextSearchInfo();
            List<EndUserProfileVo> endUserProfileVoList = new ArrayList<>();

            for(int i = 0; i < stepList.size();i++) {
                ResourceStep step = (ResourceStep) stepList.get(i);
                EndUserProfileVo endUserProfileVo = new EndUserProfileVo();
                endUserProfileVo.uuid = args.uuid;
                endUserProfileVo.duration = step.elapsed;
                endUserProfileVo.hash = step.hash;
                if(endUserProfileVo.hash != 0) {
                    endUserProfileVo.name = textSearchHelper.searchText(textSearchInfo, TextTypes.RESOURCE, endUserProfileVo.hash);
                }
                endUserProfileVo.start = step.start_time;
                endUserProfileVo.type = step.type;
                if(i==0){
                    endUserProfileVo.gap = 0;
                }else{
                    //endUserProfileVo.gap = Integer.parseInt(searchHits[i].field("start").getValue().toString()) - Integer.parseInt(searchHits[i-1].field("start").getValue().toString());
                    endUserProfileVo.gap = step.start_time - stepList.get(i-1).start_time;
                }
                if(endUserProfileVo.duration >= conf.enduser_profile_read_lower_bound_ms ) {
                    endUserProfileVoList.add(endUserProfileVo);
                }
            }
            LocalTextCache localTextCache = null;
            if(textSearchInfo.getSize() > 0) {
                localTextCache = TextRDFactory.getTextRD().getString(textSearchInfo);
            }

            int index = 1;
            for(EndUserProfileVo vo : endUserProfileVoList) {
               if(localTextCache != null && vo.hash != 0 && vo.name == null) {
                    vo.name = localTextCache.get(TextTypes.RESOURCE, vo.hash);
                }
                //if(vo.name == null) continue;
                Map<String, Object> innerMap = new HashMap<String, Object>();
                innerMap.put("index" , index);
                if(vo.name != null) innerMap.put("name",vo.name);
                innerMap.put("uuid",vo.uuid);
                innerMap.put("duration",String.valueOf(vo.duration));
                innerMap.put("start",String.valueOf(vo.start));
                innerMap.put("type",vo.type);
                innerMap.put("gap", vo.gap);
                profile.add(innerMap);
                index ++;
            }
            result.put("profile", profile);
            result.put("total", profile.size());

            if(conf.print_es_query_result) {
                QueryUtil.print(profile);
            }

         }catch(Exception ex) {
            Logger.println(ex);
        }
        return result;
    }

    @Override
    public Map<String,Object> ReadEndUserProfileOrg(EndUserXLogProfileSearchArgs args) {

        Map<String, Object> result = new HashMap<String, Object>();

        List<Map<String, Object>> profile = new ArrayList<Map<String, Object>>();

        try {
            String[] colList = new String[]{ "uuid"
                    , "duration"
                    , "type"
                    , "hash"
                    , "start" };

            List<String> timeList = DateUtil.getSearchRangeTimeForTx("enduser-profile-", args.startTime);
            if(timeList == null) return result;
            String[] indexes = timeList.toArray(new String[timeList.size()]);

            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.size(conf.es_query_fetch_size);
            builder.query(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termsQuery("uuid", new String[]{args.uuid})))
                    .size(10000)
                    .fetchSource(false);

            builder.sort("start", SortOrder.ASC);

            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();

            for (String col : colList) {
                builder.docValueField(col);
            }

            if (conf.print_es_query) {
                Logger.println(builder.toString());
            }

            if(args.error != 0L){
                SearchRequest textSearchRequest = new SearchRequest("text-info");
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("hash", new Long[]{args.error})).filter(QueryBuilders.termsQuery("div", new String[]{"enduser"})));
                searchSourceBuilder.fetchSource("text", null);
                textSearchRequest.source(searchSourceBuilder);

                SearchResponse textResponse = client.search(textSearchRequest, RequestOptions.DEFAULT);
                String errorText = "";
                if (textResponse.getHits().getHits().length > 0) {
                    errorText = textResponse.getHits().getHits()[0].getSourceAsMap().get("text").toString();
                }
                result.put("error", errorText);
            }

            SearchRequest searchRequest = new SearchRequest(indexes);
            searchRequest.source(builder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            //  조회결과가 없는 경우 Return
            if( searchHits.length == 0 ) return result;
            TextSearchInfo textSearchInfo = new TextSearchInfo();
            List<EndUserProfileVo> endUserProfileVoList = new ArrayList<>();
            //  Data Process
            for(int i = 0; i < searchHits.length; i++) {
                EndUserProfileVo endUserProfileVo = new EndUserProfileVo();
//                long startTime = Long.parseLong(searchHits[i].field("start").getValue().toString());
//                long hash = Long.parseLong(searchHits[i].field("hash").getValue().toString());

                endUserProfileVo.uuid = searchHits[i].field("uuid").getValue();
                endUserProfileVo.duration = Long.parseLong(searchHits[i].field("duration").getValue().toString());
                //endUserProfileVo.hash =  Integer.parseInt(searchHits[i].field("hash").getValue().toString());
                endUserProfileVo.hash = searchHits[i].field("hash").getValue();
                if(endUserProfileVo.hash != 0) {
                    endUserProfileVo.name = textSearchHelper.searchText(textSearchInfo, TextTypes.RESOURCE, endUserProfileVo.hash);
                }
                endUserProfileVo.start = Long.parseLong(searchHits[i].field("start").getValue().toString());
                endUserProfileVo.type = searchHits[i].field("type").getValue();
                if(i==0){
                    endUserProfileVo.gap = 0;
                }else{
                    endUserProfileVo.gap = Integer.parseInt(searchHits[i].field("start").getValue().toString()) - Integer.parseInt(searchHits[i-1].field("start").getValue().toString());
                }
                if(endUserProfileVo.duration >= conf.enduser_profile_read_lower_bound_ms ) {
                    endUserProfileVoList.add(endUserProfileVo);
                }
            }
            LocalTextCache localTextCache = null;
            if(textSearchInfo.getSize() > 0) {
                localTextCache = TextRDFactory.getTextRD().getString(textSearchInfo);
            }

            int index = 1;
            for(EndUserProfileVo vo : endUserProfileVoList) {
                if(localTextCache != null && vo.hash != 0 && vo.name != null) {
                    vo.name = localTextCache.get(TextTypes.RESOURCE, vo.hash);
                }
                //if(vo.name == null) continue;
                Map<String, Object> innerMap = new HashMap<String, Object>();
                innerMap.put("index" , index);
                if(vo.name != null) innerMap.put("name",vo.name);
                innerMap.put("uuid",vo.uuid);
                innerMap.put("duration",String.valueOf(vo.duration));
                innerMap.put("start",String.valueOf(vo.start));
                innerMap.put("type",vo.type);
                innerMap.put("gap", vo.gap);
                profile.add(innerMap);
                index ++;
            }
            result.put("profile", profile);
            result.put("total", profile.size());

            if(conf.print_es_query_result) {
                QueryUtil.print(profile);
            }

        }catch(Exception ex) {
            Logger.println(ex);
        }
        return result;
    }

    public  static void main(String[] args) throws Exception{


    }

}



