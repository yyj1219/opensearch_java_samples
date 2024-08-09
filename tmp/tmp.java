package tuna.server.db.rd.elastic;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.*;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.ParsedExtendedStats;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import scouter.config.CommonConfigure;
import scouter.db.elastic.metering.XLogHistoMetering;
import scouter.db.elastic.vo.EndUserVo;
import scouter.db.elastic.vo.HeatMapBucket;
import scouter.db.elastic.vo.XLogHistoVo;
import scouter.lang.TextTypes;
import scouter.lang.args.TopTxSearchArgs;
import scouter.lang.args.XLogInfoSearchArgs;
import scouter.lang.counters.E2ETypeConstants;
import scouter.lang.pack.XLogPack;
import scouter.server.Logger;
import scouter.server.util.QueryUtil;
import scouter.util.DateUtil;
import tuna.server.db.common.DateHistogramIntervalManager;
import tuna.server.db.common.TextSearchHelper;
import tuna.server.db.common.TextSearchInfo;
import tuna.server.db.common.elastic.ConnectionManager;
import tuna.server.db.rd.IEndUserTxRD;
import tuna.server.db.rd.factory.TextRDFactory;
import tuna.server.text.cache.LocalTextCache;

import java.io.IOException;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class EndUserTxESRD implements IEndUserTxRD {

    private CommonConfigure conf;
    private final int XLOG_HISTO_TIME_BUCKETS = 150;
    private TextSearchHelper textSearchHelper;
    public EndUserTxESRD()  {
        conf = CommonConfigure.getInstance();
        textSearchHelper = TextSearchHelper.getInstance();
    }


    //  EndUser Xlog 전체를 가져오는 Method
    //  Scroll 을 이용해 전체 데이터를 가져옴
    //  2019.07.22  최대 1,000 건만 조회하도록 변경된 LoadEndUserXLogInfo 를 사용함
    //  추후 전체데이터를 요청할 경우에 대비하여 Method를 남겨두고 Deprecate 함
    public  List<Map<String, Object >> LoadEndUserXLogInfo(XLogInfoSearchArgs args) {
        try {
            List<String> timeList = DateUtil.getSearchRangeTime("enduser-info-",args.from,args.to);
            if(timeList == null) return null;

            String[] indexes = timeList.toArray(new String[timeList.size()]);
            SearchRequest searchRequest = new SearchRequest(indexes);
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.size(conf.es_query_fetch_size);      // Result 는 1000개로 제한
            if(args.toElapsed == -1) {
                args.toElapsed = Integer.MAX_VALUE;
            }
            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("endTime").gte(args.from).lte(args.to))
                    .filter(QueryBuilders.rangeQuery("elapsedTime").gte(args.fromElapsed).lte(args.toElapsed))
                    .filter(QueryBuilders.termsQuery("objHash",args.objList));

            if(args.serviceHash != 0L){
                ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.matchQuery("serviceHash",args.serviceHash));
            }
            ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("type", E2ETypeConstants.ERROR));

            Map<String, Object> filters = args.fileterMap;
            Iterator<String> it = filters.keySet().iterator();
            while(it.hasNext()) {
                String key = it.next();
                if(key.equals("elapsed")) {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.rangeQuery("elapsedTime").gte(filters.get(key)));
                } else if (key.equals("error")) {
                    ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("errorHash", 0));
                } else {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.termsQuery(key,filters.get(key)));
                }
            }

            // Order Set
            String order = "elapsedTime";
            SortOrder orderCmd = SortOrder.DESC;
            if( (args.order != null) && (!args.order.equals(""))) {
                order = args.order;
            }
            if( (args.orderCmd != null) && (args.orderCmd.equals("asc")) ) {
                orderCmd = SortOrder.ASC;
            }

            builder.sort(order, orderCmd)
                    .fetchSource(false)
                    .docValueField("domProcessingTime")
                    .docValueField("elapsedTime")
                    .docValueField("endTime")
                    .docValueField("errorHash")
                    .docValueField("networkTime")
                    .docValueField("objHash")
                    .docValueField("objName")
                    .docValueField("serverTime")
                    .docValueField("serviceHash")
                    .docValueField("timeToDomComplete")
                    .docValueField("timeToDomInteracitve")
                    .docValueField("loadTime")
                    .docValueField("connectionTime")
                    .docValueField("sslConnectionTime")
                    .docValueField("dnsLookupTime")
                    .docValueField("timeToFirstByteRecv")
                    .docValueField("type")
                    .docValueField("userIp")
                    .docValueField("gxid")
                    .docValueField("agentHash")
                    .docValueField("pageProcessingTime")
                    .docValueField("os")
                    .docValueField("browser")
                    .docValueField("uuid");
            //start time은 연산하여 넣기

            if(conf.print_es_query) {
                Logger.println(builder.toString());
            }

            builder.query(queryBuilder);
            searchRequest.source(builder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<Map<String, Object >> resultList = new ArrayList<Map<String, Object > >();
            SearchHit[] searchHits =  searchResponse.getHits().getHits();
            TextSearchInfo textSearchInfo = new TextSearchInfo();
            for(int i = 0; i < searchHits.length; i++) {
                Map<String, Object > valueMap = new HashMap<String, Object>();
                for(DocumentField field : searchHits[i].getFields().values()) {
                    String fieldName = field.getName();
                    switch (fieldName) {
                        case "endTime":
                            valueMap.put("endTime",Long.parseLong(field.getValue().toString()) );
                            break;
                        case "serviceHash":
                            int serviceHash = field.getValue();
                            valueMap.put(fieldName,serviceHash);
                            valueMap.put("service",textSearchHelper.searchText(textSearchInfo, TextTypes.SERVICE, serviceHash));
                            break;
                        case "errorHash":
                            int errorHash = field.getValue();
                            valueMap.put("error",errorHash);
                            valueMap.put("errorMsg",textSearchHelper.searchText(textSearchInfo,TextTypes.ERROR,errorHash));
                            break;
                        default:
                            valueMap.put(fieldName,field.getValue());
                            break;
                    }
                }
                long endTime = Long.parseLong(valueMap.get("endTime").toString());
                int elapsed = Integer.parseInt(valueMap.get("elapsedTime").toString());
                valueMap.put("startTime",endTime - elapsed );
                resultList.add(valueMap);
            }
            LocalTextCache localTextCache = null;
            if(textSearchInfo.getSize() > 0) {
                localTextCache = TextESRD.getInstance().getString(textSearchInfo);
                if(localTextCache != null) {
                    for (Map<String, Object> map : resultList) {
                        if (map.get("serviceHash") != null && map.get("service") == null) {
                            map.put("service",localTextCache.get(TextTypes.SERVICE, Integer.parseInt(map.get("serviceHash").toString())));
                        }
                        if(map.get("error") != null && map.get("errorMsg") == null) {
                            map.put("errorMsg",localTextCache.get(TextTypes.ERROR, Integer.parseInt(map.get("error").toString())));
                        }
                    }
                }
            }
            if(conf.print_es_query_result) {
                QueryUtil.print(resultList);
            }
            return resultList;
        }catch (Exception ex) {
            Logger.println(ex);

        }
        return null;
    }


    public Map<String, Object> LoadEndUserXLogHistogram(XLogInfoSearchArgs args) throws  Exception{
        try {
            List<String> timeList = DateUtil.getSearchRangeTime("enduser-info-",args.from,args.to);
            if(timeList == null) return null;

            String[] indexes = timeList.toArray(new String[timeList.size()]);
            SearchRequest searchRequest = new SearchRequest(indexes);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("endTime").gte(args.from).lt(args.to))
                    .filter(QueryBuilders.termsQuery("objHash",args.objList));
            Map<String, Object> filters = args.fileterMap;
            Iterator<String> it = filters.keySet().iterator();
            while(it.hasNext()) {
                String key = it.next();
                if(key.equals("elapsed")) {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.rangeQuery("elapsedTime").gte(filters.get(key)));
                } else if (key.equals("error")) {
                    ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("errorHash", 0));
                } else {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.termsQuery(key,filters.get(key)));
                }
            }

            if(args.serviceHash != 0L){
                ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.matchQuery("serviceHash",args.serviceHash));
            }
            ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("type", E2ETypeConstants.ERROR));

            // 2. set composite aggregation.
            List<CompositeValuesSourceBuilder<?>> compositeValuesSourceBuilderList = new ArrayList<CompositeValuesSourceBuilder<?>>();

            long searchInterval = args.to - args.from;
            long histogramTimeInterval;  // ms 단위
            histogramTimeInterval = searchInterval / this.XLOG_HISTO_TIME_BUCKETS;
            Logger.println("histogramTimeInterval : " + histogramTimeInterval);

            // set histogram unit : time, elapsed
            DateHistogramValuesSourceBuilder dateHistogramValuesSourceBuilder = new DateHistogramValuesSourceBuilder("time");
            dateHistogramValuesSourceBuilder.interval(histogramTimeInterval).field("endTime").valueType(ValueType.LONG).order("ASC");;
            dateHistogramValuesSourceBuilder.timeZone(ZoneId.of(args.timeZone));

            HistogramValuesSourceBuilder histogramValuesSourceBuilder = new HistogramValuesSourceBuilder("elapsed");
            histogramValuesSourceBuilder.interval(conf.xlog_hist_elapsed_interval).field("elapsedTime").valueType(ValueType.LONG);

            compositeValuesSourceBuilderList.add(dateHistogramValuesSourceBuilder);
            compositeValuesSourceBuilderList.add(histogramValuesSourceBuilder);


            CompositeAggregationBuilder aggBuilder = AggregationBuilders.composite("aggs", compositeValuesSourceBuilderList).size(conf.es_composite_bucket_size);
            aggBuilder.subAggregation(AggregationBuilders.sum("errSum").field("errorHash")).size(conf.es_composite_bucket_size);

            // 3. set query
            sourceBuilder.query(queryBuilder)
                    .fetchSource(false) // no source
                    .aggregation(aggBuilder)
                    .size(0); // no hits

            if(conf.print_es_query) {
                Logger.println(sourceBuilder.toString());
            }
            searchRequest.source(sourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
            boolean initialSearch = true;
            int bucketSize = 0;
            List<XLogHistoVo> list = new ArrayList<XLogHistoVo>();
            ParsedComposite parsedComposite = null;

            do{
                if(!initialSearch){
                    Map<String,Object> afterKey = parsedComposite.afterKey();
                    aggBuilder.aggregateAfter(afterKey);
                } else {
                    initialSearch = false;
                }
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                Aggregations aggs = searchResponse.getAggregations();
                if(aggs == null)    break;
                parsedComposite = aggs.get("aggs");

                List<ParsedComposite.ParsedBucket> bucketList = parsedComposite.getBuckets();
                bucketSize = bucketList.size();

                for(ParsedComposite.ParsedBucket bucket : bucketList) {
                    XLogHistoVo vo = new XLogHistoVo();
                    vo.time = (Long)bucket.getKey().get("time");
                    vo.elapsed =  ((Number)bucket.getKey().get("elapsed")).intValue();
                    vo.count = bucket.getDocCount();
                    vo.error = ((Sum)bucket.getAggregations().get("errSum")).getValue();
                    list.add(vo);
                }

            }while(bucketSize > 0);
            if(list.size() == 0){
                return new HashMap<String, Object>(){{ put("xlog", new ArrayList()); }};
            }
            TreeMap<Long, HeatMapBucket[]> result = new XLogHistoMetering().process(list, histogramTimeInterval, args);

            List<Map<String, List>> xlogList = new ArrayList<Map<String, List>>();
            SortedSet<Long> keys = new TreeSet<Long>(result.keySet());
            Iterator<Long> itTime = keys.iterator();
            while(itTime.hasNext()) {
                Long time = itTime.next();
                Map<String, List> innerMap = new HashMap<String, List>();
                List<Integer> innerList = new ArrayList<Integer>();
                HeatMapBucket[] buckets = result.get(time);
                for(int i = 0; i < buckets.length;i++) {
                    HeatMapBucket bucket = buckets[i];
                    if(bucket.error) {
                        bucket.count = bucket.count * -1;
                    }
                    innerList.add(bucket.count);
                }
                innerMap.put(time.toString(), innerList);
                xlogList.add(innerMap);
            }


            //  RESULT PRINT
            Map<String, Object> resultMap = new HashMap<String, Object>();
            resultMap.put("xlog", xlogList);
            if(conf.print_es_query_result) {
                Set<String> keySet = resultMap.keySet();
                for(String key : keySet){
                    Logger.println(key + " : "  + resultMap.get(key) );
                }
            }
            System.out.println("xlogList Size : " + xlogList.size());

            return resultMap;

        }catch (Exception ex) {
            System.out.println(ex.getMessage());
            throw ex;
        }
    }

    @Override
    public List<Map<String, Object>> LoadEndUserTopNTransaction(TopTxSearchArgs args) throws Exception {
        try {
            List<String> timeList = DateUtil.getSearchRangeTime("enduser-info-",args.from,args.to);
            if(timeList == null) return new ArrayList();

            String[] indexes = timeList.toArray(new String[timeList.size()]);
            SearchRequest searchRequest = new SearchRequest(indexes);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.fetchSource(false).size(0);

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("endTime").gte(args.from).lte(args.to))
                    .filter(QueryBuilders.termsQuery("objHash",args.objList))
                    .must(QueryBuilders.matchQuery("type", E2ETypeConstants.DOCUMENT));

            Map<String, Object> filters = args.fileterMap;
            Iterator<String> it = filters.keySet().iterator();
            while(it.hasNext()) {
                String key = it.next();
                if(key.equals("elapsed")) {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.rangeQuery("elapsedTime").gte(filters.get(key)));
                } else if (key.equals("error")) {
                    ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("errorHash", 0));
                } else {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.termsQuery(key,filters.get(key)));
                }
            }

            if(args.serviceHash != 0L){
                ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.matchQuery("serviceHash",args.serviceHash));
            }
            ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("type", E2ETypeConstants.ERROR));

            TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("topN")
                    .field("serviceHash")
                    .size(args.top);

            if(args.shard_size > 0) {
                termsAggregationBuilder.shardSize(args.shard_size);
            }
            termsAggregationBuilder.subAggregation(AggregationBuilders.extendedStats("elapsedTimeStat").field("elapsedTime"));
            termsAggregationBuilder.subAggregation(AggregationBuilders.extendedStats("timeToFirstByteRecvStat").field("timeToFirstByteRecv"));
            termsAggregationBuilder.subAggregation(AggregationBuilders.extendedStats("timeToDomCompleteStat").field("timeToDomComplete"));

            termsAggregationBuilder.subAggregation(AggregationBuilders.count("call").field("serviceHash"));

            termsAggregationBuilder.order(BucketOrder.aggregation("elapsedTimeStat.avg",false));

            searchSourceBuilder.query(queryBuilder);
            searchSourceBuilder.aggregation(termsAggregationBuilder);

            if(conf.print_es_query) {
                Logger.println(searchSourceBuilder.toString());
            }
            searchRequest.source(searchSourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

            //  조회결과가 없는 경우 Empty List Return
            if( searchResponse.getAggregations() == null )    return result;
            if( !searchResponse.getAggregations().getAsMap().containsKey("topN") )    return result;

            Terms terms =  searchResponse.getAggregations().get("topN");

            for(Terms.Bucket bucket : terms.getBuckets()) {
                ParsedExtendedStats elapsedTimeStat =   bucket.getAggregations().get("elapsedTimeStat");
                ParsedExtendedStats timeToFirstByteRecvStat =   bucket.getAggregations().get("timeToFirstByteRecvStat");
                ParsedExtendedStats timeToDomCompleteStat =   bucket.getAggregations().get("timeToDomCompleteStat");

                long hash = bucket.getKeyAsNumber().longValue();

                Map<String, Object> innerMap = new HashMap<String, Object>();
                innerMap.put( "hash", hash );
                innerMap.put( "name", TextRDFactory.getTextRD().getString(args.from,args.to,TextTypes.SERVICE, hash) );
                innerMap.put( "call", ((ValueCount)bucket.getAggregations().get("call")).getValue() );
                innerMap.put( "maxElapsed", elapsedTimeStat.getMaxAsString() );
                innerMap.put( "minElapsed", elapsedTimeStat.getMinAsString() );
                innerMap.put( "avgElapsed", getNumberFormat().format(elapsedTimeStat.getAvg()) );
                innerMap.put( "devElapsed", getNumberFormat().format(elapsedTimeStat.getStdDeviation()) );
                innerMap.put( "maxFirstByte", timeToFirstByteRecvStat.getMaxAsString() );
                innerMap.put( "minFirstByte", timeToFirstByteRecvStat.getMinAsString() );
                innerMap.put( "avgFirstByte", getNumberFormat().format(timeToFirstByteRecvStat.getAvg()) );
                innerMap.put( "devFirstByte", getNumberFormat().format(timeToFirstByteRecvStat.getStdDeviation()) );
                innerMap.put( "maxDomComplete", timeToDomCompleteStat.getMaxAsString() );
                innerMap.put( "minDomComplete", timeToDomCompleteStat.getMinAsString() );
                innerMap.put( "avgDomComplete", getNumberFormat().format(timeToDomCompleteStat.getAvg()) );
                innerMap.put( "devDomComplete", getNumberFormat().format(timeToDomCompleteStat.getStdDeviation()) );
                result.add(innerMap);
            }
            if(conf.print_es_query_result) {
                QueryUtil.print(result);
            }
            return result;
        }catch(Exception ex) {
            throw ex;
        }
    }

    //  Composite Aggregation 을 이용한 Method
    //  비어있는 값을 0으로 채울 수 없음
    //  2019.07.19 : 비어있는 값을 채우기 위해 Term Aggregation 으로 대체
    @Override
    public  Map<String, Object> LoadEndUserTimeHisto2(XLogInfoSearchArgs args) throws Exception {
        try {
            List<String> timeIndexList = DateUtil.getSearchRangeTime("enduser-info-",args.from,args.to);
            if(timeIndexList == null) return new HashMap<String, Object>();

            String[] indexes = timeIndexList.toArray(new String[timeIndexList.size()]);
            SearchRequest searchRequest = new SearchRequest(indexes);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("endTime").gte(args.from).lte(args.to))
                    .filter(QueryBuilders.termsQuery("objHash",args.objList))
                    .filter(QueryBuilders.termsQuery("type", new ArrayList<Integer>(Arrays.asList(E2ETypeConstants.DOCUMENT, E2ETypeConstants.XHR))));

            if(args.serviceHash != 0L){
                ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.matchQuery("serviceHash",args.serviceHash));
            }
            ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("type", E2ETypeConstants.ERROR));

            // 2. set composite aggregation.
            List<CompositeValuesSourceBuilder<?>> compositeValuesSourceBuilderList = new ArrayList<CompositeValuesSourceBuilder<?>>();
            DateHistogramValuesSourceBuilder histogramValuesSourceBuilder = new DateHistogramValuesSourceBuilder("time");

            if((args.to - args.from) > (DateUtil.MILLIS_PER_DAY * 28 )){
                histogramValuesSourceBuilder.dateHistogramInterval(DateHistogramInterval.hours(1));
            } else if ((args.to - args.from) > (DateUtil.MILLIS_PER_DAY * 2)) {
                histogramValuesSourceBuilder.dateHistogramInterval(DateHistogramInterval.minutes(30));
            } else if ((args.to - args.from) > DateUtil.MILLIS_PER_DAY ) {
                histogramValuesSourceBuilder.dateHistogramInterval(DateHistogramInterval.minutes(10));
            } else if ((args.to - args.from) > (DateUtil.MILLIS_PER_HOUR * 6) ) {
                histogramValuesSourceBuilder.dateHistogramInterval(DateHistogramInterval.minutes(5));
            } else  {
                histogramValuesSourceBuilder.dateHistogramInterval(DateHistogramInterval.seconds(30));
            }

            //  Time Aggregation
            histogramValuesSourceBuilder.field("endTime");
            compositeValuesSourceBuilderList.add(histogramValuesSourceBuilder);
            //  Type Aggregation
            compositeValuesSourceBuilderList.add(new TermsValuesSourceBuilder("type").field("type").missingBucket(true));

            CompositeAggregationBuilder aggBuilder = AggregationBuilders.composite("groupby", compositeValuesSourceBuilderList);
            aggBuilder.subAggregation(AggregationBuilders.avg("elapsedTimeStat").field("elapsedTime")).size(conf.es_composite_bucket_size);
            aggBuilder.subAggregation(AggregationBuilders.avg("timeToFirstByteRecvStat").field("timeToFirstByteRecv")).size(conf.es_composite_bucket_size);
            aggBuilder.subAggregation(AggregationBuilders.avg("timeToDomCompleteStat").field("timeToDomComplete")).size(conf.es_composite_bucket_size);

            // 3. set query
            sourceBuilder.query(queryBuilder)
                    .fetchSource(false) // no source
                    .aggregation(aggBuilder)
                    .size(0);  // no hits

            if(conf.print_es_query) {
                Logger.println(sourceBuilder.toString());
            }

            searchRequest.source(sourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
            boolean initialSearch = true;
            int bucketSize = 0;
            ParsedComposite parsedComposite = null;

            List<Long> pageTimeList = new ArrayList<Long>();
            List<Long> pageElapsedList = new ArrayList<Long>();
            List<Long> pageFirstByteList = new ArrayList<Long>();
            List<Long> pageDomCompleteList = new ArrayList<Long>();
            List<Long> xhrTimeList = new ArrayList<Long>();
            List<Long> xhrElapsedList = new ArrayList<Long>();
            List<Long> xhrFirstByteList = new ArrayList<Long>();

            do{
                if(!initialSearch){
                    Map<String,Object> afterKey = parsedComposite.afterKey();
                    aggBuilder.aggregateAfter(afterKey);
                } else {
                    initialSearch = false;
//                    builder = jsonBuilder().startObject().startArray("result");
                }
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                Aggregations aggs = searchResponse.getAggregations();
                if(aggs == null)    break;
                parsedComposite = aggs.get("groupby");

                List<ParsedComposite.ParsedBucket> bucketList = parsedComposite.getBuckets();
                bucketSize = bucketList.size();

                for(ParsedComposite.ParsedBucket bucket : bucketList) {
                    Long time = (Long)bucket.getKey().get("time");
                    int type = Integer.parseInt(bucket.getKey().get("type").toString());
                    Map<String , Aggregation > aggMap  = bucket.getAggregations().getAsMap();
                    for(String key  : aggMap.keySet()) {
                        Object obj = aggMap.get(key);
                        Long value = 0L;
                        if (type == 11) {
                            if(key.equals("elapsedTimeStat")){
                                pageTimeList.add(time);
                                pageElapsedList.add( (long)((Avg)obj).getValue() );
                            }else if(key.equals("timeToFirstByteRecvStat")){
                                pageFirstByteList.add( (long)((Avg)obj).getValue() );
                            }else if(key.equals("timeToDomCompleteStat")){
                                pageDomCompleteList.add( (long)((Avg)obj).getValue() );
                            }
                        } else if (type == 5) {
                            if(key.equals("elapsedTimeStat")){
                                xhrTimeList.add(time);
                                xhrElapsedList.add( (long)((Avg)obj).getValue() );
                            }else if(key.equals("timeToFirstByteRecvStat")){
                                xhrFirstByteList.add( (long)((Avg)obj).getValue() );
                            }
                        }
                    }

                }

            }while(bucketSize > 0);

            Map<String, Object> pageMap = new HashMap<String, Object>();
            pageMap.put("time", pageTimeList);
            pageMap.put("elapsedTimeStat", pageElapsedList);
            pageMap.put("timeToFirstByteRecvStat", pageFirstByteList);
            pageMap.put("timeToDomCompleteStat", pageDomCompleteList);

            Map<String, Object> xhrMap = new HashMap<String, Object>();
            xhrMap.put("time", xhrTimeList);
            xhrMap.put("elapsedTimeStat", xhrElapsedList);
            xhrMap.put("timeToFirstByteRecvStat", xhrFirstByteList);

            Map<String, Object> result = new HashMap<String, Object>();
            result.put("page", pageMap);
            result.put("xhr", xhrMap);

            if(conf.print_es_query_result) {
                QueryUtil.print(result);
            }
            return result;


        }catch (Exception ex) {
            System.out.println(ex.getMessage());
            throw ex;
        }
    }


    //  Term Aggregation을 이용한 Method
    //  비어있는 값을 0으로 채울 수 있음
    @Override
    public  Map<String, Object> LoadEndUserTimeHisto(XLogInfoSearchArgs args) throws Exception {
        try {
            List<String> timeIndexList = DateUtil.getSearchRangeTime("enduser-info-",args.from,args.to);
            if(timeIndexList == null) return new HashMap<String, Object>();

            String[] indexes = timeIndexList.toArray(new String[timeIndexList.size()]);
            SearchRequest searchRequest = new SearchRequest(indexes);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("endTime").gte(args.from).lte(args.to))
                    .filter(QueryBuilders.termsQuery("objHash", args.objList))
                    .filter(QueryBuilders.termsQuery("type", new ArrayList<Integer>(Arrays.asList(E2ETypeConstants.DOCUMENT, E2ETypeConstants.XHR))));

            if(args.serviceHash != 0L){
                ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.matchQuery("serviceHash",args.serviceHash));
            }
            ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("type", E2ETypeConstants.ERROR));

            Map<String, Object> filters = args.fileterMap;
            Iterator<String> it = filters.keySet().iterator();
            while(it.hasNext()) {
                String key = it.next();
                if(key.equals("elapsed")) {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.rangeQuery("elapsedTime").gte(filters.get(key)));
                } else if (key.equals("error")) {
                    ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("errorHash", 0));
                } else {
                    ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.termsQuery(key,filters.get(key)));
                }
            }

            // 2. set DateHistogram/Terms aggregation.
            DateHistogramAggregationBuilder histogramBuilder =  AggregationBuilders.dateHistogram("time");
            histogramBuilder.dateHistogramInterval(new DateHistogramIntervalManager().getHistogramInterval(args.from, args.to));
            histogramBuilder.field("endTime").extendedBounds(new ExtendedBounds(args.from, args.to));

            TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby").field("type").minDocCount(0);
            if(args.shardSize > 0) {
                termsAggregationBuilder.shardSize(args.shardSize);
            }
            termsAggregationBuilder.subAggregation(AggregationBuilders.avg("avg_elapsed").field("elapsedTime"));
            termsAggregationBuilder.subAggregation(AggregationBuilders.avg("avg_firstnet").field("timeToFirstByteRecv"));
            termsAggregationBuilder.subAggregation(AggregationBuilders.avg("avg_dom").field("timeToDomComplete"));
            histogramBuilder.subAggregation(termsAggregationBuilder);


            histogramBuilder.minDocCount(0);
            // 3. set query
            sourceBuilder.query(queryBuilder)
                    .fetchSource(false) // no source
                    .aggregation(histogramBuilder)
                    .size(0).timeout(new TimeValue(60, TimeUnit.SECONDS));;  // no hits
            if(conf.print_es_query) {
                Logger.println(sourceBuilder.toString());
            }

            searchRequest.source(sourceBuilder)
                         .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            List<Long> pageTimeList = new ArrayList<Long>();
            List<Long> pageElapsedList = new ArrayList<Long>();
            List<Long> pageFirstByteList = new ArrayList<Long>();
            List<Long> pageDomCompleteList = new ArrayList<Long>();
            List<Long> xhrTimeList = new ArrayList<Long>();
            List<Long> xhrElapsedList = new ArrayList<Long>();
            List<Long> xhrFirstByteList = new ArrayList<Long>();

            Map<String, Object> result = new HashMap<String, Object>();

            //  조회결과가 없는 경우 Empty List Return
            if( searchResponse.getAggregations() == null )    return result;
            if( !searchResponse.getAggregations().getAsMap().containsKey("time") )    return result;

            ParsedDateHistogram pdh = (ParsedDateHistogram) searchResponse.getAggregations().getAsMap().get("time");
            List<ParsedDateHistogram.ParsedBucket> bucketList = (List<ParsedDateHistogram.ParsedBucket>) pdh.getBuckets();

            for (ParsedDateHistogram.ParsedBucket bucket : bucketList) {
                Long time = Long.parseLong(bucket.getKeyAsString());

                Terms terms = bucket.getAggregations().get("groupby");
                List<ParsedTerms.ParsedBucket> innerBucketList = (List<ParsedTerms.ParsedBucket>) terms.getBuckets();

                //  Sub Aggregation Data가 없는 경우 추가
                if(innerBucketList.size() == 0){
                    pageTimeList.add(time);
                    pageElapsedList.add(0L);
                    pageFirstByteList.add(0L);
                    pageDomCompleteList.add(0L);
                    xhrTimeList.add(time);
                    xhrElapsedList.add(0L);
                    xhrFirstByteList.add(0L);
                }else if(innerBucketList.size() == 1){
                    if( innerBucketList.get(0).getKeyAsString().equals("11") ){
                        xhrTimeList.add(time);
                        xhrElapsedList.add(0L);
                        xhrFirstByteList.add(0L);
                    }else if( innerBucketList.get(0).getKeyAsString().equals("5") ){
                        pageTimeList.add(time);
                        pageElapsedList.add(0L);
                        pageFirstByteList.add(0L);
                        pageDomCompleteList.add(0L);
                    }
                }

                for (ParsedTerms.ParsedBucket innerBucket : innerBucketList) {
                    int type = Integer.parseInt(innerBucket.getKeyAsString());
                    Map<String, Aggregation> aggMap = innerBucket.getAggregations().getAsMap();
                    for (String key : aggMap.keySet()) {
                        Object obj = aggMap.get(key);
                        Double d = ((Avg) obj).getValue();
                        Long value = Double.isInfinite(d) ? 0L : d.longValue();

                        if (type == 11) {
                            if (key.equals("avg_elapsed")) {
                                pageTimeList.add(time);
                                pageElapsedList.add(value);
                            } else if (key.equals("avg_firstnet")) {
                                pageFirstByteList.add(value);
                            } else if (key.equals("avg_dom")) {
                                pageDomCompleteList.add(value);
                            }
                        } else if (type == 5) {
                            if (key.equals("avg_elapsed")) {
                                xhrTimeList.add(time);
                                xhrElapsedList.add(value);
                            } else if (key.equals("avg_firstnet")) {
                                xhrFirstByteList.add(value);
                            }
                        }
                    }

                }
            }

            Map<String, Object> pageMap = new HashMap<String, Object>();
            pageMap.put("time", pageTimeList);
            pageMap.put("elapsedTimeStat", pageElapsedList);
            pageMap.put("timeToFirstByteRecvStat", pageFirstByteList);
            pageMap.put("timeToDomCompleteStat", pageDomCompleteList);

            Map<String, Object> xhrMap = new HashMap<String, Object>();
            xhrMap.put("time", xhrTimeList);
            xhrMap.put("elapsedTimeStat", xhrElapsedList);
            xhrMap.put("timeToFirstByteRecvStat", xhrFirstByteList);

            result.put("page", pageMap);
            result.put("xhr", xhrMap);

            if(conf.print_es_query_result) {
                QueryUtil.print(result);
            }
            return result;

        }catch (Exception ex) {
            System.out.println(ex.getMessage());
            throw ex;
        }
    }

    //  Statistics - EndUser - Page/XHR Chart의 Popup을 위한 데이터 조회 Method
    //  2019.07.23 기존 Composite query에서 Terms query로 변경
    //      - 변경 사유 : 0 인 값을 채우기 위함
    @Override
    public Map<Long, Map<String, List<Long>>> LoadEndUserTimeHistoByInstance(XLogInfoSearchArgs args) throws Exception {
        try {
            List<String> timeIndexList = DateUtil.getSearchRangeTime("enduser-info-",args.from,args.to);
            if(timeIndexList == null) return new HashMap<Long, Map<String, List<Long>>>();

            String[] indexes = timeIndexList.toArray(new String[timeIndexList.size()]);
            SearchRequest searchRequest = new SearchRequest(indexes);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.rangeQuery("endTime").gte(args.from).lte(args.to))
                    .filter(QueryBuilders.termsQuery("objHash",args.objList))
                    .filter(QueryBuilders.matchQuery("type", args.type));

            if(args.serviceHash != 0L){
                ((BoolQueryBuilder) queryBuilder).filter(QueryBuilders.matchQuery("serviceHash",args.serviceHash));
            }
            ((BoolQueryBuilder) queryBuilder).mustNot(QueryBuilders.matchQuery("type", E2ETypeConstants.ERROR));

            // 2. set DateHistogram/Terms aggregation.
            DateHistogramAggregationBuilder histogramBuilder = new DateHistogramAggregationBuilder("time");
            histogramBuilder.dateHistogramInterval(new DateHistogramIntervalManager().getHistogramInterval(args.from, args.to));
            histogramBuilder.field("endTime").extendedBounds(new ExtendedBounds(args.from, args.to));;
            histogramBuilder.minDocCount(0);

            histogramBuilder.subAggregation( AggregationBuilders.avg("elapsedTimeStat").field("elapsedTime") );
            histogramBuilder.subAggregation( AggregationBuilders.avg("timeToFirstByteRecvStat").field("timeToFirstByteRecv") );
            histogramBuilder.subAggregation( AggregationBuilders.avg("timeToDomCompleteStat").field("timeToDomComplete") );

            // 3. set query
            sourceBuilder.query(queryBuilder)
                    .fetchSource(false) // no source
                    .aggregation(histogramBuilder)
                    .size(0)    // no hits
                    .timeout(new TimeValue(60, TimeUnit.SECONDS));

            if(conf.print_es_query) {
                Logger.println(sourceBuilder.toString());
            }

            searchRequest.source(sourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
            RestHighLevelClient client = ConnectionManager.getInstance().getReadClient();
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Map<Long, Map<String, List<Long>>> result = new HashMap<Long, Map<String, List<Long>>>();

            //  조회결과가 없는 경우 Empty List Return
            if( searchResponse.getAggregations() == null )    return result;
            if( !searchResponse.getAggregations().getAsMap().containsKey("time") )    return result;

            ParsedDateHistogram pdh = (ParsedDateHistogram) searchResponse.getAggregations().getAsMap().get("time");
            List<ParsedDateHistogram.ParsedBucket> bucketList = (List<ParsedDateHistogram.ParsedBucket>) pdh.getBuckets();

            Map<String, List<Long>> innerMap = new HashMap<String, List<Long>>();
            innerMap.put("time", new ArrayList<Long>());
            innerMap.put("elapsedTime", new ArrayList<Long>());
            innerMap.put("timeToFirstByteRecv", new ArrayList<Long>());
            if(args.type == Byte.valueOf("11")) innerMap.put("timeToDomComplete", new ArrayList<Long>());

            for (ParsedDateHistogram.ParsedBucket bucket : bucketList) {
                Long time = Long.parseLong(bucket.getKeyAsString());
                innerMap.get("time").add(time);
                if( bucket.getAggregations().getAsMap().size() == 0 ) System.out.println("Empty Map");
                Map<String, Aggregation> aggMap = bucket.getAggregations().getAsMap();
                for (String key : aggMap.keySet()) {
                    Object obj = aggMap.get(key);
                    Double d = ((Avg) obj).getValue();
                    Long value = Double.isInfinite(d) ? 0L : d.longValue();

                    if (key.equals("elapsedTimeStat")) {
                        innerMap.get("elapsedTime").add(value);
                    } else if (key.equals("timeToFirstByteRecvStat")) {
                        innerMap.get("timeToFirstByteRecv").add(value);
                    } else if (key.equals("timeToDomCompleteStat") && args.type == Byte.valueOf("11")) {
                        innerMap.get("timeToDomComplete").add(value);
                    }
                }
            }

            result.put(Long.parseLong(args.objList.get(0).toString()), innerMap);

/*
            do{
                if(!initialSearch){
                    Map<String,Object> afterKey = parsedComposite.afterKey();
                    aggBuilder.aggregateAfter(afterKey);
                } else {
                    initialSearch = false;
                }

                Aggregations aggs = searchResponse.getAggregations();
                if(aggs == null)    break;

                if(aggs.get("groupby") == null) return result;
                parsedComposite = aggs.get("groupby");

                List<ParsedComposite.ParsedBucket> bucketList = parsedComposite.getBuckets();
                bucketSize = bucketList.size();

                for(ParsedComposite.ParsedBucket bucket : bucketList) {
                    Long time = (Long)bucket.getKey().get("time");
                    long objHash = Long.parseLong(bucket.getKey().get("objHash").toString());

                    if(!result.containsKey(objHash)){
                        Map<String, List<Long>> innerMap = new HashMap<String, List<Long>>();
                        innerMap.put("time", new ArrayList<Long>());
                        innerMap.put("elapsedTime", new ArrayList<Long>());
                        innerMap.put("timeToFirstByteRecv", new ArrayList<Long>());
                        if(args.type == Byte.valueOf("11")) innerMap.put("timeToDomComplete", new ArrayList<Long>());

                        result.put( objHash, innerMap);
                    }

                    result.get(objHash).get("time").add(time);

                    Map<String , Aggregation > aggMap  = bucket.getAggregations().getAsMap();
                    for(String key  : aggMap.keySet()) {
                        Object obj = aggMap.get(key);
                        Long value = 0L;

                        if(key.equals("elapsedTimeStat")){
                            result.get(objHash).get("elapsedTime").add( (long)((Avg)obj).getValue() );
                        }else if(key.equals("timeToFirstByteRecvStat")){
                            result.get(objHash).get("timeToFirstByteRecv").add( (long)((Avg)obj).getValue() );
                        }else if(key.equals("timeToDomCompleteStat") && args.type == Byte.valueOf("11")){
                            result.get(objHash).get("timeToDomComplete").add( (long)((Avg)obj).getValue() );
                        }
                    }
                }

            }while(bucketSize > 0);
*/
            if(conf.print_es_query_result) {
                QueryUtil.print(result);
            }
            return result;


        }catch (Exception ex) {
            System.out.println(ex.getMessage());
            throw ex;
        }
    }

    private NumberFormat getNumberFormat(){
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumFractionDigits(3);
        return nf;
    }

}
