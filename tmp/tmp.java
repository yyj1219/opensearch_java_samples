import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import scouter.server.Logger;
import scouter.util.DateUtil;
import tuna.server.db.common.elastic.ConnectionManager;
import tuna.server.db.rd.IEndUserAggRD;

import java.io.IOException;
import java.util.List;

public class EndUserAggESRD implements IEndUserAggRD {
    @Override
    public long getSessionCount(long startTime, long endTime) {
        List<String> timeList = DateUtil.getSearchRangeMMTime("enduser-aggregation-", startTime, endTime);
        if (timeList == null) {
            return 0;
        }

        String[] indexes = timeList.toArray(new String[timeList.size()]);
        CountRequest countRequest = new CountRequest(indexes);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.rangeQuery("startTime").gte(startTime).lte(endTime));

        searchSourceBuilder.query(queryBuilder);
        countRequest.source(searchSourceBuilder).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        try {
            CountResponse response = ConnectionManager.getInstance().getReadClient().count(countRequest, RequestOptions.DEFAULT);
            return response.getCount();
        } catch (IOException e) {
            Logger.println(e);
            return 0;
        }
    }
}
