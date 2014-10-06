package net.opentsdb.kairosdb;


import net.opentsdb.core.DataPoint;
import net.opentsdb.core.Span;
import net.opentsdb.core.TsdbQuery;
import net.opentsdb.core.TSDB;

import org.hbase.async.HBaseException;
import org.kairosdb.core.datastore.QueryCallback;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;

import com.google.common.collect.SetMultimap;

public class KTsdbQuery
	{
	private static TsdbQuery tsdbQuery;
	private long m_startTime;
	private long m_endTime;

	public KTsdbQuery(final TSDB tsdb, String metric, long startTime, long endTime, SetMultimap<String, String> tags)
		{
		tsdbQuery = new TsdbQuery(tsdb);

		Map<String, String> mapTags = createMapOfTags(tags);
		tsdbQuery.setStartTime(startTime);
		tsdbQuery.setEndTime(endTime);
		tsdbQuery.setTimeSeries(metric, mapTags, null, false);

		m_startTime = startTime;
		m_endTime = endTime;
		}

	public void run(QueryCallback cachedSearchResult) throws IOException
		{

		TreeMap<byte[], Span> spanMap;
		try {
			spanMap = tsdbQuery.findSpans().join();
		} catch (HBaseException e) {
			throw new IOException ("HBaseException when fetching the data: " + e.getMessage(), e);
		} catch (InterruptedException e) {
			throw new IOException ("InterruptedException when fetching the data: " + e.getMessage(), e);
		} catch (Exception e) {
			throw new IOException ("Exception occurred when fetching the data: " + e.getMessage(), e);
		}

		if (spanMap != null)
		{
			String type = "long";
			for(Span span : spanMap.values()) {

				cachedSearchResult.startDataPointSet(type, span.getInclusiveTags());
				for (DataPoint dataPoint : span) {
					if (dataPoint.timestamp() < m_startTime || dataPoint.timestamp() > m_endTime) {
						// Remove data points not in the time range
						continue;
					}

					//Convert timestamps back to milliseconds
					if (dataPoint.isInteger() && type.equals("long")) {
						cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.longValue());
					} else if (dataPoint.isInteger() && type.equals("double")) {
						type = "long";
						cachedSearchResult.startDataPointSet(type, span.getInclusiveTags());
						cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.longValue());
					} else if (type.equals("double")) {
						cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.doubleValue());
					} else {
						type = "double";
						cachedSearchResult.startDataPointSet(type, span.getInclusiveTags());
						cachedSearchResult.addDataPoint(dataPoint.timestamp(), dataPoint.doubleValue());
					}
				}
				cachedSearchResult.endDataPoints();

			}
			}
		}

	private Map<String, String> createMapOfTags(SetMultimap<String, String> tags)
		{
		HashMap<String, String> ret = new HashMap<String, String>();
		for (String key : tags.keySet())
			{
			boolean first = true;
			StringBuilder values = new StringBuilder();
			for (String value : tags.get(key))
				{
				if (!first)
					values.append("|");
				first = false;
				values.append(value);
				}

			ret.put(key, values.toString());
			}

		return (ret);
		}
	}
