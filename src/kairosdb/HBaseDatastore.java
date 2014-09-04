package net.opentsdb.kairosdb;


import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.stumbleupon.async.Callback;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.exception.DatastoreException;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.DataPointRow;
import org.kairosdb.core.datastore.TagSet;

import net.opentsdb.core.TsdbQuery;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import java.io.IOException;
import java.util.List;

public class HBaseDatastore implements Datastore
{
	public static final String TIMESERIES_TABLE_PROPERTY = "kairosdb.datastore.hbase.timeseries_table";
	public static final String UNIQUEIDS_TABLE_PROPERTY = "kairosdb.datastore.hbase.uinqueids_table";
	public static final String ZOO_KEEPER_QUORUM_PROPERTY = "kairosdb.datastore.hbase.zoo_keeper_quorum";
	public static final String ZOO_KEEPER_BASE_PROPERTY = "kairosdb.datastore.hbase.zoo_keeper_base_dir";
	public static final String AUTO_CREATE_METRIC_PROPERTY = "kairosdb.datastore.hbase.auto_create_metrics";

	private TSDB m_tsdb;

	@Inject
	public HBaseDatastore(@Named(TIMESERIES_TABLE_PROPERTY) String timeSeriesTable,
	                      @Named(UNIQUEIDS_TABLE_PROPERTY) String uidTable,
	                      @Named(ZOO_KEEPER_QUORUM_PROPERTY) String zkQuorum,
	                      @Named(ZOO_KEEPER_BASE_PROPERTY) String zkBase,
	                      @Named(AUTO_CREATE_METRIC_PROPERTY) boolean autoCreateMetrics) throws DatastoreException
		{
		HBaseClient hbaseClient;

		if ((zkBase != null) && (!zkBase.equals("")))
			hbaseClient = new HBaseClient(zkQuorum, zkBase);
		else
			hbaseClient = new HBaseClient(zkQuorum);

		Config config;
		try {
			config = new Config(false);
			config.setAutoMetric(autoCreateMetrics);
			config.overrideConfig("tsd.storage.hbase.zk_quorum", zkQuorum);
			config.overrideConfig("tsd.storage.hbase.zk_basedir", zkBase);
			config.overrideConfig("tsd.storage.hbase.data_table", timeSeriesTable);
			config.overrideConfig("tsd.storage.hbase.uid_table", uidTable);
			config.overrideConfig("tsd.storage.enable_compaction", "false");

			m_tsdb = new TSDB(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new DatastoreException(e.getMessage());
		}
		}


	@Override
	public void putDataPoints(DataPointSet dps) throws DatastoreException
		{
		final class PutErrback implements Callback<Exception, Exception>
			{
			public DatastoreException call(final Exception arg)
				{
				return new DatastoreException(arg);
				}

			public String toString()
				{
				return "report error";
				}
			}

		for (DataPoint dp : dps.getDataPoints())
			{
			//Need to convert the timestamp to seconds
			long timestamp = dp.getTimestamp();

			if (dp.isLong())
				{
				m_tsdb.addPoint(dps.getName(), timestamp, dp.getLongValue(),
						dps.getTags()).addErrback(new PutErrback());
				}
			else
				{
				m_tsdb.addPoint(dps.getName(), timestamp, (float) dp.getDoubleValue(),
						dps.getTags()).addErrback(new PutErrback());
				}
			}
		}

	@Override
	public Iterable<String> getMetricNames() throws DatastoreException
		{
		try
			{
			// todo pass in search parameter
			return m_tsdb.suggestMetrics("");
			}
		catch (HBaseException e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public Iterable<String> getTagNames() throws DatastoreException
		{
		try
			{
			// todo pass in search parameter
			return m_tsdb.suggestTagNames("");
			}
		catch (HBaseException e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public Iterable<String> getTagValues() throws DatastoreException
		{
		try
			{
			// todo pass in search parameter
			return m_tsdb.suggestTagValues("");
			}
		catch (HBaseException e)
			{
			throw new DatastoreException(e);
			}
		}

	@Override
	public void queryDatabase(DatastoreMetricQuery query, QueryCallback cachedSearchResult) throws DatastoreException
		{
		try
			{
			KTsdbQuery tsdbquery = new KTsdbQuery(m_tsdb, query.getName(), query.getStartTime(), query.getEndTime(), query.getTags());

			tsdbquery.run(cachedSearchResult);
			}
		catch (Exception e)
			{
			throw new DatastoreException(e);
			}
		}
		
	@Override
	public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
		{
		throw new DatastoreException("Query metric tags is not implemented for HBase");
		}

	@Override
	public void close() throws DatastoreException
		{
		try
			{
			m_tsdb.shutdown().join();
			}
		catch (Exception e)
			{
			throw new DatastoreException(e);
			}
		}
		
	public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
		{
		throw new DatastoreException("Delete not implemented");
		}


	@Override
	public void putDataPoint(String metricName,
			ImmutableSortedMap<String, String> tags, DataPoint dataPoint)
			throws DatastoreException {
		
		final class PutErrback implements Callback<Exception, Exception>
		{
		public DatastoreException call(final Exception arg)
			{
			return new DatastoreException(arg);
			}

		public String toString()
			{
			return "report error";
			}
		}

		
		m_tsdb.addPoint(metricName, dataPoint.getTimestamp(), dataPoint.getLongValue(),
				tags).addErrback(new PutErrback());
	}
	}
