package com.pardot.rhombus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pardot.rhombus.cobject.CObjectTokenVisitor;
import com.pardot.rhombus.cobject.CObjectTokenVisitorFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Michael Frank
 * Date: 6/30/14
 */
public class TableScanner {

	private static Logger logger = LoggerFactory.getLogger(TableScanner.class);

	public static final long minToken = Long.MIN_VALUE;
	public static final long maxToken = Long.MAX_VALUE;
	public final int maxSavepointLinesToCheck = 10;
	private long batchSize = 200;
	private int statementRetries = 5;

	private ObjectMapper objectMapper;
	private String objectType;
	private final int numPartitions;
	CObjectTokenVisitorFactory visitorFactory;
	private ExecutorService executorService;
	private File savepointDirectory;
	private PrintWriter[] savepointWriters = null;
	private final CountDownLatch shutdownLatch;

	public TableScanner(ObjectMapper objectMapper, String objectType, int numPartitions, CObjectTokenVisitorFactory visitorFactory, String savepointDirectoryPath) {
		Preconditions.checkNotNull(objectMapper);
		Preconditions.checkNotNull(objectType);
		Preconditions.checkNotNull(numPartitions);
		Preconditions.checkNotNull(visitorFactory);
		this.objectMapper = objectMapper;
		this.objectType = objectType;
		this.numPartitions = numPartitions;
		this.visitorFactory = visitorFactory;
		executorService = Executors.newFixedThreadPool(numPartitions);
		shutdownLatch = new CountDownLatch(numPartitions);
		if (savepointDirectoryPath != null) {
			this.savepointDirectory = new File(savepointDirectoryPath);
			this.savepointDirectory.mkdir();
		}
	}

	public void scan() {
		List<Map.Entry<Long, Long>> ranges = makeRanges();
		if (this.savepointDirectory != null) {
			this.savepointWriters = this.makeSavepointWriters(this.numPartitions);
		}

		int rangeNum = 0;
		for(final Map.Entry<Long, Long> range : ranges) {
			final Integer partitionId = rangeNum;
			rangeNum++;
			final CObjectTokenVisitor visitor = visitorFactory.getInstance("" + rangeNum);
			Runnable r = new Runnable() {
				@Override
				public void run() {
					handle(range.getKey(), range.getValue(), visitor, partitionId);
				}
			};
			executorService.execute(r);
		}
		try {
			shutdownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			if (this.savepointWriters != null) {
				for(int i = 0; i < numPartitions; i++) {
					this.savepointWriters[i].close();
				}
			}
		}
	}

	public List<Map.Entry<Long, Long>> makeRanges() {
		Long[] savedStartTokens = this.readSavepoints();
		List<Map.Entry<Long, Long>> ranges = Lists.newArrayList();
		BigInteger fullRange = BigInteger.valueOf(maxToken).subtract(BigInteger.valueOf(minToken)).add(BigInteger.ONE);
		BigInteger rangeLength = fullRange.divide(BigInteger.valueOf(numPartitions));
		BigInteger rangeStart = BigInteger.valueOf(minToken);
		for(int i = 0 ; i < numPartitions - 1 ; i++) {
			BigInteger rangeEnd = rangeStart.add(rangeLength).subtract(BigInteger.ONE);
			// If we have a savepoint for this partition, use that start token instead of the default
			if (savedStartTokens != null) {
				rangeStart = BigInteger.valueOf(savedStartTokens[i]);
			}
			AbstractMap.SimpleEntry<Long, Long> range = new AbstractMap.SimpleEntry<Long, Long>(rangeStart.longValue(), rangeEnd.longValue());
			ranges.add(range);
			rangeStart = rangeEnd.add(BigInteger.ONE);
		}
		BigInteger rangeEnd = BigInteger.valueOf(maxToken);
		if (savedStartTokens != null) {
			rangeStart = BigInteger.valueOf(savedStartTokens[numPartitions - 1]);
		}
		AbstractMap.SimpleEntry<Long, Long> range = new AbstractMap.SimpleEntry<Long, Long>(rangeStart.longValue(), rangeEnd.longValue());
		ranges.add(range);
		return ranges;
	}

	private Long[] readSavepoints() {
		Long[] out = null;
		if (this.savepointDirectory != null) {
			File[] existingSavepoints = this.savepointDirectory.listFiles();
			// Ensure we have the right number of existing savepoint files
			if (existingSavepoints == null) {
				logger.info("No savepoint files found in savepoint directory.");
			} else if (existingSavepoints.length != numPartitions) {
				logger.info("Found " + existingSavepoints.length + " files in savepoint directory, expected " + numPartitions);
			} else {
				// We have the right number of files, let's parse them
				Map<String, File> files = Maps.newHashMap();
				for (File savepoint : existingSavepoints) {
					files.put(savepoint.getName(), savepoint);
				}
				out = new Long[numPartitions];
				for(int partitionId = 0; partitionId < numPartitions; partitionId++) {
					// If any of the savepoints are missing for our partitions, fail out from restoring from savepoints
					if (!files.keySet().contains(TableScanner.getSavepointFilename(partitionId))) {
						return null;
					}
					File savepointFile = files.get(TableScanner.getSavepointFilename(partitionId));
					UUID lastUuid = null;
					try {
						ReversedLinesFileReader reader = new ReversedLinesFileReader(savepointFile);
						int lineCounter = 0;
						while (lastUuid == null && lineCounter < this.maxSavepointLinesToCheck) {
							try {
								lastUuid = UUID.fromString(reader.readLine());
							} catch (IllegalArgumentException e) {
								// Do nothing here, just let the line counter get incremented
							}
							lineCounter++;
						}
						if (lastUuid == null) {
							logger.warn("Unable to parse UUID from the last " + this.maxSavepointLinesToCheck + " lines of savepoint file " + TableScanner.getSavepointFilename(partitionId));
							return null;
						}
						reader.close();
					} catch (IOException e) {
						logger.error("Error reading savepoint file", e);
						return null;
					}
					out[partitionId] = this.objectMapper.getTokenForId(this.objectType, lastUuid);
				}
			}
		}
		logger.info("Read savepoints successfully: " + Arrays.toString(out));
		return out;
	}

	/**
	 * @param partitionId 0-based partition id
	 * @return Savepoint filename
	 */
	public static String getSavepointFilename(Integer partitionId) {
		return "savepoint-" + partitionId.toString() + ".txt";
	}

	private PrintWriter[] makeSavepointWriters(Integer partitionCount) {
		PrintWriter[] out = new PrintWriter[partitionCount];
		try {
			this.readySafepointDirectory(partitionCount);
			for(int i = 0; i < partitionCount; i++) {
				// Build a writer for each partition thread
				out[i] = new PrintWriter(new FileOutputStream(this.savepointDirectory.getName() + "/" + TableScanner.getSavepointFilename(i), true));
			}
		} catch (IOException e) {
			return null;
		}
		return out;
	}

	private void readySafepointDirectory(Integer partitionCount) throws IOException {
		File[] fileArray = this.savepointDirectory.listFiles();
		// If there are already files in the savepoint directory...
		if (fileArray != null) {
			// Let's find out if they match our config or not (i.e. probably from the same command run previously)
			Map<String, File> files = Maps.newHashMap();
			for (File savepoint : fileArray) {
				files.put(savepoint.getName(), savepoint);
			}
			Set<String> requiredFilenames = Sets.newHashSet();
			for(int i = 0; i < partitionCount; i++) {
				requiredFilenames.add(TableScanner.getSavepointFilename(i));
			}

			// If they don't have the right filenames, clean the directory out
			if (!files.keySet().equals(requiredFilenames)) {
				logger.info("Existing savepoint files don't match what we're looking for, clearing them out and starting over.");
				FileUtils.cleanDirectory(this.savepointDirectory);
			}
			logger.info("Appending savepoints to existing files.");
			// Otherwise leave the files there, we can append to them
		}
	}

	public void handle(Long minToken, Long maxToken, CObjectTokenVisitor visitor, Integer partitionId) {
		visitor.setUp();
		List<Map<String, Object>> results = executeStatement(objectType, minToken, maxToken, batchSize);
		while(results != null && results.size() > 0) {
			visitResults(results, visitor);
			String minUuid = String.valueOf(results.get(results.size() - 1).get("id"));
			results = executeStatement(objectType, minUuid, maxToken, batchSize);
			if (this.savepointWriters != null) {
				// We've processed this range, so save our progress for later
				this.savepointWriters[partitionId].append(minUuid);
				this.savepointWriters[partitionId].append("\n");
				this.savepointWriters[partitionId].flush();
			}
		}
		visitor.cleanUp();
		shutdownLatch.countDown();
	}

	private void visitResults(List<Map<String, Object>> results, CObjectTokenVisitor visitor) {
		for(Map<String, Object> result : results) {
			visitor.visit(result);
		}
	}

	private List<Map<String, Object>> executeStatement(String objectType, Object minToken, Long maxToken, long batchSize) {
		int retries = 0;
		while(retries < statementRetries) {
			try {
				List<Map<String, Object>> results;
				if(Long.class.isAssignableFrom(minToken.getClass())) {
					results = objectMapper.scanTableWithStartToken(objectType, (Long)minToken, maxToken, batchSize);
				} else {
					results = objectMapper.scanTableWithStartId(objectType, (String)minToken, maxToken, batchSize);
				}
				return results;
			} catch(Exception e) {
				logger.error("Exception number " + retries + " executing statement", e);
				retries++;
				if(retries >= statementRetries) {
					throw new RuntimeException(e);
				}
			}
		}
		throw new RuntimeException("Bad logic in statement retry");
	}

	public long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getStatementRetries() {
		return statementRetries;
	}

	public void setStatementRetries(int statementRetries) {
		this.statementRetries = statementRetries;
	}
}
