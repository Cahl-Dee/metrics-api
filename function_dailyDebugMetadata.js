// TODO: getting latest processed block is borked

const METHODS = {
  GET_STREAM_STATUS: "getStreamStatus",
  GET_CURRENT_METRICS: "getCurrentMetrics",
  GET_HISTORICAL_METRICS: "getHistoricalMetrics",
  GET_SEQUENCE_ANALYSIS: "getSequenceAnalysis",
};

async function main(params) {
  if (!params?.user_data) {
    params.user_data = {};
  }

  const {
    chain = "BASE",
    days = 7,
    method = METHODS.GET_CURRENT_METRICS,
    offset = 0,
    limit = 30,
  } = params.user_data;

  if (![7, 30, 90].includes(days)) {
    throw new Error("Days must be 7, 30, or 90");
  }

  const prefix = `MA_${chain.toUpperCase()}_`;
  const keys = {
    blockMetrics: (blockNum) => `${prefix}block-metrics_${blockNum}`,
    dailyMetrics: (date) => `${prefix}daily-metrics_${date}`,
    dailyBlocks: (date) => `${prefix}daily-blocks_${date}`,
  };

  try {
    let response;
    switch (method) {
      case METHODS.GET_STREAM_STATUS:
        response = await getProcessingStatus(keys);
        break;

      case METHODS.GET_CURRENT_METRICS:
        const status = await getProcessingStatus(keys);
        if (status.error) {
          throw new Error(status.error);
        }
        if (!status.currentDate) {
          throw new Error("No current date found in processing status");
        }
        response = await getCurrentProcessingMetrics(keys, status.currentDate);
        break;

      case METHODS.GET_HISTORICAL_METRICS:
        const lastCompleteDay = await findMostRecentCompleteDay(keys);
        if (!lastCompleteDay) {
          throw new Error("No complete daily metrics found in last 90 days");
        }

        const end = new Date(lastCompleteDay);
        const start = new Date(end);
        start.setDate(start.getDate() - days);

        response = await getPaginatedHistoricalData(
          keys,
          start,
          end,
          offset,
          limit
        );
        break;

      case METHODS.GET_SEQUENCE_ANALYSIS:
        response = await getSequenceAnalysis(keys, days);
        break;

      default:
        throw new Error(
          `Method ${method} not supported. Available methods: ${Object.values(
            METHODS
          ).join(", ")}`
        );
    }

    return {
      chain: chain.toLowerCase(),
      ...response,
    };
  } catch (e) {
    return {
      chain: chain.toLowerCase(),
      error: e.message,
    };
  }
}

// Add version check helper
async function isLegacyStorage(keys) {
  const legacyList = await qnLib.qnGetList(keys.processedBlocks);
  return legacyList && legacyList.length > 0;
}

async function getProcessingStatus(keys) {
  try {
    // Check yesterday first
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split("T")[0];
    let dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(yesterdayStr));

    // If no blocks yesterday, check in 10 day chunks up to 90 days
    if (!dailyBlocks?.length) {
      const date = new Date();
      for (let chunk = 0; chunk < 9; chunk++) {
        // 9 chunks of 10 days = 90 days
        for (let i = 0; i < 10; i++) {
          const dateStr = date.toISOString().split("T")[0];
          dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(dateStr));
          if (dailyBlocks?.length) {
            break;
          }
          date.setDate(date.getDate() - 1);
        }
        if (dailyBlocks?.length) break;
      }
    }

    if (!dailyBlocks?.length) {
      return { error: "No processed blocks found in last 90 days" };
    }

    const latestBlock = Math.max(...dailyBlocks.map(Number));

    // Get block metrics for latest block
    const blockMetricsStr = await qnLib.qnGetSet(
      keys.blockMetrics(latestBlock)
    );
    if (!blockMetricsStr) {
      return {
        error: "Latest block metrics not found",
        debug: {
          latestBlock,
          key: keys.blockMetrics(latestBlock),
        },
      };
    }

    const blockMetrics = JSON.parse(blockMetricsStr);
    return {
      latestBlock,
      latestBlockTimestamp: blockMetrics.timestamp,
      currentDate: blockMetrics.date,
      lastUpdated: blockMetrics.lastUpdated,
    };
  } catch (error) {
    return {
      error: error.message,
      stack: error.stack,
      debug: { error },
    };
  }
}

async function getCurrentProcessingMetrics(keys, currentDate) {
  if (!currentDate) return { error: "No current date provided" };

  try {
    const dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(currentDate));

    if (!dailyBlocks.length) {
      return { error: "No blocks found for current date" };
    }

    const metrics = await Promise.all(
      dailyBlocks.map((blockNum) => qnLib.qnGetSet(keys.blockMetrics(blockNum)))
    );

    const validMetrics = metrics
      .filter(Boolean)
      .map((m) => JSON.parse(m))
      .filter((m) => m.lastUpdated)
      .sort((a, b) => new Date(a.lastUpdated) - new Date(b.lastUpdated));

    let medianSecBetweenBlocks = 0;
    if (validMetrics.length > 1) {
      const timestamps = validMetrics.map((m) =>
        new Date(m.lastUpdated).getTime()
      );
      const timeDiffs = timestamps
        .slice(1)
        .map((time, i) => time - timestamps[i]);
      timeDiffs.sort((a, b) => a - b);

      const mid = Math.floor(timeDiffs.length / 2);
      medianSecBetweenBlocks =
        timeDiffs.length % 2 === 0
          ? (timeDiffs[mid - 1] + timeDiffs[mid]) / 2 / 1000
          : timeDiffs[mid] / 1000;
    }

    const lastBlockNum = Math.max(...dailyBlocks.map(Number));

    return {
      date: currentDate,
      blocksProcessed: validMetrics.length,
      lastProcessedBlock: {
        number: lastBlockNum,
        timestamp: validMetrics[validMetrics.length - 1]?.timestamp,
      },
      medianSecBetweenBlocks,
    };
  } catch (error) {
    return { error: error.message };
  }
}

async function getHistoricalProcessingData(keys, days, currentDate) {
  const end = new Date(currentDate);
  const start = new Date(end);
  start.setDate(start.getDate() - days);

  const timeseriesData = [];
  const processedDates = new Set();
  let currentDay = new Date(start);

  while (currentDay <= end) {
    const dateStr = currentDay.toISOString().split("T")[0];
    const dailyMetricsStr = await qnLib.qnGetSet(keys.dailyMetrics(dateStr));

    if (dailyMetricsStr) {
      const metrics = JSON.parse(dailyMetricsStr);
      processedDates.add(dateStr);

      const nextDayMetricsStr = await qnLib.qnGetSet(
        keys.dailyMetrics(
          new Date(currentDay.getTime() + 86400000).toISOString().split("T")[0]
        )
      );
      const nextDayMetrics = nextDayMetricsStr
        ? JSON.parse(nextDayMetricsStr)
        : null;

      timeseriesData.push({
        date: dateStr,
        lastUpdated: metrics.metadata.lastUpdated,
        numBlocks: metrics.metadata.numBlocks,
        numFailedBlocks: metrics.metadata.numFailedBlocks,
        numProcessedBlocks: metrics.metadata.numProcessedBlocks,
        isSequentialWithNextDay: nextDayMetrics
          ? metrics.metadata.lastBlock ===
            nextDayMetrics.metadata.firstBlock - 1
          : null,
      });
    }

    currentDay.setDate(currentDay.getDate() + 1);
  }

  const sequenceAnalysis = analyzeSequence(timeseriesData, start, end);

  return { timeseriesData, sequenceAnalysis };
}

async function getPaginatedHistoricalData(keys, start, end, offset, limit) {
  const timeseriesData = [];
  let currentDay = new Date(start);
  currentDay.setDate(currentDay.getDate() + offset);
  let count = 0;

  while (currentDay <= end && count < limit) {
    const dateStr = currentDay.toISOString().split("T")[0];
    const dailyMetricsStr = await qnLib.qnGetSet(keys.dailyMetrics(dateStr));

    if (dailyMetricsStr) {
      const metrics = JSON.parse(dailyMetricsStr);
      const nextDayStr = new Date(currentDay.getTime() + 86400000)
        .toISOString()
        .split("T")[0];
      const nextDayMetrics = await qnLib.qnGetSet(
        keys.dailyMetrics(nextDayStr)
      );

      timeseriesData.push({
        date: dateStr,
        lastUpdated: metrics.metadata.lastUpdated,
        numBlocks: metrics.metadata.numBlocks,
        numFailedBlocks: metrics.metadata.numFailedBlocks,
        numProcessedBlocks: metrics.metadata.numProcessedBlocks,
        isSequentialWithNextDay: nextDayMetrics
          ? metrics.metadata.lastBlock ===
            JSON.parse(nextDayMetrics).metadata.firstBlock - 1
          : null,
      });
      count++;
    }
    currentDay.setDate(currentDay.getDate() + 1);
  }

  return {
    data: timeseriesData,
    pagination: {
      offset,
      limit,
      hasMore: currentDay <= end,
      nextOffset: offset + count,
    },
  };
}

async function getSequenceAnalysis(keys, days) {
  try {
    const end = new Date();
    const start = new Date(end);
    start.setDate(end.getDate() - Math.min(days, 7)); // Limit to max 7 days

    const dailyResults = [];
    let currentDay = new Date(start);

    while (currentDay <= end) {
      const dateStr = currentDay.toISOString().split("T")[0];
      const metricsStr = await qnLib.qnGetSet(keys.dailyMetrics(dateStr));

      if (metricsStr) {
        dailyResults.push({
          date: dateStr,
          hasMetrics: true,
        });
      }

      currentDay.setDate(currentDay.getDate() + 1);
    }

    return {
      daysAnalyzed: dailyResults.length,
      completeDays: dailyResults.filter((d) => d.hasMetrics).length,
      startDate: start.toISOString().split("T")[0],
      endDate: end.toISOString().split("T")[0],
    };
  } catch (error) {
    return { error: error.message };
  }
}

async function findMostRecentCompleteDay(keys) {
  // Start from current UTC date
  const currentDate = new Date();
  currentDate.setUTCHours(0, 0, 0, 0);
  let checkDate = new Date(currentDate);

  // Process 9 batches of 10 days each
  for (let batch = 0; batch < 9; batch++) {
    const batchDates = [];
    for (let i = 0; i < 10; i++) {
      const dateStr = checkDate.toISOString().split("T")[0];
      batchDates.push(dateStr);
      checkDate.setDate(checkDate.getDate() - 1);
    }

    // Check all dates in batch concurrently
    const batchResults = await Promise.all(
      batchDates.map(async (dateStr) => ({
        date: dateStr,
        hasMetrics: !!(await qnLib.qnGetSet(keys.dailyMetrics(dateStr))),
      }))
    );

    // Find first complete day in batch
    const completeDay = batchResults.find((result) => result.hasMetrics);
    if (completeDay) {
      return completeDay.date;
    }
  }

  return null;
}

function analyzeSequence(timeseriesData, start, end) {
  if (!timeseriesData.length) return null;

  const latestDay = timeseriesData.sort((a, b) =>
    b.date.localeCompare(a.date)
  )[0].date;

  const missingDays = [];
  let currentDay = new Date(start);
  const endDate = new Date(end);

  while (currentDay <= endDate) {
    const dateStr = currentDay.toISOString().split("T")[0];
    if (!timeseriesData.find((d) => d.date === dateStr)) {
      missingDays.push(dateStr);
    }
    currentDay.setDate(currentDay.getDate() + 1);
  }

  const avgProcessingTime =
    timeseriesData.reduce((acc, day) => {
      const processTime = new Date(day.lastUpdated) - new Date(day.date);
      return acc + processTime;
    }, 0) /
    timeseriesData.length /
    (1000 * 3600); // Convert to hours

  return {
    latestProcessedDay: latestDay,
    missingDays,
    avgDayProcessingTime: avgProcessingTime,
  };
}

function formatResponse(
  chain,
  days,
  processingStatus,
  currentProcessingMetrics,
  timeseriesData,
  sequenceAnalysis
) {
  return {
    chain: chain.toLowerCase(),
    period: `${days}d`,
    processingStatus: {
      latestBlock: processingStatus.latestBlock,
      latestBlockTimestamp: processingStatus.latestBlockTimestamp,
      lastUpdated: processingStatus.lastUpdated,
    },
    currentProcessing: currentProcessingMetrics,
    sequenceAnalysis,
    timeseriesData,
  };
}
