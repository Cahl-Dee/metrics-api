const METHODS = {
  GET_CURRENTLY_PROCESSING_STATS: "getCurrentlyProcessingStats",
  GET_PROCESSED_DAYS_STATS: "getProcessedDaysStats",
};

async function main(params) {
  if (!params?.user_data) {
    params.user_data = {};
  }

  const {
    chain = "BASE",
    days = 7,
    method = METHODS.GET_CURRENTLY_PROCESSING_STATS,
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
      case METHODS.GET_CURRENTLY_PROCESSING_STATS:
        const processingDay = await findCurrentProcessingDay(keys);
        if (!processingDay) throw new Error("No current processing data found");
        response = await getCurrentlyProcessingStats(keys, processingDay);
        break;

      case METHODS.GET_PROCESSED_DAYS_STATS:
        const lastProcessedDay = await findMostRecentProcessedDay(keys);
        if (!lastProcessedDay) throw new Error("No processed data found");
        const end = new Date(lastProcessedDay.date);
        const start = new Date(end);
        start.setDate(start.getDate() - days);
        response = await getProcessedDaysStats(keys, start, end);
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

async function findMostRecentDay(keys) {
  const date = new Date();
  for (let i = 0; i < 90; i++) {
    const dateStr = date.toISOString().split("T")[0];
    const metricsStr = await qnLib.qnGetSet(keys.dailyMetrics(dateStr));
    if (metricsStr) {
      return { date: dateStr, metrics: JSON.parse(metricsStr) };
    }
    date.setDate(date.getDate() - 1);
  }
  return null;
}

async function getCurrentlyProcessingStats(keys, currentDay) {
  const dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(currentDay.date));
  if (!dailyBlocks?.length) return { error: "No blocks found" };

  const blockMetrics = await Promise.all(
    dailyBlocks.map((num) => qnLib.qnGetSet(keys.blockMetrics(num)))
  );

  const validMetrics = blockMetrics
    .filter(Boolean)
    .map((m) => JSON.parse(m))
    .sort((a, b) => new Date(a.lastUpdated) - new Date(b.lastUpdated));

  // Calculate processing times between consecutive blocks
  const processingTimes = [];
  for (let i = 1; i < validMetrics.length; i++) {
    const currentBlockTime = new Date(validMetrics[i].lastUpdated).getTime();
    const previousBlockTime = new Date(
      validMetrics[i - 1].lastUpdated
    ).getTime();
    const delta = currentBlockTime - previousBlockTime;
    if (delta > 0) processingTimes.push(delta);
  }

  const medianProcessingTime =
    processingTimes.length > 0
      ? processingTimes.sort((a, b) => a - b)[
          Math.floor(processingTimes.length / 2)
        ]
      : 0;

  return {
    date: currentDay.date,
    lastUpdated: validMetrics[validMetrics.length - 1]?.lastUpdated,
    blocksProcessed: validMetrics.length,
    medianBlockProcessingTime: medianProcessingTime,
    lastProcessedBlock: {
      number: Math.max(...dailyBlocks.map(Number)),
      timestamp: validMetrics[validMetrics.length - 1]?.timestamp,
    },
  };
}

async function getProcessedDaysStats(keys, start, end) {
  const timeseriesData = [];
  let currentDay = new Date(start);

  while (currentDay <= end) {
    const dateStr = currentDay.toISOString().split("T")[0];
    const metricsStr = await qnLib.qnGetSet(keys.dailyMetrics(dateStr));

    if (metricsStr) {
      const metrics = JSON.parse(metricsStr);
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
        medianBlockProcessingTime: metrics.metadata.medianBlockProcessingTime,
        isSequentialWithNextDay: nextDayMetrics
          ? metrics.metadata.lastBlock ===
            JSON.parse(nextDayMetrics).metadata.firstBlock - 1
          : null,
        firstBlockNum: metrics.metadata.firstBlock,
        lastBlockNum: metrics.metadata.lastBlock,
      });
    }
    currentDay.setDate(currentDay.getDate() + 1);
  }

  return { data: timeseriesData };
}

async function findCurrentProcessingDay(keys) {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(yesterday.getDate() - 1);

  // Check today first, then yesterday
  for (const date of [today, yesterday]) {
    const dateStr = date.toISOString().split("T")[0];
    const dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(dateStr));
    if (dailyBlocks?.length) {
      return { date: dateStr };
    }
  }
  return null;
}

async function findMostRecentProcessedDay(keys) {
  const date = new Date();
  for (let i = 0; i < 90; i++) {
    const dateStr = date.toISOString().split("T")[0];
    const metricsStr = await qnLib.qnGetSet(keys.dailyMetrics(dateStr));
    if (metricsStr) {
      return { date: dateStr, metrics: JSON.parse(metricsStr) };
    }
    date.setDate(date.getDate() - 1);
  }
  return null;
}
