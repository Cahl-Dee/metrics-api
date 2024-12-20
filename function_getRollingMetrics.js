async function main(params) {
  if (!params?.user_data) {
    params.user_data = {};
  }

  const {
    chain = "ETH", // string            | optional, defaults to ETH
    date, // YYYY-MM-DD string | optional, defaults to latest
    metric = "all", // string            | specific metric or 'all', dafaults to all
    days = 7, // number            | 7, 30, or 90
  } = params.user_data;

  const prefix = `MA_${chain.toUpperCase()}_`;
  const dailyMetricsPrefix = `${prefix}daily-metrics_`;

  try {
    const targetDate = date
      ? date
      : await getLatestProcessedDate(dailyMetricsPrefix);
    const { start, end, prevStart, prevEnd } = calculateDateRanges(
      targetDate,
      days
    );

    const currentPeriodMetrics = await getPeriodMetrics(
      start,
      end,
      dailyMetricsPrefix
    );
    const previousPeriodMetrics = await getPeriodMetrics(
      prevStart,
      prevEnd,
      dailyMetricsPrefix
    );

    return formatResponse(
      chain,
      days,
      start,
      end,
      prevStart,
      prevEnd,
      currentPeriodMetrics,
      previousPeriodMetrics,
      metric
    );
  } catch (e) {
    throw new Error(`Failed to retrieve rolling metrics: ${e.message}`);
  }
}

async function getLatestProcessedDate(dailyMetricsPrefix) {
  const sets = await qnLib.qnListAllSets();
  const dates = sets
    .filter((set) => set.startsWith(dailyMetricsPrefix))
    .map((set) => set.slice(-10))
    .sort();

  if (!dates.length) {
    throw new Error("No metrics data available");
  }

  return dates[dates.length - 1];
}

function calculateDateRanges(targetDate, days) {
  const end = new Date(targetDate);
  end.setUTCHours(0, 0, 0, 0);
  const start = new Date(end);
  start.setDate(start.getDate() - days);

  const prevEnd = new Date(start);
  const prevStart = new Date(prevEnd);
  prevStart.setDate(prevStart.getDate() - days);

  return {
    start: start.toISOString().split("T")[0],
    end: end.toISOString().split("T")[0],
    prevStart: prevStart.toISOString().split("T")[0],
    prevEnd: prevEnd.toISOString().split("T")[0],
  };
}

async function getPeriodMetrics(startDate, endDate, prefix) {
  const metrics = {
    numTransactions: 0,
    totalFees: 0,
    numContractDeployments: 0,
    contractDeploymentCoverage: "partial",
    numActiveAddresses: 0,
    totalBlockTime: 0,
    numBlocks: 0,
    daysWithData: 0,
  };

  let currentDate = new Date(startDate);
  const end = new Date(endDate);

  while (currentDate <= end) {
    const dateStr = currentDate.toISOString().split("T")[0];
    try {
      const dayMetricsStr = await qnLib.qnGetSet(`${prefix}${dateStr}`);

      if (dayMetricsStr) {
        const dayMetrics = JSON.parse(dayMetricsStr);
        metrics.numTransactions += dayMetrics.metrics.numTransactions;
        metrics.totalFees += dayMetrics.metrics.totalFees;
        metrics.numContractDeployments +=
          dayMetrics.metrics.numContractDeployments;
        metrics.contractDeploymentCoverage =
          dayMetrics.metrics.contractDeploymentCoverage;
        metrics.numActiveAddresses += dayMetrics.metrics.numActiveAddresses;
        metrics.totalBlockTime +=
          dayMetrics.metrics.avgBlockTime *
          (dayMetrics.metadata.lastBlock - dayMetrics.metadata.firstBlock);
        metrics.numBlocks +=
          dayMetrics.metadata.lastBlock - dayMetrics.metadata.firstBlock;
        metrics.daysWithData++;
      }
    } catch (e) {
      console.error(
        `Error processing metrics for date ${dateStr}: ${e.message}`
      );
    }

    currentDate.setDate(currentDate.getDate() + 1);
  }

  return calculateAverages(metrics);
}

function calculateAverages(metrics) {
  if (metrics.daysWithData === 0) return { metrics: {} };

  const avgDailyTransactions = metrics.numTransactions / metrics.daysWithData;

  return {
    metrics: {
      numTransactions: avgDailyTransactions,
      tps: avgDailyTransactions / 86400,
      avgTxFee: metrics.totalFees / metrics.numTransactions,
      totalFees: metrics.totalFees / metrics.daysWithData,
      avgBlockFees: metrics.totalFees / metrics.numBlocks,
      numContractDeployments:
        metrics.numContractDeployments / metrics.daysWithData,
      contractDeploymentCoverage: metrics.contractDeploymentCoverage,
      numActiveAddresses: metrics.numActiveAddresses / metrics.daysWithData,
      avgBlockTime: metrics.totalBlockTime / metrics.numBlocks,
    },
    daysWithData: metrics.daysWithData,
  };
}

function formatResponse(
  chain,
  days,
  start,
  end,
  prevStart,
  prevEnd,
  currentPeriod,
  previousPeriod,
  metric
) {
  const response = {
    chain: chain.toLowerCase(),
    period: `${days}d`,
    currentPeriod: { start, end },
    previousPeriod: { start: prevStart, end: prevEnd },
    daysWithData: {
      current: currentPeriod.daysWithData,
      previous: previousPeriod.daysWithData,
    },
  };

  response.metrics = getScopedMetrics(
    currentPeriod.metrics,
    previousPeriod.metrics,
    metric
  );
  return response;
}

function getScopedMetrics(current, previous, metric) {
  if (metric && metric !== "all") {
    return {
      [metric]: {
        current: current[metric] || 0,
        previous: previous[metric] || 0,
        changePct: calculateChange(current[metric], previous[metric]),
      },
    };
  }

  const result = {};
  for (const [key, value] of Object.entries(current)) {
    result[key] = {
      current: value,
      previous: previous[key] || 0,
      changePct: calculateChange(value, previous[key]),
    };
  }
  return result;
}

function calculateChange(current, previous) {
  if (!previous) return current ? 100 : 0;
  return ((current - previous) / previous) * 100;
}
