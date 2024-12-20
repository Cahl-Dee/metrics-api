async function main(params) {
  const simulateOnly = true;

  const date = params.user_data?.date || "2024-10-09";
  const chain = params.user_data?.chain || "BASE";

  const prefix = `MA_${chain.toUpperCase()}_`;
  const keys = {
    dailyMetrics: (date) => `${prefix}daily-metrics_${date}`,
    dailyBlocks: (date) => `${prefix}daily-blocks_${date}`,
    dailyAddresses: (date) => `${prefix}daily-active-addresses_${date}`,
    blockMetrics: (blockNum) => `${prefix}block-metrics_${blockNum}`,
    processedBlocks: `${prefix}blocks-processed`,
  };

  const dailyBlocksKey = keys.dailyBlocks(date);

  let blockNumbers = await qnLib.qnGetList(dailyBlocksKey);
  if (!Array.isArray(blockNumbers)) {
    return {
      error: `Failed to fetch an array of block numbers for the specified date: ${date}`,
    };
  }
  blockNumbers = blockNumbers.map(Number);

  if (blockNumbers.length === 0) {
    return { error: `No blocks found for the specified date: ${date}` };
  }

  const dailyMetrics = await calculateDailyMetrics(
    date,
    blockNumbers,
    keys,
    true
  );
  let returnObj = {};

  if (simulateOnly) {
    returnObj = dailyMetrics;
  } else {
    returnObj = await qnLib.qnAddSet(
      keys.dailyMetrics(date),
      JSON.stringify(dailyMetrics)
    );
  }

  return returnObj;
}

async function calculateDailyMetrics(date, blockNumbers, keys, hasDebugTrace) {
  let numTransactions = 0;
  let totalFees = 0;
  let numContractDeployments = 0;
  let firstBlock = null;
  let lastBlock = null;
  let firstBlockTimestamp = null;
  let lastBlockTimestamp = null;
  let numProcessedBlocks = 0;
  let failedBlocks = [];

  const blockMetricsPromises = blockNumbers.map(async (blockNumber) => {
    try {
      const blockMetricsStr = await qnLib.qnGetSet(
        keys.blockMetrics(blockNumber)
      );
      if (!blockMetricsStr) {
        failedBlocks.push({
          block: blockNumber,
          reason: "Missing block metrics",
        });
        return null;
      }
      const blockMetrics = JSON.parse(blockMetricsStr);
      return { blockNumber, blockMetrics };
    } catch (e) {
      failedBlocks.push({ block: blockNumber, reason: e.message });
      return null;
    }
  });

  const blockMetricsResults = await Promise.all(blockMetricsPromises);

  blockMetricsResults.forEach(({ blockNumber, blockMetrics }) => {
    if (!blockMetrics) return;

    if (!firstBlock || blockNumber < firstBlock) {
      firstBlock = blockNumber;
      firstBlockTimestamp = blockMetrics.timestamp;
    }
    if (!lastBlock || blockNumber > lastBlock) {
      lastBlock = blockNumber;
      lastBlockTimestamp = blockMetrics.timestamp;
    }

    numTransactions += blockMetrics.numTransactions;
    totalFees += blockMetrics.totalFees;
    numContractDeployments += blockMetrics.numContractDeployments;
    numProcessedBlocks++;
  });

  let numActiveAddresses = 0;
  try {
    const activeAddresses = await qnLib.qnGetList(keys.dailyAddresses(date));
    numActiveAddresses = activeAddresses ? activeAddresses.length : 0;
  } catch (e) {
    console.error(
      `Failed to get active addresses for date ${date}: ${e.message}`
    );
  }

  const avgTxFee = numTransactions > 0 ? totalFees / numTransactions : 0;
  const avgBlockFees =
    numProcessedBlocks > 0 ? totalFees / numProcessedBlocks : 0;
  const avgBlockTime =
    firstBlockTimestamp && lastBlockTimestamp && lastBlock > firstBlock
      ? (lastBlockTimestamp - firstBlockTimestamp) / (lastBlock - firstBlock)
      : 0;

  return {
    metrics: {
      numTransactions,
      avgTxFee,
      totalFees,
      avgBlockFees,
      numContractDeployments,
      contractDeploymentCoverage: hasDebugTrace ? "full" : "partial",
      numActiveAddresses,
      avgBlockTime,
    },
    metadata: {
      date,
      firstBlock,
      lastBlock,
      firstBlockTimestamp,
      lastBlockTimestamp,
      numBlocks: blockNumbers.length,
      numProcessedBlocks,
      numFailedBlocks: failedBlocks.length,
      failedBlocks,
      isComplete: true,
      lastUpdated: new Date().toISOString(),
    },
  };
}
