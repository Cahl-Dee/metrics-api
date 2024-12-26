// TO DO:
// - don't delete anything or write anything until we have successfully done the calculations
// - don't have default values for the values at the top, stream needs to provide these
// - make sure first and last block are correct via RPC calls

async function main(params) {
  console.log(params);
  const simulateOnly = params.user_data?.simulateOnly ?? false;
  const cleanupEnabled = params.user_data?.cleanup ?? true; // superceded by simulate only flag

  const date = params.user_data?.date || "2024-12-23";
  const chain = params.user_data?.chain || "ETH";

  const prefix = `MA_${chain.toUpperCase()}_`;
  const keys = {
    dailyMetrics: (date) => `${prefix}daily-metrics_${date}`,
    dailyBlocks: (date) => `${prefix}daily-blocks_${date}`,
    dailyAddresses: (date) => `${prefix}daily-active-addresses_${date}`,
    blockMetrics: (blockNum) => `${prefix}block-metrics_${blockNum}`,
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
    true,
    simulateOnly,
    cleanupEnabled
  );
  let returnObj = {};

  if (simulateOnly) {
    returnObj = dailyMetrics;
  } else {
    await qnLib.qnAddSet(keys.dailyMetrics(date), JSON.stringify(dailyMetrics));
    returnObj = {
      status: "success",
      date: date,
      message: "Daily metrics calculated and stored successfully",
      data: dailyMetrics,
    };
  }

  return returnObj;
}

async function calculateDailyMetrics(
  date,
  blockNumbers,
  keys,
  hasDebugTrace,
  simulateOnly,
  cleanupEnabled
) {
  let numTransactions = 0;
  let totalFees = 0;
  let numContractDeployments = 0;
  let firstBlock = null;
  let lastBlock = null;
  let firstBlockTimestamp = null;
  let lastBlockTimestamp = null;
  let numProcessedBlocks = 0;
  let failedBlocks = [];
  let sequenceErrors = [];

  // Sort blocks first
  blockNumbers.sort((a, b) => a - b);

  // Check sequence before processing
  for (let i = 1; i < blockNumbers.length; i++) {
    const expected = blockNumbers[i - 1] + 1;
    const actual = blockNumbers[i];
    if (actual !== expected) {
      sequenceErrors.push({
        gap: {
          start: blockNumbers[i - 1],
          end: blockNumbers[i],
          missing: actual - expected,
        },
      });
    }
  }

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

  // Get contract deployment coverage from first valid block metric
  const coverageType =
    blockMetricsResults.find((r) => r?.blockMetrics)?.blockMetrics
      .contractDeploymentCoverage || "unknown";

  // Calculate median processing time
  const processingTimes = blockMetricsResults
    .filter(
      (result) =>
        result?.blockMetrics?.lastUpdated && result?.blockMetrics?.timestamp
    )
    .map(({ blockMetrics }) => {
      const processedAt = new Date(blockMetrics.lastUpdated).getTime();
      const blockTime = blockMetrics.timestamp * 1000; // Convert to milliseconds
      return processedAt - blockTime;
    })
    .sort((a, b) => a - b);

  const medianProcessingTime =
    processingTimes.length > 0
      ? processingTimes[Math.floor(processingTimes.length / 2)]
      : 0;

  // After metrics calculation and before return, add cleanup logic
  if (!simulateOnly && cleanupEnabled) {
    // Cleanup temporary lists
    await qnLib.qnDeleteList(keys.dailyBlocks(date));
    await qnLib.qnDeleteList(keys.dailyAddresses(date));

    // Clean up block metrics
    const blockMetricsToDelete = blockNumbers.map((num) =>
      keys.blockMetrics(num)
    );
    await qnLib.qnBulkSets({ delete_sets: blockMetricsToDelete });
  }

  return {
    metrics: {
      numTransactions,
      avgTxFee,
      totalFees,
      avgBlockFees,
      numContractDeployments,
      contractDeploymentCoverage: coverageType,
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
      isSequential: sequenceErrors.length === 0,
      sequenceErrors,
      isComplete: failedBlocks.length === 0 && sequenceErrors.length === 0,
      lastUpdated: new Date().toISOString(),
      cleanupPerformed: !simulateOnly && cleanupEnabled,
      medianBlockProcessingTime: medianProcessingTime,
    },
  };
}
