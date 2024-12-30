// TO DO:
// - make sure first and last block are correct via RPC calls

async function main(params) {
  // Validate input parameters
  if (!params.user_data?.date || !params.user_data?.chain) {
    return {
      error: "Missing required parameters: date and chain must be provided",
    };
  }

  // Validate date format (YYYY-MM-DD)
  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRegex.test(params.user_data.date)) {
    return {
      error: "Invalid date format. Expected YYYY-MM-DD",
      date: params.user_data.date,
    };
  }

  // Validate chain parameter
  if (
    typeof params.user_data.chain !== "string" ||
    !params.user_data.chain.trim()
  ) {
    return {
      error: "Invalid chain parameter. Expected a non-empty string",
      chain: params.user_data.chain,
    };
  }

  // Extract configuration
  const config = {
    simulateOnly: params.user_data.simulateOnly ?? false,
    cleanupEnabled: params.user_data.cleanup ?? true,
    date: params.user_data.date,
    chain: params.user_data.chain,
  };

  // Setup keys
  const prefix = `MA_${config.chain.toUpperCase()}_`;
  const keys = {
    dailyMetrics: (date) => `${prefix}daily-metrics_${date}`,
    dailyBlocks: (date) => `${prefix}daily-blocks_${date}`,
    dailyAddresses: (date) => `${prefix}daily-active-addresses_${date}`,
    blockMetrics: (blockNum) => `${prefix}block-metrics_${blockNum}`,
  };

  // Check if daily metrics already exist
  try {
    const existingMetrics = await qnLib.qnGetSet(
      keys.dailyMetrics(config.date)
    );
    if (existingMetrics) {
      try {
        const parsedMetrics = JSON.parse(existingMetrics);
        return {
          error: `Daily metrics already exist`,
          existingMetrics: parsedMetrics,
          chain: config.chain,
          date: config.date,
        };
      } catch (parseError) {
        return {
          error: `Invalid JSON format in existing metrics: ${parseError.message}`,
          chain: config.chain,
          date: config.date,
          rawMetrics: existingMetrics,
        };
      }
    }
  } catch (error) {
    return {
      error: `Failed to check for existing daily metrics: ${error.message}`,
      chain: config.chain,
      date: config.date,
    };
  }

  // Fetch and validate block numbers
  let blockNumbers;
  try {
    blockNumbers = await qnLib.qnGetList(keys.dailyBlocks(config.date));
    if (!Array.isArray(blockNumbers) || blockNumbers.length === 0) {
      return {
        error: `No blocks found`,
        chain: config.chain,
        date: config.date,
      };
    }
    blockNumbers = blockNumbers.map(Number);
  } catch (error) {
    return {
      error: `Failed to fetch block numbers: ${error.message}`,
      chain: config.chain,
      date: config.date,
    };
  }

  // Calculate metrics (no side effects)
  let dailyMetrics;
  try {
    dailyMetrics = await calculateDailyMetrics(config.date, blockNumbers, keys);

    // Check for missing blocks
    if (dailyMetrics.missingBlocks) {
      return {
        error: "Cannot proceed - missing block metrics",
        chain: config.chain,
        date: config.date,
        totalMissingBlocks: dailyMetrics.totalMissing,
        missingBlocksSample: dailyMetrics.sampleMissing,
        message: `Missing ${dailyMetrics.totalMissing} blocks`,
      };
    }

    if (!dailyMetrics?.metrics || !dailyMetrics?.metadata) {
      return {
        error: "Failed to calculate daily metrics",
        chain: config.chain,
        date: config.date,
      };
    }
  } catch (error) {
    return {
      error: `Metrics calculation failed: ${error.message}`,
      chain: config.chain,
      date: config.date,
    };
  }

  // Handle storage and cleanup
  if (config.simulateOnly) {
    return {
      status: "success",
      dailyMetricsWritten: false,
      cleanupPerformed: false,
      date: config.date,
      chain: config.chain,
      message: "Simulation complete",
      data: dailyMetrics,
    };
  }

  try {
    // Store metrics
    await qnLib.qnAddSet(
      keys.dailyMetrics(config.date),
      JSON.stringify(dailyMetrics)
    );

    // Perform cleanup if enabled
    if (config.cleanupEnabled) {
      await performCleanup(config.date, blockNumbers, keys);
      return {
        status: "success",
        date: config.date,
        chain: config.chain,
        dailyMetricsWritten: true,
        cleanupPerformed: true,
        message: "Metrics stored and cleanup completed",
        data: dailyMetrics,
      };
    }

    return {
      status: "success",
      date: config.date,
      chain: config.chain,
      dailyMetricsWritten: true,
      cleanupPerformed: false,
      message: "Metrics stored successfully",
      data: dailyMetrics,
    };
  } catch (error) {
    return {
      error: `Storage/cleanup failed: ${error.message}`,
      date: config.date,
      chain: config.chain,
      data: dailyMetrics,
    };
  }
}

async function calculateDailyMetrics(date, blockNumbers, keys) {
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

  // Early check for missing blocks
  const missingBlocks = blockMetricsResults
    .map((result, index) => (!result ? blockNumbers[index] : null))
    .filter((block) => block !== null);

  if (missingBlocks.length > 0) {
    return {
      missingBlocks: true,
      totalMissing: missingBlocks.length,
      sampleMissing: missingBlocks.slice(0, 10),
      allMissingBlocks: missingBlocks,
    };
  }

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
  const sortedMetrics = blockMetricsResults
    .filter((result) => result?.blockMetrics?.lastUpdated)
    .sort((a, b) => a.blockMetrics.timestamp - b.blockMetrics.timestamp);

  const processingTimes = [];
  for (let i = 1; i < sortedMetrics.length; i++) {
    const currentUpdated = new Date(
      sortedMetrics[i].blockMetrics.lastUpdated
    ).getTime();
    const previousUpdated = new Date(
      sortedMetrics[i - 1].blockMetrics.lastUpdated
    ).getTime();
    const diff = currentUpdated - previousUpdated;
    if (diff > 0) processingTimes.push(diff);
  }

  const medianProcessingTime =
    processingTimes.length > 0
      ? processingTimes.sort((a, b) => a - b)[
          Math.floor(processingTimes.length / 2)
        ]
      : 0;

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
      cleanupPerformed: false, // Will be updated after cleanup
      medianBlockProcessingTime: medianProcessingTime,
    },
  };
}

async function performCleanup(date, blockNumbers, keys) {
  // Cleanup temporary lists
  await qnLib.qnDeleteList(keys.dailyBlocks(date));
  await qnLib.qnDeleteList(keys.dailyAddresses(date));

  // Clean up block metric sets
  const blockMetricsToDelete = blockNumbers.map((num) =>
    keys.blockMetrics(num)
  );
  await qnLib.qnBulkSets({ delete_sets: blockMetricsToDelete });
}
