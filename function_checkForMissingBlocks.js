// A utility function to check for missing blocks on a given date.
// Only works if prev and next day has processed daily metrics.
// Typical blocks per day:
//      Base: 43199

async function main(params) {
  const date = "2024-10-09";
  const chain = "BASE";
  const prefix = `MA_${chain.toUpperCase()}_`;
  const keys = {
    dailyMetrics: (date) => `${prefix}daily-metrics_${date}`,
    dailyBlocks: (date) => `${prefix}daily-blocks_${date}`,
    dailyAddresses: (date) => `${prefix}daily-active-addresses_${date}`,
    blockMetrics: (blockNum) => `${prefix}block-metrics_${blockNum}`,
    processedBlocks: `${prefix}blocks-processed`,
  };

  let range = await getBlockRangeFromMetrics(keys, date);
  // range.nextDayFirstBlock = 20605327; // use this to override, useful when boundary missing

  if (!range.prevDayLastBlock || !range.nextDayFirstBlock) {
    const missingBoundaries = [
      !range.prevDayLastBlock && "prevDay",
      !range.nextDayFirstBlock && "nextDay",
    ].filter(Boolean);

    return {
      error: `Missing boundary daily metrics: ${missingBoundaries.join(", ")}`,
    };
  }

  const missingBlocks = await checkForMissingBlocks(keys, range, date);

  if (!missingBlocks) {
    return {
      date,
      boundaries: {
        prevDayLastBlock: range.prevDayLastBlock,
        nextDayFirstBlock: range.nextDayFirstBlock,
      },
      missingDailyMetrics: range.processedDayMetadata.target
        ? !range.processedDayMetadata.target
        : true,
      missingBlocks: "Daily blocks list not found",
    };
  }

  const processingState = await checkProcessedBlocksSequence(keys, range, date);
  return {
    date,
    missingBlocks,
    boundaries: {
      prevDayLastBlock: range.prevDayLastBlock,
      nextDayFirstBlock: range.nextDayFirstBlock,
    },
    missingDailyMetrics: range.processedDayMetadata.target
      ? !range.processedDayMetadata.target
      : true,
    totalMissing: missingBlocks.length,
    processingState,
    sequenceValidation: verifyBlockSequence(
      processingState.processedBlocksInRange,
      range
    ),
  };
}

async function checkForMissingBlocks(keys, range, date) {
  const missingBlocks = [];
  const { prevDayLastBlock, nextDayFirstBlock } = range;

  const dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(date));

  if (!dailyBlocks || !dailyBlocks.length) {
    return null;
  }

  const blockNumbers = dailyBlocks.map(Number).sort((a, b) => a - b);

  if (prevDayLastBlock && nextDayFirstBlock && blockNumbers.length) {
    if (blockNumbers[0] > prevDayLastBlock + 1) {
      for (let j = prevDayLastBlock + 1; j < blockNumbers[0]; j++) {
        missingBlocks.push(j);
      }
    }
    if (blockNumbers[blockNumbers.length - 1] < nextDayFirstBlock - 1) {
      for (
        let j = blockNumbers[blockNumbers.length - 1] + 1;
        j < nextDayFirstBlock;
        j++
      ) {
        missingBlocks.push(j);
      }
    }
    for (let i = 0; i < blockNumbers.length - 1; i++) {
      const block = blockNumbers[i];
      const nextBlock = blockNumbers[i + 1];
      if (nextBlock - block > 1) {
        for (let j = block + 1; j < nextBlock; j++) {
          missingBlocks.push(j);
        }
      }
    }
  }

  return missingBlocks;
}

async function checkProcessedBlocksSequence(keys, range, date) {
  const processedBlocks = await qnLib.qnGetList(keys.processedBlocks);
  const dailyBlocks = await qnLib.qnGetList(keys.dailyBlocks(date));

  return {
    processedBlocksInRange: processedBlocks
      .map(Number)
      .filter((b) => b > range.prevDayLastBlock && b < range.nextDayFirstBlock),
    dailyBlocksPresent: dailyBlocks.length > 0,
    dailyAddressesPresent:
      (await qnLib.qnGetList(keys.dailyAddresses(date))).length > 0,
    temporaryBlockMetricsPresent: await checkTemporaryBlockMetrics(
      keys,
      dailyBlocks
    ),
  };
}

async function checkTemporaryBlockMetrics(keys, blockNumbers) {
  // Get all sets at once
  const allSets = await qnLib.qnListAllSets();
  const blockMetricsPrefix = keys.blockMetrics("").split("_")[0] + "_";

  // Filter for block metrics sets
  const existingBlockMetrics = allSets
    .filter((key) => key.startsWith(blockMetricsPrefix))
    .map((key) => parseInt(key.split("_").pop()));

  // Check which block numbers have metrics
  return blockNumbers
    .map(Number)
    .filter((blockNum) => existingBlockMetrics.includes(blockNum));
}

async function getBlockRangeFromMetrics(keys, targetDate) {
  // Get adjacent dates
  const targetDateObj = new Date(targetDate);
  const prevDate = new Date(targetDateObj);
  prevDate.setDate(prevDate.getDate() - 1);
  const nextDate = new Date(targetDateObj);
  nextDate.setDate(nextDate.getDate() + 1);

  const dates = {
    prevDay: prevDate.toISOString().split("T")[0],
    target: targetDate,
    nextDay: nextDate.toISOString().split("T")[0],
  };

  // Get metrics for all 3 days
  const processedDayMetadata = {};
  for (const [key, date] of Object.entries(dates)) {
    const metricsStr = await qnLib.qnGetSet(keys.dailyMetrics(date));
    if (metricsStr) {
      processedDayMetadata[key] = JSON.parse(metricsStr).metadata;
    }
  }

  return {
    prevDayLastBlock: processedDayMetadata.prevDay?.lastBlock
      ? processedDayMetadata.prevDay.lastBlock
      : null,
    nextDayFirstBlock: processedDayMetadata.nextDay?.firstBlock
      ? processedDayMetadata.nextDay.firstBlock
      : null,
    processedDayMetadata,
  };
}

function verifyBlockSequence(blocks, range) {
  const sorted = blocks.map(Number).sort((a, b) => a - b);
  return {
    hasGaps: !sorted.every(
      (block, i) => i === 0 || block === sorted[i - 1] + 1
    ),
    firstBlock: sorted[0],
    lastBlock: sorted[sorted.length - 1],
    expectedFirst: range.prevDayLastBlock + 1,
    expectedLast: range.nextDayFirstBlock - 1,
  };
}
