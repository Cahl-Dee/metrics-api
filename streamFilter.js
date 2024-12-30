function main(params) {
  // When debugging, set to true to simulate processing without writing to KV store, then uncomment returns you'll see at key points below
  const simulateOnly = false;

  // Chain specific configs, adjust as needed
  const chain = "ETH";
  const decimals = 18;
  const hasDebugTrace = true;

  // Centralized key definitions
  const prefix = `MA_${chain.toUpperCase()}_`;
  const keys = {
    blockMetrics: (blockNumber) => `${prefix}block-metrics_${blockNumber}`,
    dailyMetrics: (date) => `${prefix}daily-metrics_${date}`,
    dailyAddresses: (date) => `${prefix}daily-active-addresses_${date}`,
    dailyBlocks: (date) => `${prefix}daily-blocks_${date}`,
  };

  // Validate we have expected dataset (only works when stream is live)
  if (params.metadata?.dataset) {
    if (
      hasDebugTrace &&
      params.metadata.dataset !== "block_with_receipts_debug_trace"
    ) {
      return {
        error: "Unexpected dataset, expected block_with_receipts_debug_trace",
      };
    }
    if (!hasDebugTrace && params.metadata.dataset !== "block_with_receipts") {
      return { error: "Unexpected dataset, expected block_with_receipts" };
    }
  }

  // Extract top level data
  const data = params.data ? params.data[0] : params[0];
  const block = data.block;
  const receipts = data.receipts;
  const trace = hasDebugTrace ? data.trace : null;

  const blockNumber = parseInt(block.number, 16);
  const blockTimestamp = parseInt(block.timestamp, 16);
  const blockDate = new Date(blockTimestamp * 1000).toISOString().split("T")[0];

  // Check processed blocks
  if (qnContainsListItem(keys.dailyBlocks(blockDate), blockNumber.toString())) {
    // use this when testing - console logging will be added in future versions, for now we need to return the object
    // const logObj = {
    //     blockNumber,
    //     blockDate,
    //     status: 'already_processed'
    // };
    // return logObj;

    return null;
  }

  // Calculate block metrics
  let totalFeesWei = BigInt(0);
  const weiPerEth = BigInt(10) ** BigInt(decimals);
  const numContractDeployments = countContractDeployments(
    receipts,
    hasDebugTrace ? trace : null
  );
  const activeAddresses = new Set();

  // Process transactions
  for (let i = 0; i < block.transactions.length; i++) {
    const tx = block.transactions[i];
    const receipt = receipts[i];

    // Track active addresses
    if (tx.from) {
      activeAddresses.add(tx.from.toLowerCase());
    }

    // Calculate fees
    try {
      const gasUsed = BigInt(receipt.gasUsed); // Actual gas used from receipt
      const gasPrice = BigInt(tx.gasPrice);
      totalFeesWei += gasUsed * gasPrice;
    } catch (e) {
      console.error(
        `Error calculating fees for tx ${tx.hash} in block ${blockNumber}: ${e.message}`
      );
    }
  }
  // Add active addresses in a single upsert
  if (!simulateOnly) {
    qnUpsertList(keys.dailyAddresses(blockDate), {
      add_items: Array.from(activeAddresses),
    });
  }
  // use this when testing - console logging will be added in future versions, for now we need to return the object
  // return activeAddresses;

  // Store block metrics
  const blockMetrics = {
    timestamp: blockTimestamp,
    date: blockDate,
    numTransactions: block.transactions.length,
    totalFees: Number(totalFeesWei) / Number(weiPerEth),
    numContractDeployments,
    contractDeploymentCoverage: hasDebugTrace ? "full" : "partial",
    lastUpdated: new Date().toISOString(),
  };
  // use this when testing - console logging will be added in future versions, for now we need to return the object
  // return blockMetrics;

  if (!simulateOnly) {
    // Store block metrics
    qnAddSet(keys.blockMetrics(blockNumber), JSON.stringify(blockMetrics));

    // Mark block as processed in daily list
    qnAddListItem(keys.dailyBlocks(blockDate), blockNumber.toString());
  }

  // Process previous day metrics if needed
  const prevBlockNumber = blockNumber - 1;
  const prevBlockMetricsStr = qnGetSet(keys.blockMetrics(prevBlockNumber));
  let prevBlockDate;

  if (prevBlockMetricsStr) {
    const prevBlockMetrics = JSON.parse(prevBlockMetricsStr);
    prevBlockDate = prevBlockMetrics.date;

    // If previous block was in a different day
    if (prevBlockDate && prevBlockDate !== blockDate) {
      return {
        user_data: {
          chain: chain,
          date: prevBlockDate,
          simulateOnly: simulateOnly,
        },
      };
    }
  }

  // use this when testing - console logging will be added in future versions, for now we need to return the object
  // const logObj = {
  //     blockNumber,
  //     blockDate,
  //     status: 'block_processed'
  // };
  // return logObj;

  return null;
}

function countContractDeployments(receipts, traces) {
  let numDeployments = 0;

  if (traces) {
    const processTrace = (trace) => {
      const item = trace.result ? trace.result : trace;
      if (item.type && ["CREATE", "CREATE2"].includes(item.type)) {
        numDeployments++;
      }
      if (item.calls) {
        item.calls.forEach(processTrace);
      }
    };
    traces.forEach(processTrace);
  } else if (receipts) {
    // If no trace available, count receipts with contract addresses
    numDeployments = receipts.filter(
      (receipt) => receipt.contractAddress
    ).length;
  }

  return numDeployments;
}
