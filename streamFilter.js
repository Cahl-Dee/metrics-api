function main(params) {
    const simulateOnly = true; // Set to true to simulate processing without writing to KV store, then add returns with data

    // Chain specific configs, adjust as needed
    const chain = 'ETH';
    const decimals = 18;
    const hasDebugTrace = true;

    if (params.metadata && params.metadata.dataset && hasDebugTrace && params.metadata.dataset != 'block_with_receipts_debug_trace') {
        return { error: 'Unexpected dataset, expected block_with_receipts_debug_trace' }
    }
    if (params.metadata && params.metadata.dataset && !hasDebugTrace && params.metadata.dataset != 'block_with_receipts') {
        return { error: 'Unexpected dataset, expected block_with_receipts' }
    }

    const PREFIX = `MA_${chain.toUpperCase()}_`;        // used to prefix set and list names
    const WEI_PER_ETH = BigInt(10) ** BigInt(decimals); // wei = smallest unit of native asset | eth = native asset
    
    // Extract top level data
    const data = params.data ? params.data[0] : params[0];
    const block = data.block;
    const receipts = data.receipts;
    const trace = hasDebugTrace ? data.trace : null;

    const blockNumber = parseInt(block.number, 16);
    const blockTimestamp = parseInt(block.timestamp, 16);
    const blockDate = new Date(blockTimestamp * 1000).toISOString().split('T')[0];
    const lastUpdated = new Date().toISOString();

    // Check if already processed
    const processedBlocksKey = `${PREFIX}processed_blocks`;
    const processedBlocks = qnGetList(processedBlocksKey);
    if (processedBlocks.includes(blockNumber.toString())) {
        // use this when testing - console logging will be added in future versions, for now we need to return the object
        // const logObj = {
        //     blockNumber,
        //     blockDate,
        //     status: 'already_processed'
        // };
        // // return logObj;

        return null;
    }
    
    // Calculate block metrics
    let blockFeesWei = BigInt(0);
    const contractDeploymentCount = countContractDeployments(receipts, hasDebugTrace ? trace : null);
    const activeAddresses = new Set();
    
    // Process transactions
    for (const tx of block.transactions) {
        // Track active addresses
        if (tx.from) {
            activeAddresses.add(tx.from.toLowerCase());
        }
        
        // Calculate fees
        try {
            const gas = BigInt(tx.gas);
            const gasPrice = BigInt(tx.gasPrice);
            blockFeesWei += gas * gasPrice;
        } catch (e) {
            console.error(`Error calculating fees for tx ${tx.hash} in block ${blockNumber}: ${e.message}`);
        }
    }
    // Add active addresses in a single upsert
    if(!simulateOnly) qnUpsertList(`${PREFIX}addresses_${blockDate}`, { add_items: activeAddresses });
    
    // Store block metrics
    const blockMetricsKey = `${PREFIX}block_metrics_${blockNumber.toString()}`;
    const blockMetrics = {
        timestamp: blockTimestamp,
        date: blockDate,
        transactions: block.transactions.length,
        fees: Number(blockFeesWei) / Number(WEI_PER_ETH),
        contractDeploymentsTotal: contractDeploymentCount,
        contractDeploymentCoverage: hasDebugTrace ? 'full' : 'partial',
        lastUpdated
    };
    if(!simulateOnly) qnAddSet(blockMetricsKey, JSON.stringify(blockMetrics));
    // use this when testing - console logging will be added in future versions, for now we need to return the object
    // return blockMetrics;
    
    // Mark block as processed
    if(!simulateOnly) qnAddListItem(processedBlocksKey, blockNumber.toString());
    if(!simulateOnly) qnAddListItem(`${PREFIX}blocks_${blockDate}`, blockNumber.toString());
    
    // Check previous block's date
    const prevBlockNumber = blockNumber - 1;
    const prevBlockMetricsStr = qnGetSet(`${PREFIX}block_metrics_${prevBlockNumber.toString()}`);
    let prevBlockDate;
    if (prevBlockMetricsStr) {
        const prevBlockMetrics = JSON.parse(prevBlockMetricsStr);
        prevBlockDate = prevBlockMetrics.date;
    }
    
    // If previous block was in a different day
    if (prevBlockDate && prevBlockDate !== blockDate) {
        // Check if previous day is complete
        const prevDayBlocks = qnGetList(`${PREFIX}blocks_${prevBlockDate}`).map(Number).sort((a, b) => a - b);
        
        // Verify sequence is complete
        const isComplete = prevDayBlocks.length > 0 && 
                          prevDayBlocks[prevDayBlocks.length - 1] === prevBlockNumber &&
                          prevDayBlocks.every((block, index) => 
                              index === 0 || block === prevDayBlocks[index - 1] + 1);
        
        if (isComplete) {
            // Process the completed day
            const dayMetrics = calculateDayMetrics(prevBlockDate, prevDayBlocks, PREFIX, hasDebugTrace);
            
            // Store final metrics
            dayMetrics.lastUpdated = lastUpdated;
            if(!simulateOnly) qnAddSet(`${PREFIX}metrics_${prevBlockDate}`, JSON.stringify(dayMetrics));
            
            // Cleanup temporary data
            if(!simulateOnly) qnDeleteList(`${PREFIX}blocks_${prevBlockDate}`);
            if(!simulateOnly) qnDeleteList(`${PREFIX}addresses_${prevBlockDate}`);
            
            // Clean up block metrics
            const setsForDeletion = [];
            for (const blockNum of prevDayBlocks) {
                setsForDeletion.push(`${PREFIX}block_metrics_${blockNum}`);
            }
            if(!simulateOnly) qnBulkSets({ delete_sets: setsForDeletion });
            
            // use this when testing - console logging will be added in future versions, for now we need to return the object
            // const logObj = {
            //     blockNumber,
            //     blockDate,
            //     status: 'day_completed',
            //     completedDate: prevBlockDate,
            //     metrics: dayMetrics
            // };
            // return logObj;
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

function calculateDayMetrics(date, blockNumbers, prefix, hasDebugTrace) {
    let totalTransactions = 0;
    let totalFeesEth = 0;
    let totalContractCreations = 0;
    let firstBlock = null;
    let lastBlock = null;
    let firstBlockTimestamp = null;
    let lastBlockTimestamp = null;
    let successfulBlocks = 0;
    let failedBlocks = [];
    const lastUpdated = new Date().toISOString();
    
    // Process each block in the day
    for (const blockNumber of blockNumbers) {
        try {
            const blockMetricsStr = qnGetSet(`${prefix}block_metrics_${blockNumber}`);
            if (!blockMetricsStr) {
                failedBlocks.push({
                    block: blockNumber,
                    reason: 'Missing block metrics'
                });
                continue;
            }
            
            const blockMetrics = JSON.parse(blockMetricsStr);
            
            // Update block range info
            if (!firstBlock || blockNumber < firstBlock) {
                firstBlock = blockNumber;
                firstBlockTimestamp = blockMetrics.timestamp;
            }
            if (!lastBlock || blockNumber > lastBlock) {
                lastBlock = blockNumber;
                lastBlockTimestamp = blockMetrics.timestamp;
            }
            
            totalTransactions += blockMetrics.transactions;
            totalFeesEth += blockMetrics.fees;
            totalContractCreations += blockMetrics.contractDeploymentsTotal;
            successfulBlocks++;
            
        } catch (e) {
            failedBlocks.push({
                block: blockNumber,
                reason: e.message
            });
            continue;
        }
    }
    
    // Get active addresses
    let activeAddresses = 0;
    try {
        activeAddresses = qnGetList(`${prefix}addresses_${date}`).length;
    } catch (e) {
        console.error(`Failed to get active addresses for date ${date}: ${e.message}`);
    }
    
    // Calculate averages
    const averageTxCostEth = totalTransactions > 0 ? totalFeesEth / totalTransactions : 0;
    const averageFeesPerBlock = successfulBlocks > 0 ? totalFeesEth / successfulBlocks : 0;
    const averageBlockTime = (lastBlockTimestamp - firstBlockTimestamp) / (lastBlock - firstBlock);
    
    const metrics = {
        date,
        blockCount: blockNumbers.length,
        successfulBlocks,
        failedBlocksCount: failedBlocks.length,
        blocks: blockNumbers,
        totalTransactions,
        totalFees: totalFeesEth,
        averageTxCostEth,
        averageFeesPerBlock,
        totalContractCreations,
        contractDeploymentCoverage: hasDebugTrace ? 'full' : 'partial',
        activeAddresses,
        firstBlock,
        lastBlock,
        firstBlockTimestamp,
        lastBlockTimestamp,
        averageBlockTime,
        isComplete: true,
        lastUpdated
    };
    
    // use this when testing - console logging will be added in future versions, for now we need to return the object
    // return getProcessingResults(date, metrics, failedBlocks);
    
    return metrics;
}

function countContractDeployments(receipts, traces) {
    let deploymentCount = 0;

    if (traces) {
        const processTrace = (traceItem) => {
            // Normalize item structure regardless of top level or call
            const item = traceItem.result ? traceItem.result : traceItem;

            if (item.type && (item.type === 'CREATE' || item.type === 'CREATE2')) deploymentCount++;
            
            // Process nested calls within
            if (item.calls) {
                item.calls.forEach(call => {
                    processTrace(call);
                });
            }
        };

        // Process each trace entry
        traces.forEach(processTrace);

    } else if (receipts) {
        // If no trace available, count receipts with contract addresses
        deploymentCount = receipts.filter(receipt => receipt.contractAddress).length;
    }

    return deploymentCount;
}

function getProcessingResults(date, metrics, failedBlocks) {
    // Format failed blocks
    let failedBlocksLog = '';
    if (failedBlocks.length > 0) {
        const numFailedBlocksToDisplay = Math.min(failedBlocks.length, 10);
        const displayBlocks = failedBlocks.slice(0, numFailedBlocksToDisplay);
        const remaining = failedBlocks.length - numFailedBlocksToDisplay;
        
        failedBlocksLog = displayBlocks
            .map(fb => `Block ${fb.block}: ${fb.reason}`)
            .join('\n');
            
        if (remaining > 0) {
            failedBlocksLog += `\n...and ${remaining} more failed blocks`;
        }
    }
    
    const results = {
        date,
        processingResults: {
            totalBlocks: metrics.blockCount,
            successfulBlocks: metrics.successfulBlocks,
            failedBlocks: failedBlocks.length,
            totalTransactions: metrics.totalTransactions,
            totalContractCreations: metrics.totalContractCreations,
            activeAddresses: metrics.activeAddresses
        },
        metrics,
        failedBlocks: failedBlocksLog || 'No failed blocks'
    };

    return results;
}