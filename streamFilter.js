function main(params) {
    // chain specific configs, adjust as needed
    const chain = 'ETH';
    const decimals = 18;

    const PREFIX = `MA_${chain.toUpperCase()}_`;        // used to prefix set and list names
    const WEI_PER_ETH = BigInt(10) ** BigInt(decimals); // wei = smallest unit of native asset | eth = native asset
    
    const block = params[0].block;
    const blockNumber = parseInt(block.number, 16);
    const blockTimestamp = parseInt(block.timestamp, 16);
    const blockDate = new Date(blockTimestamp * 1000).toISOString().split('T')[0];
    
    // Check if already processed
    const processedBlocksKey = `${PREFIX}processed_blocks`;
    const processedBlocks = qnGetList(processedBlocksKey);
    if (processedBlocks.includes(blockNumber.toString())) {
        const logObj = {
            blockNumber,
            blockDate,
            status: 'already_processed'
        };
        console.log(logObj);
        return null;
    }
    
    // Calculate block metrics
    let blockFeesWei = BigInt(0);
    const contractDeployments = new Set();
    
    // Process transactions
    for (const tx of block.transactions) {
        // Track active addresses
        if (tx.from) {
            qnAddListItem(`${PREFIX}addresses_${blockDate}`, tx.from.toLowerCase());
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
    
    // Check receipts for contract deployments
    if (block.receipts) {
        for (const receipt of block.receipts) {
            if (receipt.contractAddress) {
                contractDeployments.add(receipt.contractAddress);
            }
        }
    }
    
    // Store block metrics
    const blockMetricsKey = `${PREFIX}block_metrics_${blockNumber.toString()}`;
    const blockMetrics = {
        timestamp: blockTimestamp,
        transactions: block.transactions.length,
        fees: Number(blockFeesWei) / Number(WEI_PER_ETH),
        contractDeployments: contractDeployments.size
    };
    qnAddSet(blockMetricsKey, JSON.stringify(blockMetrics));
    
    // Mark block as processed
    qnAddListItem(processedBlocksKey, blockNumber.toString());
    qnAddListItem(`${PREFIX}blocks_${blockDate}`, blockNumber.toString());
    qnAddSet(`${PREFIX}block_date_${blockNumber.toString()}`, blockDate);
    
    // Check previous block's date
    const prevBlockNumber = blockNumber - 1;
    const prevBlockDate = qnGetSet(`${PREFIX}block_date_${prevBlockNumber.toString()}`);
    
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
            const dayMetrics = calculateDayMetrics(prevBlockDate, prevDayBlocks, PREFIX);
            
            // Store final metrics
            qnAddSet(`${PREFIX}metrics_${prevBlockDate}`, JSON.stringify(dayMetrics));
            
            // Cleanup temporary data
            qnDeleteList(`${PREFIX}blocks_${prevBlockDate}`);
            qnDeleteList(`${PREFIX}addresses_${prevBlockDate}`);
            
            // Clean up block metrics
            for (const blockNum of prevDayBlocks) {
                qnDeleteSet(`${PREFIX}block_metrics_${blockNum}`);
                qnDeleteSet(`${PREFIX}block_date_${blockNum}`);
            }
            
            const logObj = {
                blockNumber,
                blockDate,
                status: 'day_completed',
                completedDate: prevBlockDate,
                metrics: dayMetrics
            };
            // use this when testing
            // return logObj;
        }
    }
    
    const logObj = {
        blockNumber,
        blockDate,
        status: 'block_processed'
    };
    // use this when testing
    // return logObj;

    return null;
}

function calculateDayMetrics(date, blockNumbers, prefix) {
    let totalTransactions = 0;
    let totalFeesEth = 0;
    let totalContractCreations = 0;
    let firstBlock = null;
    let lastBlock = null;
    let firstBlockTimestamp = null;
    let lastBlockTimestamp = null;
    let successfulBlocks = 0;
    let failedBlocks = [];
    
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
            totalContractCreations += blockMetrics.contractDeployments;
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
        activeAddresses,
        firstBlock,
        lastBlock,
        firstBlockTimestamp,
        lastBlockTimestamp,
        isComplete: true,
        updatedAt: Math.floor(Date.now() / 1000)
    };
    
    logProcessingResults(date, metrics, failedBlocks);
    
    return metrics;
}

function logProcessingResults(date, metrics, failedBlocks) {
    // Format failed blocks for logging
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
    
    console.log({
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
    });
}