function main(params) {
    // When debugging, set to true to simulate processing without writing to KV store, then uncomment returns you'll see at key points below
    const simulateOnly = false; 

    // Chain specific configs, adjust as needed
    const chain = 'ETH';
    const decimals = 18;
    const hasDebugTrace = true;

    // Centralized key definitions
    const prefix = `MA_${chain.toUpperCase()}_`;
    const keys = {
        processedBlocks:                 `${prefix}blocks-processed`,
        blockMetrics:   (blockNumber) => `${prefix}block-metrics_${blockNumber}`,
        dailyMetrics:   (date) =>        `${prefix}daily-metrics_${date}`,
        dailyAddresses: (date) =>        `${prefix}daily-active-addresses_${date}`,
        dailyBlocks:    (date) =>        `${prefix}daily-blocks_${date}`,
    };

    // Validate we have expected dataset (only works when stream is live)
    if (params.metadata?.dataset) {
        if (hasDebugTrace && params.metadata.dataset !== 'block_with_receipts_debug_trace') {
            return { error: 'Unexpected dataset, expected block_with_receipts_debug_trace' };
        }
        if (!hasDebugTrace && params.metadata.dataset !== 'block_with_receipts') {
            return { error: 'Unexpected dataset, expected block_with_receipts' };
        }
    }

    // Extract top level data
    const data = params.data ? params.data[0] : params[0];
    const block = data.block;
    const receipts = data.receipts;
    const trace = hasDebugTrace ? data.trace : null;

    const blockNumber = parseInt(block.number, 16);
    const blockTimestamp = parseInt(block.timestamp, 16);
    const blockDate = new Date(blockTimestamp * 1000).toISOString().split('T')[0];

    // Check processed blocks
    const processedBlocks = qnGetList(keys.processedBlocks);
    if (processedBlocks.includes(blockNumber.toString())) {
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
    const numContractDeployments = countContractDeployments(receipts, hasDebugTrace ? trace : null);
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
            const gasUsed = BigInt(receipt.gasUsed);    // Actual gas used from receipt
            const gasPrice = BigInt(tx.gasPrice);
            totalFeesWei += gasUsed * gasPrice;
        } catch (e) {
            console.error(`Error calculating fees for tx ${tx.hash} in block ${blockNumber}: ${e.message}`);
        }
    }
    // Add active addresses in a single upsert
    if(!simulateOnly) {
        qnUpsertList(keys.dailyAddresses(blockDate), { add_items: Array.from(activeAddresses) });
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
        contractDeploymentCoverage: hasDebugTrace ? 'full' : 'partial',
        lastUpdated: new Date().toISOString(),
    };
    // use this when testing - console logging will be added in future versions, for now we need to return the object
    // return blockMetrics;
    
    if(!simulateOnly) {
        // Store block metrics
        qnAddSet(keys.blockMetrics(blockNumber), JSON.stringify(blockMetrics));

        // Mark block as processed
        qnAddListItem(keys.processedBlocks, blockNumber.toString());

        // Add block no to list of blocks on given daily
        qnAddListItem(keys.dailyBlocks(blockDate), blockNumber.toString());
    } 
    
    // Process previous day metrics if needed
    const prevBlockNumber = blockNumber - 1;
    const prevBlockMetricsStr = qnGetSet(keys.blockMetrics(prevBlockNumber));
    let prevBlockDate;
    if (prevBlockMetricsStr) {
        const prevBlockMetrics = JSON.parse(prevBlockMetricsStr);
        prevBlockDate = prevBlockMetrics.date;
    }
    
    // If previous block was in a different day
    if (prevBlockDate && prevBlockDate !== blockDate) {
        const prevDayBlocks = qnGetList(keys.dailyBlocks(prevBlockDate))
            .map(Number)
            .sort((a, b) => a - b);
        
        const isSequenceComplete = prevDayBlocks.length > 0 && 
                                 prevDayBlocks[prevDayBlocks.length - 1] === prevBlockNumber &&
                                 prevDayBlocks.every((block, index) => 
                                     index === 0 || block === prevDayBlocks[index - 1] + 1);
        
        if (isSequenceComplete) {
            const dailyMetrics = calculateDayMetrics(prevBlockDate, prevDayBlocks, keys, hasDebugTrace);
            dailyMetrics.lastUpdated = new Date().toISOString();
            
            if(!simulateOnly) {
                // Store day metrics
                qnAddSet(keys.dailyMetrics(prevBlockDate), JSON.stringify(dailyMetrics));
                
                // Cleanup temporary lists
                qnDeleteList(keys.dailyBlocks(prevBlockDate));
                qnDeleteList(keys.dailyAddresses(prevBlockDate));
                
                // Clean up temporary block metric sets
                const blockMetricsToDelete = prevDayBlocks.map(num => keys.blockMetrics(num));
                qnBulkSets({ delete_sets: blockMetricsToDelete });
            }
        }

        // use this when testing - console logging will be added in future versions, for now we need to return the object
        // const logObj = {
        //     blockNumber,
        //     prevBlockDate,
        //     status: 'day_processed',
        //     daymetrics: dailyMetrics ? dailyMetrics.metrics : null
        // };
        // return logObj;
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

function calculateDayMetrics(date, blockNumbers, keys, hasDebugTrace) {
    let numTransactions = 0;
    let totalFees = 0;
    let numContractDeployments = 0;
    let firstBlock = null;
    let lastBlock = null;
    let firstBlockTimestamp = null;
    let lastBlockTimestamp = null;
    let numProcessedBlocks = 0;
    let failedBlocks = [];
    
    // Process blocks
    for (const blockNumber of blockNumbers) {
        try {
            const blockMetricsStr = qnGetSet(keys.blockMetrics(blockNumber));
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
            
            numTransactions += blockMetrics.numTransactions;
            totalFees += blockMetrics.totalFees;
            numContractDeployments += blockMetrics.numContractDeployments;
            numProcessedBlocks++;
            
        } catch (e) {
            failedBlocks.push({ block: blockNumber, reason: e.message });
        }
    }
    
    // Get active addresses
    let numActiveAddresses = 0;
    try {
        numActiveAddresses = qnGetList(keys.dailyAddresses(date)).length;
    } catch (e) {
        console.error(`Failed to get active addresses for date ${date}: ${e.message}`);
    }
    
    // Calculate averages
    const avgTxFee = numTransactions > 0 ? totalFees / numTransactions : 0;
    const avgBlockFees = numProcessedBlocks > 0 ? totalFees / numProcessedBlocks : 0;
    const avgBlockTime = (lastBlockTimestamp - firstBlockTimestamp) / (lastBlock - firstBlock);
    
    return {
        metrics: {
            numTransactions,
            avgTxFee,
            totalFees,
            avgBlockFees,
            numContractDeployments,
            contractDeploymentCoverage: hasDebugTrace ? 'full' : 'partial',
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
            //blocks: blockNumbers,
            failedBlocks,
            isComplete: true,
            lastUpdated: new Date().toISOString()
        }   
    };
}

function countContractDeployments(receipts, traces) {
    let numDeployments = 0;

    if (traces) {
        const processTrace = (trace) => {
            const item = trace.result ? trace.result : trace;
            if (item.type && ['CREATE', 'CREATE2'].includes(item.type)) {
                numDeployments++;
            }
            if (item.calls) {
                item.calls.forEach(processTrace);
            }
        };
        traces.forEach(processTrace);
    } else if (receipts) {
        // If no trace available, count receipts with contract addresses
        numDeployments = receipts.filter(receipt => receipt.contractAddress).length;
    }

    return numDeployments;
}