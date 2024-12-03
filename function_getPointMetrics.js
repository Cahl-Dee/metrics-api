async function main(params) {
    const {
        chain = 'ETH',    // optional, defaults to ETH
        date,             // YYYY-MM-DD format | optional
        blockNumber,      // optional, defaults to latest block if no date or block provided
        metric            // specific metric name or 'all'
    } = params;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    
    try {
        let metricsStr;
        let targetDate = date;

        if (!date && !blockNumber) {
            // Get the latest processed block
            const lastProcessedBlock = await qnGetSet(`${prefix}last_processed_block`);
            if (!lastProcessedBlock) {
                throw new Error('No processed blocks found');
            }
            
            // Get the date for this block
            const blockDate = await qnGetSet(`${prefix}block_date_${lastProcessedBlock}`);
            if (!blockDate) {
                throw new Error('Date mapping not found for latest block');
            }
            
            targetDate = blockDate;
        } else if (blockNumber) {
            // Get the date for specified block
            const blockDate = await qnGetSet(`${prefix}block_date_${blockNumber}`);
            if (!blockDate) {
                throw new Error(`Block ${blockNumber} not found`);
            }
            targetDate = blockDate;
        }
        
        metricsStr = await qnGetSet(`${prefix}metrics_${targetDate}`);
        
        if (!metricsStr) {
            throw new Error('No metrics found for specified date/block');
        }
        
        const metrics = JSON.parse(metricsStr);
        
        return {
            chain: chain.toUpperCase(),
            date: targetDate,
            blockRange: {
                first: metrics.firstBlock,
                last: metrics.lastBlock
            },
            metrics: metric && metric !== 'all' ? 
                { [metric]: metrics[metric] } : 
                {
                    totalTransactions: metrics.totalTransactions,
                    totalFees: metrics.totalFees,
                    averageTxCostEth: metrics.averageTxCostEth,
                    totalContractCreations: metrics.totalContractCreations,
                    activeAddresses: metrics.activeAddresses,
                }
        };
    } catch (e) {
        console.error(`Error retrieving point metrics: ${e.message}`);
        throw e;
    }
}