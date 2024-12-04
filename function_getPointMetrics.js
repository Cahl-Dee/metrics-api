async function main(params) {
    const {
        chain = 'ETH',    // string            | optional, defaults to ETH
        date,             // YYYY-MM-DD string | optional, defaults to latest day available if no date or block provided
        blockNumber,      // integer           | optional
        metric = 'all'    // string            | specific metric name or 'all', dafaults to all
    } = params.user_data;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    
    try {
        if(date && blockNumber) {
            throw new Error('Only one of date or block number can be provided');
        }
        
        if (date) {
            // Get daily metrics
            const dailyMetricsStr = await qnLib.qnGetSet(`${prefix}metrics_${date}`);
            if (!dailyMetricsStr) {
                throw new Error('No metrics found for specified date');
            }
            
            const metrics = JSON.parse(dailyMetricsStr);
            const secondsInDay = 86400;
            const tps = secondsInDay > 0 ? metrics.totalTransactions / secondsInDay : 0;
            
            return {
                chain: chain.toLowerCase(),
                type: 'day',
                date,
                blockRange: {
                    first: metrics.firstBlock,
                    last: metrics.lastBlock
                },
                metrics: metric && metric !== 'all' ? 
                    { [metric]: metrics[metric] } : 
                    {
                        transactionCount: metrics.totalTransactions,
                        tps,
                        averageTxCost: metrics.averageTxCostEth,
                        totalFees: metrics.totalFees,
                        contractDeploymentCount: metrics.totalContractCreations,
                        activeAddressCount: metrics.activeAddresses,
                    }
            };
        }

        let blockNo = blockNumber;

        // If no block number, default to latest processed block 
        if (!blockNumber) blockNo = await qnLib.qnGetSet(`${prefix}last_processed_block`);

        // Get block metrics
        const blockMetricsStr = await qnLib.qnGetSet(`${prefix}block_metrics_${blockNo}`);
        if (!blockMetricsStr) {
            throw new Error(`No metrics found for block ${blockNumber}`);
        }
        
        const blockMetrics = JSON.parse(blockMetricsStr);
        
        return {
            chain: chain.toLowerCase(),
            type: 'block',
            blockNumber,
            timestamp: blockMetrics.timestamp,
            date: blockMetrics.date,
            metrics: metric && metric !== 'all' ? 
                { [metric]: blockMetrics[metric] } : 
                {
                    transactionCount: blockMetrics.transactions,
                    averageTxCost: blockMetrics.transactions > 0 ? blockMetrics.fees / blockMetrics.transactions : 0,
                    totalFees: blockMetrics.fees,
                    contractDeploymentCount: blockMetrics.contractDeployments
                }
        };
        
    } catch (e) {
        console.error(`Error retrieving metrics: ${e.message}`);
        throw e;
    }
}