async function main(params) {
    const {
        chain = 'ETH',    // optional, defaults to ETH
        date,             // YYYY-MM-DD format | optional
        blockNumber,      // optional, defaults to latest block if no date or block provided
        metric            // specific metric name or 'all'
    } = params;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    
    try {
        if(!date && !blockNumber) {
            throw new Error('Either date or block number must be provided');
        }

        if(date && blockNumber) {
            throw new Error('Only one of date or block number can be provided');
        }

        // If block number specified, get block metrics
        if (blockNumber) {
            const blockMetricsStr = await qnGetSet(`${prefix}block_metrics_${blockNumber}`);
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
        }
        
        // Get daily metrics
        const dailyMetricsStr = await qnGetSet(`${prefix}metrics_${date}`);
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
        
    } catch (e) {
        console.error(`Error retrieving metrics: ${e.message}`);
        throw e;
    }
}