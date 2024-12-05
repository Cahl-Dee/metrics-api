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
            const dailyMetricsStr = await qnLib.qnGetSet(`${prefix}daily-metrics_${date}`);
            if (!dailyMetricsStr) {
                throw new Error('No metrics found for specified date');
            }
            
            const metrics = JSON.parse(dailyMetricsStr);
            const secondsInDay = 86400;
            const tps = secondsInDay > 0 ? metrics.metrics.numTransactions / secondsInDay : 0;
            
            return {
                chain: chain.toLowerCase(),
                type: 'day',
                date,
                blockRange: {
                    first: metrics.metadata.firstBlock,
                    last: metrics.metadata.lastBlock
                },
                metrics: metric && metric !== 'all' ? 
                    { [metric]: metrics.metrics[metric] } : 
                    {
                        numTransactions: metrics.metrics.numTransactions,
                        tps,
                        avgTxFee: metrics.metrics.avgTxFeeEth,
                        totalFees: metrics.metrics.totalFeesEth,
                        avgBlockFees: metrics.metrics.avgBlockFeesEth,
                        numContractDeployments: metrics.metrics.numContractDeployments,
                        contractDeploymentCoverage: metrics.metrics.contractDeploymentCoverage,
                        numActiveAddresses: metrics.metrics.numActiveAddresses,
                        avgBlockTime: metrics.metrics.avgBlockTime
                    }
            };
        }

        let blockNo = blockNumber;

        // If no block number, default to latest processed block
        if (!blockNumber) {
            blockNo = await qnLib.qnGetSet(`${prefix}last_processed_block`);
        }

        // Get block metrics
        const blockMetricsStr = await qnLib.qnGetSet(`${prefix}block-metrics_${blockNo}`);
        if (!blockMetricsStr) {
            throw new Error(`No metrics found for block ${blockNumber}`);
        }
        
        const blockMetrics = JSON.parse(blockMetricsStr);
        
        return {
            chain: chain.toLowerCase(),
            type: 'block',
            blockNumber: blockNo,
            timestamp: blockMetrics.timestamp,
            date: blockMetrics.date,
            metrics: metric && metric !== 'all' ? 
                { [metric]: blockMetrics[metric] } : 
                {
                    numTransactions: blockMetrics.numTransactions,
                    avgTxFee: blockMetrics.numTransactions > 0 ? blockMetrics.totalFeesEth / blockMetrics.numTransactions : 0,
                    totalFees: blockMetrics.totalFeesEth,
                    numContractDeployments: blockMetrics.numContractDeployments
                }
        };
        
    } catch (e) {
        console.error(`Error retrieving metrics: ${e.message}`);
        throw e;
    }
}