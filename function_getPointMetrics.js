async function main(params) {
    const {
        chain = 'ETH',    // string            | optional, defaults to ETH
        date,             // YYYY-MM-DD string | optional, defaults to latest day available
        metric = 'all'    // string            | specific metric name or 'all', dafaults to all
    } = params.user_data;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    const dailyMetricsPrefix = `${prefix}daily-metrics_`;
    const dailyMetricsKey = (dateVal) => `${dailyMetricsPrefix}${dateVal}`;

    
    try {
        let dateToRetrieve = date;

        if (!date) {
            // Get the latest processed date
            const sets = await qnLib.qnListAllSets();
            const dates = sets
                .filter(set => set.startsWith(dailyMetricsPrefix))
                .map(set => set.slice(-10));
            dates.sort();
            dateToRetrieve = dates[dates.length - 1];
        }

        // Get daily metrics
        const dailyMetricsStr = await qnLib.qnGetSet(dailyMetricsKey(dateToRetrieve));
        if (!dailyMetricsStr) {
            throw new Error('No metrics found for specified date');
        }
        
        const metrics = JSON.parse(dailyMetricsStr);
        const secondsInDay = 86400;
        const tps = secondsInDay > 0 ? metrics.metrics.numTransactions / secondsInDay : 0;
        
        return {
            chain: chain.toLowerCase(),
            date: dateToRetrieve,
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
        
    } catch (e) {
        console.error(`Error retrieving metrics: ${e.message}`);
        throw e;
    }
}