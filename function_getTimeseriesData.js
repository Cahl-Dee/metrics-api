async function main(params) {
    const {
        days,          // 7, 30, or 90 | optional, defaults to 7
        metric,        // required specific metric name
        chain = 'ETH', // optional, defaults to ETH
        endDate        // optional, defaults to most recent
    } = params;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    
    if(!days){
        days = 7;
    }

    if (![7, 30, 90].includes(days)) {
        throw new Error('Days parameter must be 7, 30, or 90');
    }
    
    if (!metric) {
        throw new Error('Metric name is required for timeseries data');
    }
    
    const end = endDate ? new Date(endDate) : new Date();
    end.setUTCHours(0, 0, 0, 0);
    const start = new Date(end);
    start.setDate(start.getDate() - days);
    
    const dataPoints = [];
    let currentDate = new Date(start);
    
    while (currentDate <= end) {
        const dateStr = currentDate.toISOString().split('T')[0];
        const dayMetricsStr = await qnGetSet(`${prefix}metrics_${dateStr}`);
        
        if (dayMetricsStr) {
            const dayMetrics = JSON.parse(dayMetricsStr);
            dataPoints.push({
                date: dateStr,
                value: extractMetricValue(dayMetrics, metric)
            });
        }
        
        currentDate.setDate(currentDate.getDate() + 1);
    }
    
    return {
        chain: chain.toUpperCase(),
        metric,
        period: `${days}d`,
        start: start.toISOString().split('T')[0],
        end: end.toISOString().split('T')[0],
        dataPoints
    };
}

function extractMetricValue(metrics, metricName) {
    switch(metricName) {
        case 'transactions':
            return metrics.totalTransactions;
        case 'tps': {
            const periodDuration = metrics.lastBlockTimestamp - metrics.firstBlockTimestamp;
            return periodDuration > 0 ? metrics.totalTransactions / periodDuration : 0;
        }
        case 'fees':
            return metrics.totalFees;
        case 'contractCreations':
            return metrics.totalContractCreations;
        case 'activeAddresses':
            return metrics.activeAddresses;
        case 'averageTxCostEth':
            return metrics.averageTxCostEth;
        case 'averageFeesPerBlock':
            return metrics.averageFeesPerBlock;
        case 'successRate':
            return metrics.successfulBlocks / (metrics.successfulBlocks + metrics.failedBlocksCount);
        default:
            throw new Error(`Unknown metric: ${metricName}`);
    }
}