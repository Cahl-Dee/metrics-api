async function main(params) {
    if (!params?.user_data) {
        params.user_data = {};
    }

    const {
        chain = 'ETH',    // string            | optional, defaults to ETH
        date,             // YYYY-MM-DD string | optional, defaults to latest day available
        metric = 'all'    // string            | specific metric name or 'all', dafaults to all
    } = params.user_data;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    const dailyMetricsPrefix = `${prefix}daily-metrics_`;
    const dailyMetricsKey = (dateVal) => `${dailyMetricsPrefix}${dateVal}`;
    
    try {
        const dateToRetrieve = date ? date : await getLatestProcessedDate(dailyMetricsPrefix);
        const metrics = await getMetricsForDate(dateToRetrieve, dailyMetricsKey);
        
        return formatResponse(chain, dateToRetrieve, metrics, metric);
    } catch (e) {
        throw new Error(`Failed to retrieve metrics: ${e.message}`);
    }
}

async function getLatestProcessedDate(dailyMetricsPrefix) {
    const sets = await qnLib.qnListAllSets();
    const dates = sets
        .filter(set => set.startsWith(dailyMetricsPrefix))
        .map(set => set.slice(-10))
        .sort();

    if (!dates.length) {
        throw new Error('No metrics data available');
    }

    return dates[dates.length - 1];
}

async function getMetricsForDate(date, dailyMetricsKey) {
    const metricsStr = await qnLib.qnGetSet(dailyMetricsKey(date));
    if (!metricsStr) {
        throw new Error('No metrics found for specified date: ' + date);
    }
    return JSON.parse(metricsStr);
}

function formatResponse(chain, date, metricsData, metric) {
    const tps = metricsData.metrics.numTransactions / 86400;

    return {
        chain: chain.toLowerCase(),
        date,
        blockRange: {
            first: metricsData.metadata.firstBlock,
            last: metricsData.metadata.lastBlock
        },
        metrics: getScopedMetrics(metricsData.metrics, metric, tps)
    };
}

function getScopedMetrics(metrics, metric, tps) {
    if (metric === 'tps') {
        return { tps };
    }
    
    if (metric && metric !== 'all') {
        return { [metric]: metrics[metric] };
    }

    return {
        numTransactions: metrics.numTransactions,
        tps,
        avgTxFee: metrics.avgTxFeeEth,
        totalFees: metrics.totalFeesEth,
        avgBlockFees: metrics.avgBlockFeesEth,
        numContractDeployments: metrics.numContractDeployments,
        contractDeploymentCoverage: metrics.contractDeploymentCoverage,
        numActiveAddresses: metrics.numActiveAddresses,
        avgBlockTime: metrics.avgBlockTime
    };
}