async function main(params) {
    if (!params?.user_data) {
        params.user_data = {};
    }

    const {
        chain = 'ETH',    // string            | optional, defaults to ETH
        date,             // YYYY-MM-DD string | optional, defaults to latest
        metric,           // string            | required specific metric name
        days = 7         // number            | 7, 30, or 90
    } = params.user_data;

    if (![7, 30, 90].includes(days)) {
        throw new Error('Days parameter must be 7, 30, or 90');
    }
    
    if (!metric) {
        throw new Error('Metric name is required for timeseries data');
    }

    const prefix = `MA_${chain.toUpperCase()}_`;
    const dailyMetricsPrefix = `${prefix}daily-metrics_`;
    
    try {
        const endDate = date ? new Date(date) : await getLatestProcessedDate(dailyMetricsPrefix);
        const { dataPoints } = await getTimeseriesData(endDate, days, dailyMetricsPrefix, metric);
        
        return formatResponse(chain, metric, days, dataPoints);
    } catch (e) {
        throw new Error(`Failed to retrieve timeseries data: ${e.message}`);
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

async function getTimeseriesData(endDate, days, prefix, metric) {
    const end = new Date(endDate);
    end.setUTCHours(0, 0, 0, 0);
    const start = new Date(end);
    start.setDate(start.getDate() - days);
    
    const dataPoints = [];
    let currentDate = new Date(start);
    
    while (currentDate <= end) {
        const dateStr = currentDate.toISOString().split('T')[0];
        try {
            const dayMetricsStr = await qnLib.qnGetSet(`${prefix}${dateStr}`);
            
            if (dayMetricsStr) {
                const dayMetrics = JSON.parse(dayMetricsStr);
                dataPoints.push({
                    date: dateStr,
                    value: extractMetricValue(dayMetrics.metrics, metric)
                });
            }
        } catch (e) {
            console.error(`Error processing metrics for date ${dateStr}: ${e.message}`);
        }
        
        currentDate.setDate(currentDate.getDate() + 1);
    }
    
    return { 
        dataPoints,
        start: start.toISOString().split('T')[0],
        end: end.toISOString().split('T')[0]
    };
}

function extractMetricValue(metrics, metricName) {
    if (metricName === 'tps') {
        return metrics.numTransactions / 86400;
    }
    
    if (!metrics[metricName]) {
        throw new Error(`Unknown metric: ${metricName}`);
    }

    return metrics[metricName];
}

function formatResponse(chain, metric, days, dataPoints) {
    return {
        chain: chain.toLowerCase(),
        metric,
        period: `${days}d`,
        dataPoints
    };
}