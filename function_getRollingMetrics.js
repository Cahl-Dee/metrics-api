async function getRollingMetrics(params) {
    const {
        days = 7,          // 7, 30, or 90 | optional, defaults to 7
        metric,        // specific metric name or 'all'
        chain = 'ETH', // optional, defaults to ETH
        date,          // optional specific date, defaults to most recent
    } = params;
    
    const prefix = `MA_${chain.toUpperCase()}_`;
    
    if (![7, 30, 90].includes(days)) {
        throw new Error('Days parameter must be 7, 30, or 90');
    }
    
    // Get target date
    let targetDate;
    if (date) {
        targetDate = date;
    } else {
        // Get the latest processed block
        const lastProcessedBlock = await qnGetSet(`${prefix}last_processed_block`);
        if (!lastProcessedBlock) {
            throw new Error('No processed blocks found');
        }
        
        // Get block metrics to find its date
        const blockMetricsStr = await qnGetSet(`${prefix}block_metrics_${lastProcessedBlock}`);
        if (!blockMetricsStr) {
            throw new Error('Metrics not found for latest block');
        }
        
        const blockMetrics = JSON.parse(blockMetricsStr);
        targetDate = blockMetrics.date;
    }
    
    const end = new Date(targetDate);
    end.setUTCHours(0, 0, 0, 0);
    const start = new Date(end);
    start.setDate(start.getDate() - days);
    
    const prevEnd = new Date(start);
    const prevStart = new Date(prevEnd);
    prevStart.setDate(prevStart.getDate() - days);
    
    try {
        const currentPeriod = await getPeriodMetrics(start, end, prefix);
        const previousPeriod = await getPeriodMetrics(prevStart, prevEnd, prefix);
        
        const results = {
            chain: chain.toLowerCase(),
            period: `${days}d`,
            currentPeriod: {
                start: start.toISOString().split('T')[0],
                end: end.toISOString().split('T')[0]
            },
            previousPeriod: {
                start: prevStart.toISOString().split('T')[0],
                end: prevEnd.toISOString().split('T')[0]
            },
            metrics: {},
            daysWithData: {
                current: currentPeriod.daysWithData,
                previous: previousPeriod.daysWithData
            }
        };
        
        if (metric && metric !== 'all') {
            results.metrics[metric] = {
                current: currentPeriod.averages[metric] || 0,
                previous: previousPeriod.averages[metric] || 0,
                change: calculateChange(
                    currentPeriod.averages[metric],
                    previousPeriod.averages[metric]
                )
            };
        } else {
            for (const [key, value] of Object.entries(currentPeriod.averages)) {
                results.metrics[key] = {
                    current: value,
                    previous: previousPeriod.averages[key] || 0,
                    change: calculateChange(value, previousPeriod.averages[key])
                };
            }
        }
        
        return results;
        
    } catch (e) {
        console.error(`Error retrieving rolling metrics: ${e.message}`);
        throw e;
    }
}

async function getPeriodMetrics(startDate, endDate, prefix) {
    const metrics = {
        totalTransactions: 0,
        totalFees: 0,
        totalContractCreations: 0,
        activeAddresses: 0,
        daysWithData: 0,
        averages: {}
    };
    
    let currentDate = new Date(startDate);
    
    while (currentDate <= endDate) {
        const dateStr = currentDate.toISOString().split('T')[0];
        try {
            const dayMetricsStr = await qnGetSet(`${prefix}metrics_${dateStr}`);
            
            if (dayMetricsStr) {
                const dayMetrics = JSON.parse(dayMetricsStr);
                metrics.totalTransactions += dayMetrics.totalTransactions;
                metrics.totalFees += dayMetrics.totalFees;
                metrics.totalContractCreations += dayMetrics.totalContractCreations;
                metrics.activeAddresses += dayMetrics.activeAddresses;
                metrics.daysWithData++;
            }
        } catch (e) {
            console.error(`Error processing metrics for date ${dateStr}: ${e.message}`);
        }
        
        currentDate.setDate(currentDate.getDate() + 1);
    }

    // Calculate period duration in seconds
    const periodDuration = (endDate - startDate) / 1000;
    
    // Calculate period averages if we have data
    if (metrics.daysWithData > 0) {
        metrics.averages = {
            transactionCount: metrics.totalTransactions / metrics.daysWithData,
            tps: periodDuration > 0 ? metrics.totalTransactions / periodDuration : 0,
            totalFees: metrics.totalFees / metrics.daysWithData,
            contractDeploymentCount: metrics.totalContractCreations / metrics.daysWithData,
            activeAddressCount: metrics.activeAddresses / metrics.daysWithData,
            averageTxCost: metrics.totalTransactions > 0 ? 
                metrics.totalFees / metrics.totalTransactions : 0
        };
    }
    
    return metrics;
}

function calculateChange(current, previous) {
    if (!previous) return current ? 100 : 0;
    return ((current - previous) / previous) * 100;
}