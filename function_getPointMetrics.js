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
        
        // Handle date-based or latest metrics
        let targetDate = date;
        
        if (!date) {
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
        }
        
        // Get daily metrics
        const dailyMetricsStr = await qnGetSet(`${prefix}metrics_${targetDate}`);
        if (!dailyMetricsStr) {
            throw new Error('No metrics found for specified date');
        }
        
        const metrics = JSON.parse(dailyMetricsStr);
        const secondsInDay = 86400;
        const tps = secondsInDay > 0 ? metrics.totalTransactions / secondsInDay : 0;
        
        return {
            chain: chain.toLowerCase(),
            type: 'day',
            date: targetDate,
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