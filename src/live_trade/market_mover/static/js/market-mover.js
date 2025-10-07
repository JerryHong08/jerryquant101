/**
 * Market Mover Real-time Visualization JavaScript
 * Handles WebSocket connections, chart updates, and user interactions
 */

class MarketMoverApp {
    constructor() {
        this.socket = null;
        this.chart = null;
        this.miniChart = null;
        this.chartData = null;
        this.showHighlights = true;
        this.timeRange = 'all';
        this.selectedStock = null;
        
        // Global time clock - the current time reference for all operations
        this.currentTime = null;
        
        this.initializeApp();
    }
    
    initializeApp() {
        this.initializeSocket();
        this.initializeChart();
        this.initializeEventListeners();
        this.initializeModal();
        this.updateConnectionStatus('connecting', 'Connecting to server...');
    }
    
    // WebSocket Connection
    initializeSocket() {
        this.socket = io();
        
        this.socket.on('connect', () => {
            this.updateConnectionStatus('connected', 'Connected');
            this.showToast('Connected to server', 'success');
        });
        
        this.socket.on('disconnect', () => {
            this.updateConnectionStatus('disconnected', 'Disconnected');
            this.showToast('Connection lost', 'error');
        });
        
        this.socket.on('chart_update', (data) => {
            this.handleChartUpdate(data);
        });
        
        this.socket.on('historical_data_loaded', (data) => {
            this.handleHistoricalDataLoaded(data);
        });
        
        this.socket.on('stock_detail_response', (data) => {
            this.handleStockDetailResponse(data);
        });
        
        this.socket.on('error', (data) => {
            this.showToast(data.message, 'error');
        });
    }
    
    // Chart Initialization
    initializeChart() {
        const ctx = document.getElementById('market-chart').getContext('2d');
        
        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: []
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'point',
                    intersect: false
                },
                plugins: {
                    title: {
                        display: true,
                        text: 'Market Movers - Real-time Percent Change',
                        font: {
                            size: 16
                        }
                    },
                    legend: {
                        display: false  // Hide default legend, we'll use custom rankings
                    },
                    tooltip: {
                        callbacks: {
                            title: (context) => {
                                const point = context[0];
                                const dataset = point.dataset;
                                return `${dataset.label} (Rank: ${dataset.rank})`;
                            },
                            label: (context) => {
                                const dataset = context.dataset;
                                const value = context.parsed.y;
                                const velocity = dataset.velocity || 0;
                                
                                let label = `Change: ${value.toFixed(2)}%`;
                                if (velocity !== 0) {
                                    const direction = velocity > 0 ? '↑' : '↓';
                                    label += ` | Rank ${direction}${Math.abs(velocity)}`;
                                }
                                
                                if (dataset.highlight) {
                                    label += ' | 🔥 Hot Mover';
                                }
                                
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'minute',
                            displayFormats: {
                                minute: 'HH:mm',
                                hour: 'HH:mm'
                            },
                            tooltipFormat: 'PPpp'
                        },
                        title: {
                            display: true,
                            text: 'Time (EST)'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Percent Change (%)'
                        },
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        }
                    }
                },
                onClick: (event, elements) => {
                    if (elements.length > 0) {
                        const element = elements[0];
                        const dataset = this.chart.data.datasets[element.datasetIndex];
                        this.requestStockDetail(dataset.label);
                    }
                }
            }
        });
    }
    
    // Event Listeners
    initializeEventListeners() {
        // Historical data loading
        document.getElementById('load-historical-btn').addEventListener('click', () => {
            const dateInput = document.getElementById('historical-date');
            if (dateInput.value) {
                const date = dateInput.value.replace(/-/g, '');
                this.loadHistoricalData(date);
            }
        });
        
        // Time range selection
        document.getElementById('time-range').addEventListener('change', (e) => {
            this.timeRange = e.target.value;
            this.applyTimeRangeFilter();
        });
        
        // Reset zoom
        document.getElementById('reset-zoom-btn').addEventListener('click', () => {
            this.chart.resetZoom();
        });
        
        // Toggle highlights
        document.getElementById('toggle-highlights-btn').addEventListener('click', () => {
            this.showHighlights = !this.showHighlights;
            this.updateHighlights();
        });
    }
    
    // Modal Initialization
    initializeModal() {
        const modal = document.getElementById('stock-detail-modal');
        const closeBtn = modal.querySelector('.close');
        
        closeBtn.addEventListener('click', () => {
            this.closeModal();
        });
        
        window.addEventListener('click', (event) => {
            if (event.target === modal) {
                this.closeModal();
            }
        });
    }
    
    // Data Handlers
    handleChartUpdate(data) {
        console.log('Received chart update:', data);
        console.log('Datasets count:', data.datasets ? data.datasets.length : 0);
        console.log('Timestamps count:', data.timestamps ? data.timestamps.length : 0);
        
        this.chartData = data;
        
        // Update global time clock with the latest timestamp from data
        this.updateGlobalClock(data);
        
        this.updateChart(data);
        this.updateRankings(data);
        this.updateHighlights(data);
        this.updateLastUpdateTime();
    }
    
    // Global Time Clock Management
    updateGlobalClock(data) {
        let latestTime = null;
        
        if (data.datasets && data.datasets.length > 0) {
            // Find the latest timestamp across all datasets
            for (const dataset of data.datasets) {
                if (dataset.data && dataset.data.length > 0) {
                    const datasetLatestTime = new Date(dataset.data[dataset.data.length - 1].x);
                    if (!latestTime || datasetLatestTime > latestTime) {
                        latestTime = datasetLatestTime;
                    }
                }
            }
        }
        
        // Fallback to system time if no data available
        this.currentTime = latestTime || new Date();
        
        console.log('Global clock updated to:', this.currentTime);
    }
    
    getCurrentTime() {
        // Return the current time reference (either from data or system time)
        return this.currentTime || new Date();
    }
    
    // Chart Updates
    updateChart(data) {
        console.log('updateChart called with data:', data);
        
        if (!data.datasets) {
            console.log('No datasets found in data');
            return;
        }
        
        console.log('Raw datasets count:', data.datasets.length);
        
        // Sort datasets by rank
        const sortedDatasets = data.datasets
            .filter(dataset => dataset.data && dataset.data.length > 0)
            .sort((a, b) => a.rank - b.rank)
            .slice(0, 20); // Top 20 only
        
        console.log('Filtered datasets count:', sortedDatasets.length);
        
        if (sortedDatasets.length > 0) {
            console.log('Sample dataset:', sortedDatasets[0]);
        }
        
        // Apply time range filter if needed
        const filteredDatasets = this.applyTimeRangeToDatasets(sortedDatasets);
        
        console.log('Final datasets for chart:', filteredDatasets.length);
        
        this.chart.data.datasets = filteredDatasets;
        this.chart.update('none'); // Smooth update without animation
    }
    
    applyTimeRangeToDatasets(datasets) {
        if (this.timeRange === 'all') {
            return datasets;
        }
        
        // Use global clock instead of system time
        const referenceTime = this.getCurrentTime();
        let cutoffTime;
        
        switch (this.timeRange) {
            case '5m':
                cutoffTime = new Date(referenceTime.getTime() - 5 * 60 * 1000);
                break;
            case '15m':
                cutoffTime = new Date(referenceTime.getTime() - 15 * 60 * 1000);
                break;
            case '30m':
                cutoffTime = new Date(referenceTime.getTime() - 30 * 60 * 1000);
                break;
            case '1h':
                cutoffTime = new Date(referenceTime.getTime() - 60 * 60 * 1000);
                break;
            default:
                return datasets;
        }
        
        console.log(`Time range filter: ${this.timeRange}, Reference time: ${referenceTime}, Cutoff: ${cutoffTime}`);
        
        return datasets.map(dataset => ({
            ...dataset,
            data: dataset.data.filter(point => 
                point && new Date(point.x) >= cutoffTime
            )
        }));
    }
    
    applyTimeRangeFilter() {
        if (this.chartData) {
            this.updateChart(this.chartData);
        }
    }
    
    // Rankings Panel
    updateRankings(data) {
        const rankingsList = document.getElementById('rankings-list');
        rankingsList.innerHTML = '';
        
        if (!data.datasets) return;
        
        const sortedStocks = data.datasets
            .sort((a, b) => a.rank - b.rank)
            .slice(0, 20);
        
        sortedStocks.forEach(stock => {
            const item = document.createElement('div');
            item.className = 'ranking-item';
            
            if (stock.highlight) {
                item.classList.add('highlighted');
            }
            
            if (stock.metadata && this.isNewEntrant(stock)) {
                item.classList.add('new-entrant');
            }
            
            const latestChange = this.getLatestValue(
                stock.data && stock.data.length > 0 
                    ? stock.data.map(point => point.y) 
                    : []
            );
            const numericChange = typeof latestChange === 'number' ? latestChange : 0;
            const changeClass = numericChange >= 0 ? 'positive' : 'negative';
            const changeSymbol = numericChange >= 0 ? '+' : '';
            
            let velocityIndicator = '';
            if (stock.velocity > 0) {
                velocityIndicator = ` ↑${stock.velocity}`;
            } else if (stock.velocity < 0) {
                velocityIndicator = ` ↓${Math.abs(stock.velocity)}`;
            }
            
            // Create checkbox for highlight toggle
            const checkboxId = `highlight-${stock.label}`;
            
            item.innerHTML = `
                <div class="ranking-content">
                    <div class="ranking-left">
                        <input type="checkbox" id="${checkboxId}" class="highlight-checkbox" 
                            ${stock.highlight ? 'checked' : ''} 
                            data-ticker="${stock.label}">
                        <label for="${checkboxId}" class="checkbox-label">
                            <span class="ranking-ticker">#${stock.rank} ${stock.label}</span>
                            ${velocityIndicator ? `<small style="color: #ffa500;">${velocityIndicator}</small>` : ''}
                        </label>
                    </div>
                    <div class="ranking-right">
                        <span class="ranking-change ${changeClass}">
                            ${changeSymbol}${numericChange.toFixed(2)}%
                        </span>
                    </div>
                </div>
            `;
            
            // Add click handler for stock detail (excluding checkbox area)
            const rankingContent = item.querySelector('.ranking-right');
            rankingContent.addEventListener('click', () => {
                this.requestStockDetail(stock.label);
            });
            
            // Add change handler for checkbox
            const checkbox = item.querySelector('.highlight-checkbox');
            checkbox.addEventListener('change', (e) => {
                e.stopPropagation(); // Prevent triggering stock detail
                this.toggleStockHighlight(stock.label, e.target.checked);
            });
            
            rankingsList.appendChild(item);
        });
    }

    // Add new method to handle highlight toggling
    toggleStockHighlight(ticker, isHighlighted) {
        console.log(`Toggling highlight for ${ticker}: ${isHighlighted}`);
        
        // Update local chart data immediately for responsive UI
        if (this.chartData && this.chartData.datasets) {
            const dataset = this.chartData.datasets.find(d => d.label === ticker);
            if (dataset) {
                dataset.highlight = isHighlighted;
                dataset.borderWidth = isHighlighted ? 3 : 1;
                
                // Update chart immediately
                this.chart.update('none');
                
                // Update highlights panel
                this.updateHighlights(this.chartData);
            }
        }
        
        // Send update to server
        this.socket.emit('toggle_stock_highlight', {
            ticker: ticker,
            highlight: isHighlighted
        });
    }
    
    // Highlights Panel
    updateHighlights(data) {
        const highlightsList = document.getElementById('highlights-list');
        highlightsList.innerHTML = '';
        
        if (!data.highlights || data.highlights.length === 0) {
            highlightsList.innerHTML = '<div style="opacity: 0.7;">No hot movers detected</div>';
            return;
        }
        
        data.highlights.forEach(highlight => {
            const item = document.createElement('div');
            item.className = 'highlight-item';
            
            let description = '';
            if (highlight.is_new) {
                description = `🆕 New in Top 20 at rank ${highlight.rank}`;
            } else if (highlight.velocity > 0) {
                description = `🚀 Jumped ${highlight.velocity} positions to rank ${highlight.rank}`;
            }
            
            item.innerHTML = `
                <span class="highlight-ticker">${highlight.ticker}</span>
                <div class="highlight-info">${description}</div>
            `;
            
            item.addEventListener('click', () => {
                this.requestStockDetail(highlight.ticker);
            });
            
            highlightsList.appendChild(item);
        });
    }
    
    // Stock Detail Modal
    requestStockDetail(ticker) {
        this.selectedStock = ticker;
        this.socket.emit('request_stock_detail', { ticker: ticker });
    }
    
    showStockDetail(ticker, detail) {
        if (!detail) {
            this.showToast('Stock details not available', 'error');
            return;
        }
        
        // Update modal content
        document.getElementById('modal-stock-ticker').textContent = ticker;
        document.getElementById('detail-current-price').textContent = 
            '$' + (detail.metadata?.current_price || 0).toFixed(2);
        document.getElementById('detail-prev-close').textContent = 
            '$' + (detail.metadata?.prev_close || 0).toFixed(2);
        
        const latestChange = this.getLatestValue(detail.percent_changes || []);
        document.getElementById('detail-percent-change').textContent = 
            (latestChange >= 0 ? '+' : '') + latestChange.toFixed(2) + '%';
        
        document.getElementById('detail-volume').textContent = 
            this.formatNumber(detail.metadata?.volume || 0);
        document.getElementById('detail-current-rank').textContent = 
            '#' + (detail.current_rank || '--');
        
        const rankChange = detail.rank_velocity || 0;
        const rankChangeElement = document.getElementById('detail-rank-change');
        if (rankChange > 0) {
            rankChangeElement.textContent = `↑${rankChange}`;
            rankChangeElement.style.color = '#4CAF50';
        } else if (rankChange < 0) {
            rankChangeElement.textContent = `↓${Math.abs(rankChange)}`;
            rankChangeElement.style.color = '#f44336';
        } else {
            rankChangeElement.textContent = 'No change';
            rankChangeElement.style.color = '#666';
        }
        
        document.getElementById('detail-first-appearance').textContent = 
            detail.first_appearance ? new Date(detail.first_appearance).toLocaleString() : '--';
        
        // Create mini chart
        this.createMiniChart(detail);
        
        // Show modal
        document.getElementById('stock-detail-modal').style.display = 'block';
    }
    
    createMiniChart(detail) {
        const ctx = document.getElementById('detail-mini-chart');
        
        if (this.miniChart) {
            this.miniChart.destroy();
        }
        
        const timestamps = detail.timestamps || [];
        const percentChanges = detail.percent_changes || [];
        
        const chartData = timestamps.map((timestamp, index) => ({
            x: new Date(timestamp),
            y: percentChanges[index]
        }));
        
        this.miniChart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: detail.ticker || 'Stock',
                    data: chartData,
                    borderColor: '#4CAF50',
                    backgroundColor: 'rgba(76, 175, 80, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            displayFormats: {
                                minute: 'HH:mm'
                            }
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Change (%)'
                        }
                    }
                }
            }
        });
    }
    
    closeModal() {
        document.getElementById('stock-detail-modal').style.display = 'none';
        if (this.miniChart) {
            this.miniChart.destroy();
            this.miniChart = null;
        }
    }
    
    // Utility Functions
    loadHistoricalData(date) {
        this.socket.emit('load_historical_data', { date: date });
        this.showToast('Loading historical data...', 'info');
    }
    
    updateConnectionStatus(status, text) {
        const indicator = document.getElementById('connection-indicator');
        const statusText = document.getElementById('connection-text');
        
        indicator.className = `indicator ${status}`;
        statusText.textContent = text;
    }
    
    updateLastUpdateTime() {
        // Use global clock for consistent time display
        const displayTime = this.getCurrentTime();
        document.getElementById('last-update-time').textContent = 
            displayTime.toLocaleTimeString();
    }
    
    getLatestValue(dataArray) {
        if (!dataArray || dataArray.length === 0) return 0;
        return dataArray[dataArray.length - 1];
    }
    
    isNewEntrant(stock) {
        // Use global clock for more accurate new entrant detection
        if (!stock.data || stock.data.length === 0) return false;
        
        const referenceTime = this.getCurrentTime();
        const firstDataTime = new Date(stock.data[0].x);
        
        // Consider it new if first data point is within the last 5 minutes
        const timeDiff = referenceTime.getTime() - firstDataTime.getTime();
        const isNew = timeDiff <= 5 * 60 * 1000; // 5 minutes
        
        // Also check if it has very few data points (original logic)
        const hasFewPoints = stock.data.length <= 3;
        
        return isNew || hasFewPoints;
    }
    
    // Enhanced method to get time range for display purposes
    getTimeRangeDisplay() {
        const referenceTime = this.getCurrentTime();
        let startTime;
        
        switch (this.timeRange) {
            case '5m':
                startTime = new Date(referenceTime.getTime() - 5 * 60 * 1000);
                break;
            case '15m':
                startTime = new Date(referenceTime.getTime() - 15 * 60 * 1000);
                break;
            case '30m':
                startTime = new Date(referenceTime.getTime() - 30 * 60 * 1000);
                break;
            case '1h':
                startTime = new Date(referenceTime.getTime() - 60 * 60 * 1000);
                break;
            default:
                return 'All Data';
        }
        
        return `${startTime.toLocaleTimeString()} - ${referenceTime.toLocaleTimeString()}`;
    }
    
    // Method to manually set the global clock (useful for testing or special scenarios)
    setGlobalClock(time) {
        this.currentTime = new Date(time);
        console.log('Global clock manually set to:', this.currentTime);
        
        // Re-apply time range filter if needed
        if (this.chartData && this.timeRange !== 'all') {
            this.applyTimeRangeFilter();
        }
    }
    
    // Method to reset global clock to system time
    resetGlobalClock() {
        this.currentTime = new Date();
        console.log('Global clock reset to system time:', this.currentTime);
        
        // Re-apply time range filter if needed
        if (this.chartData && this.timeRange !== 'all') {
            this.applyTimeRangeFilter();
        }
    }
    
    // Utility Functions
    formatNumber(num) {
        if (num >= 1e9) {
            return (num / 1e9).toFixed(1) + 'B';
        } else if (num >= 1e6) {
            return (num / 1e6).toFixed(1) + 'M';
        } else if (num >= 1e3) {
            return (num / 1e3).toFixed(1) + 'K';
        }
        return num.toLocaleString();
    }
    
    showToast(message, type = 'info') {
        const toastContainer = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        
        toastContainer.appendChild(toast);
        
        // Auto remove after 5 seconds
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 5000);
    }
}

// Initialize the app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.marketMoverApp = new MarketMoverApp();
});