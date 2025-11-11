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
        this.isLogScale = true;
        
        // Global time clock - the current time reference for all operations
        this.currentTime = null;
        
        // Notification system
        this.previousHighlights = new Set(); // Track previous highlights
        this.notificationSound = null;

        this.initializeApp();
    }
    
    initializeApp() {
        this.initializeSocket();
        this.initializeChart();
        this.initializeEventListeners();
        this.initializeModal();
        this.initializeNotifications();
        this.updateConnectionStatus('connecting', 'Connecting to server...');
    }
    
    // Initialize notification system
    initializeNotifications() {
        // Request notification permission
        this.requestNotificationPermission();
        
        // Create audio element for notification sound
        this.notificationSound = new Audio();
        this.notificationSound.preload = 'auto';
        
        // You can use a data URL for a simple beep sound or add an actual sound file
        // For now, using a simple data URL for a beep sound
        this.notificationSound.src = 'data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdJivrJBhNjVgodDbq2EcBj+a2/LDciUFLIHO8tiJNwgZaLvt559NEAxQp+PwtmMcBjiR1/LMeSwFJHfH8N2QQAoUXrTp66hVFApGn+DyvmYeBzKS1fDNeSsFJHfH8N2QQAoUXrTp66hVFApGn+DyvmYeBzKS1fDNeSsFJHfH8N2QQAoUXrTp66hVFApGn+DyvmYeB...';
        
        // Alternatively, you can use Web Audio API for a more controlled sound
        this.initializeAudioContext();
    }

    // Request notification permission
    async requestNotificationPermission() {
        if ('Notification' in window) {
            if (Notification.permission === 'default') {
                const permission = await Notification.requestPermission();
                if (permission === 'granted') {
                    this.showToast('Notifications enabled for new highlights', 'success');
                } else if (permission === 'denied') {
                    this.showToast('Notifications disabled. You can enable them in browser settings.', 'info');
                }
            }
        } else {
            console.log('This browser does not support notifications');
        }
    }

    // Initialize Web Audio API for notification sound
    initializeAudioContext() {
        try {
            this.audioContext = new (window.AudioContext || window.webkitAudioContext)();
        } catch (e) {
            console.log('Web Audio API not supported');
        }
    }

    // Play notification sound using Web Audio API
    playNotificationSound() {
        if (this.audioContext) {
            // Create a simple beep sound
            const oscillator = this.audioContext.createOscillator();
            const gainNode = this.audioContext.createGain();
            
            oscillator.connect(gainNode);
            gainNode.connect(this.audioContext.destination);
            
            oscillator.frequency.setValueAtTime(800, this.audioContext.currentTime); // 800 Hz
            oscillator.type = 'sine';
            
            gainNode.gain.setValueAtTime(0, this.audioContext.currentTime);
            gainNode.gain.linearRampToValueAtTime(0.3, this.audioContext.currentTime + 0.01);
            gainNode.gain.exponentialRampToValueAtTime(0.001, this.audioContext.currentTime + 0.3);
            
            oscillator.start(this.audioContext.currentTime);
            oscillator.stop(this.audioContext.currentTime + 0.3);
        } else {
            // Fallback to HTML5 audio
            this.notificationSound.currentTime = 0;
            this.notificationSound.play().catch(e => {
                console.log('Could not play notification sound:', e);
            });
        }
    }

    // Show browser notification for new highlight
    showHighlightNotification(stock) {
        if ('Notification' in window && Notification.permission === 'granted') {
            const title = `New Hot Mover: ${stock.label}`;
            const latestChange = this.getLatestValue(
                stock.data && stock.data.length > 0 
                    ? stock.data.map(point => point.y) 
                    : []
            );
            const changeText = latestChange >= 0 ? `+${latestChange.toFixed(2)}%` : `${latestChange.toFixed(2)}%`;
            
            const options = {
                body: `Rank #${stock.rank} - ${changeText}`,
                icon: '/static/icons/notification-icon.png', // Add your notification icon
                badge: '/static/icons/badge-icon.png', // Add your badge icon
                tag: `highlight-${stock.label}`, // Prevent duplicate notifications
                requireInteraction: false,
                silent: false
            };

            const notification = new Notification(title, options);

            // Auto close after 5 seconds
            setTimeout(() => {
                notification.close();
            }, 5000);

            // Handle notification click
            notification.onclick = () => {
                window.focus();
                this.requestStockDetail(stock.label);
                notification.close();
            };
        }
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
                    y: this.getYAxisConfig() // 使用动态Y轴配置
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

    // 新方法：获取Y轴配置
    getYAxisConfig() {
        const baseConfig = {
            title: {
                display: true,
                text: 'Percent Change (%)'
            },
            grid: {
                color: 'rgba(0, 0, 0, 0.1)'
            }
        };

        // 计算当前数据的最小值和最大值
        const { minValue, maxValue } = this.getDataRange();

        if (this.isLogScale) {
            // 对数模式下，确保最小值大于0
            const logMinValue = minValue <= 0 ? 0.01 : Math.max(minValue * 0.9, 0.01);
            const logMaxValue = Math.max(maxValue * 1.1, logMinValue * 2);

            return {
                ...baseConfig,
                type: 'logarithmic',
                min: logMinValue,
                max: logMaxValue,
                ticks: {
                    callback: function(value, index, values) {
                        // 自定义刻度标签格式
                        if (value >= 1) {
                            return value.toFixed(1) + '%';
                        } else {
                            return value.toFixed(2) + '%';
                        }
                    }
                }
            };
        } else {
            // 线性模式下，设置合理的边距
            const margin = (maxValue - minValue) * 0.1 || 1; // 10%边距，最小1%
            const linearMinValue = minValue - margin;
            const linearMaxValue = maxValue + margin;

            return {
                ...baseConfig,
                min: linearMinValue,
                max: linearMaxValue
            };
        }
    }

    // get current data range
    getDataRange() {
        let minValue = 0;
        let maxValue = 0;
        let hasData = false;

        if (this.chart && this.chart.data.datasets) {
            this.chart.data.datasets.forEach(dataset => {
                // only consider visible datasets with data
                if (!dataset.hidden && dataset.data && dataset.data.length > 0) {
                    dataset.data.forEach(point => {
                        if (point && typeof point.y === 'number') {
                            if (!hasData) {
                                minValue = point.y;
                                maxValue = point.y;
                                hasData = true;
                            } else {
                                minValue = Math.min(minValue, point.y);
                                maxValue = Math.max(maxValue, point.y);
                            }
                        }
                    });
                }
            });
        }

        if (!hasData) {
            return { minValue: -5, maxValue: 5 };
        }

        return { minValue, maxValue };
    }
    
    toggleLogScale() {
        this.isLogScale = !this.isLogScale;
        
        const button = document.getElementById('log-scale-toggle');
        button.textContent = this.isLogScale ? 'Log' : 'Linear';
        button.classList.toggle('active', this.isLogScale);
        
        // need transform data in log scale mode
        if (this.isLogScale) {
            this.transformDataForLogScale();
        } else {
            this.restoreOriginalData();
        }
        
        this.chart.options.scales.y = this.getYAxisConfig();
        
        this.chart.update();
        
        this.showToast(
            `Switched to ${this.isLogScale ? 'logarithmic' : 'linear'} scale`, 
            'info'
        );
    }

    transformDataForLogScale() {
        if (!this.chart.data.datasets) return;
        
        this.chart.data.datasets.forEach(dataset => {
            if (dataset.data && dataset.originalData) {
                // if there is already originalData, use it
                dataset.data = dataset.originalData.map(point => ({
                    x: point.x,
                    y: this.convertToLogSafeValue(point.y)
                }));
            } else if (dataset.data) {
                // if not set originalData
                dataset.originalData = [...dataset.data];
                dataset.data = dataset.data.map(point => ({
                    x: point.x,
                    y: this.convertToLogSafeValue(point.y)
                }));
            }
        });
    }

    restoreOriginalData() {
        if (!this.chart.data.datasets) return;
        
        this.chart.data.datasets.forEach(dataset => {
            if (dataset.originalData) {
                dataset.data = [...dataset.originalData];
            }
        });
    }
    
    convertToLogSafeValue(value) {
        if (value <= 0) {
            return Math.max(Math.abs(value), 0.01);
        }
        return Math.max(value, 0.01);
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

        // Log scale toggle
        document.getElementById('log-scale-toggle').addEventListener('click', () => {
            this.toggleLogScale();
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
        
        // Store current visibility states from both chart and chartData before updating
        const currentVisibilityStates = {};
        
        // First check chart datasets
        if (this.chart.data.datasets) {
            this.chart.data.datasets.forEach(dataset => {
                currentVisibilityStates[dataset.label] = dataset.hidden;
            });
        }
        
        // Then check existing chartData as fallback
        if (this.chartData && this.chartData.datasets) {
            this.chartData.datasets.forEach(dataset => {
                if (!currentVisibilityStates.hasOwnProperty(dataset.label)) {
                    currentVisibilityStates[dataset.label] = dataset.hidden;
                }
            });
        }
        
        // Apply visibility states to incoming data BEFORE storing it
        if (data.datasets) {
            data.datasets.forEach(dataset => {
                if (currentVisibilityStates.hasOwnProperty(dataset.label)) {
                    dataset.hidden = currentVisibilityStates[dataset.label];
                }
            });
        }
        
        // this.chartData = data;
        this.chartData = JSON.parse(JSON.stringify(data)); // Deep copy to preserve original data
        
        // Update global time clock with the latest timestamp from data
        this.updateGlobalClock(data);
        
        this.updateChart(data);
        this.updateRankings(data);
        this.updateHighlights(data);
        this.updateLastUpdateTime();
    }

    handleHistoricalDataLoaded(data) {
        console.log('Historical data loaded:', data);
        this.showToast('Historical data loaded successfully', 'success');
        this.handleChartUpdate(data);
    }

    handleStockDetailResponse(data) {
        console.log('Received stock detail response:', data);
        if (data.detail) {
            this.showStockDetail(data.ticker, data.detail);
        } else {
            this.showToast(`No details available for ${data.ticker}`, 'error');
        }
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
        
        // Store current visibility states before updating
        const currentVisibilityStates = {};
        if (this.chart.data.datasets) {
            this.chart.data.datasets.forEach(dataset => {
                currentVisibilityStates[dataset.label] = dataset.hidden;
            });
        }
        
        // Sort datasets by rank
        const sortedDatasets = data.datasets
            .filter(dataset => dataset.data && dataset.data.length > 0)
            .sort((a, b) => a.rank - b.rank)
            .slice(0, 20); // Top 20 only
        
        console.log('Filtered datasets count:', sortedDatasets.length);
        
        if (sortedDatasets.length > 0) {
            console.log('Sample dataset:', sortedDatasets[0]);
        }
        
        // Apply time range filter if needed (this now preserves hidden state internally)
        const filteredDatasets = this.applyTimeRangeToDatasets(sortedDatasets);
        
        // Restore visibility states from previous chart state
        filteredDatasets.forEach(dataset => {
            if (currentVisibilityStates.hasOwnProperty(dataset.label)) {
                dataset.hidden = currentVisibilityStates[dataset.label];
            }
        });
        
        console.log('Final datasets for chart:', filteredDatasets.length);
        
        this.chart.data.datasets = filteredDatasets;
        
        if (this.isLogScale) {
            this.transformDataForLogScale();
        }
        
        this.chart.options.scales.y = this.getYAxisConfig();
        
        this.chart.update('none'); // Smooth update without animation
        
        // Sync checkbox states with chart visibility after update
        this.syncCheckboxStates();
    }
    
    applyTimeRangeToDatasets(datasets) {
        if (this.timeRange === 'all') {
            // Return a deep copy to avoid modifying original data
            return datasets.map(dataset => ({
                ...dataset,
                data: [...dataset.data] // Ensure we don't modify original data array
            }));
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
                return datasets.map(dataset => ({
                    ...dataset,
                    data: [...dataset.data]
                }));
        }
        
        console.log(`Time range filter: ${this.timeRange}, Reference time: ${referenceTime}, Cutoff: ${cutoffTime}`);
        
        return datasets.map(dataset => {
            // Preserve existing hidden state during time range filtering
            const existingDataset = this.chart.data.datasets ? 
                this.chart.data.datasets.find(d => d.label === dataset.label) : null;
            
            return {
                ...dataset,
                data: dataset.data.filter(point => 
                    point && new Date(point.x) >= cutoffTime
                ),
                // Preserve hidden state if it exists
                hidden: existingDataset ? existingDataset.hidden : dataset.hidden
            };
        });
    }
    
    applyTimeRangeFilter() {
        if (this.chartData) {
            // Store current visibility states before updating
            const currentVisibilityStates = {};
            if (this.chart.data.datasets) {
                this.chart.data.datasets.forEach(dataset => {
                    currentVisibilityStates[dataset.label] = dataset.hidden;
                });
            }
            
            // Always use the original complete data for filtering
            const originalData = {
                ...this.chartData,
                datasets: this.chartData.datasets.map(dataset => ({
                    ...dataset,
                    data: [...dataset.data] // Ensure we work with a copy
                }))
            };
            this.updateChart(originalData);
            
            // Ensure visibility states are preserved after filter
            if (this.chart.data.datasets) {
                this.chart.data.datasets.forEach(dataset => {
                    if (currentVisibilityStates.hasOwnProperty(dataset.label)) {
                        dataset.hidden = currentVisibilityStates[dataset.label];
                    }
                });
                this.chart.update('none');
                
                // Sync checkbox states with chart visibility
                this.syncCheckboxStates();
            }
        }
    }

    // New method to sync checkbox states with chart visibility
    syncCheckboxStates() {
        if (this.chart.data.datasets) {
            const isInSolo = this.isInSoloMode();
            
            this.chart.data.datasets.forEach(dataset => {
                const checkbox = document.querySelector(`[data-ticker="${dataset.label}"]`);
                if (checkbox) {
                    checkbox.checked = !dataset.hidden;
                    
                    // Update ranking item styling based on visibility and solo mode
                    const rankingItem = checkbox.closest('.ranking-item');
                    if (isInSolo) {
                        if (!dataset.hidden) {
                            rankingItem.classList.add('solo-mode');
                            rankingItem.classList.remove('solo-hidden');
                        } else {
                            rankingItem.classList.add('solo-hidden');
                            rankingItem.classList.remove('solo-mode');
                        }
                    } else {
                        rankingItem.classList.remove('solo-mode', 'solo-hidden');
                    }
                }
            });
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
            
            // Create checkbox for chart visibility toggle
            const checkboxId = `visibility-${stock.label}`;
            
        // Format volume and price
        const volume = this.formatNumber(stock.metadata?.volume || 0);
        const currentPrice = stock.metadata?.current_price || 0;
        const formattedPrice = currentPrice >= 1 ? `$${currentPrice.toFixed(2)}` : `$${currentPrice.toFixed(4)}`;
        
        item.innerHTML = `
            <div class="ranking-content">
                <div class="ranking-checkbox">
                    <input type="checkbox" id="${checkboxId}" class="visibility-checkbox" 
                           ${stock.hidden !== true ? 'checked' : ''} 
                           data-ticker="${stock.label}">
                </div>
                <div class="ranking-ticker">
                    <span>#${stock.rank} ${stock.label}</span>
                    ${velocityIndicator ? `<small style="color: #ffa500;">${velocityIndicator}</small>` : ''}
                </div>
                <div class="ranking-change ${changeClass}">
                    ${changeSymbol}${numericChange.toFixed(2)}%
                </div>
                <div class="ranking-volume">
                    ${volume}
                </div>
                <div class="ranking-price">
                    ${formattedPrice}
                </div>
                <div class="ranking-detail">
                    📊
                </div>
            </div>
        `;
        
        // Add click handler for stock detail - ranking-detail
        const rankingDetail = item.querySelector('.ranking-detail');
        rankingDetail.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent triggering highlight toggle
            this.requestStockDetail(stock.label);
        });

        // Add click handler for highlight toggle (excluding checkbox and ranking-detail areas)
        const rankingContent = item.querySelector('.ranking-content');
        rankingContent.addEventListener('click', (e) => {
            // Only toggle highlight if not clicking on checkbox or ranking-detail
            if (!e.target.closest('.visibility-checkbox') && 
                !e.target.closest('.ranking-detail')) {
                this.toggleStockHighlight(stock.label, !stock.highlight);
            }
        });
        
        // Add click handler for checkbox - controls chart visibility
        const checkbox = item.querySelector('.visibility-checkbox'); 
        checkbox.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent triggering highlight toggle
            
            const chartDataset = this.chart.data.datasets ? 
                this.chart.data.datasets.find(d => d.label === stock.label) : null;
            const currentlyVisible = chartDataset ? !chartDataset.hidden : true;
            
            // Check if Ctrl key is also pressed for solo mode
            if (e.ctrlKey || e.metaKey) {
                e.preventDefault(); // Prevent default checkbox behavior
                this.toggleSoloMode(stock.label, currentlyVisible);
                return;
            } else {
                // Always sync checkbox with intended action
                e.target.checked = currentlyVisible; // Set to intended state first
                this.toggleStockVisibility(stock.label, currentlyVisible);
            }
        });
        
        rankingsList.appendChild(item);
        });
    }

    // New method to handle chart visibility toggling
    toggleStockVisibility(ticker, currentlyVisible) {
        const newVisibility = !currentlyVisible;
        console.log(`Toggling visibility for ${ticker}: ${currentlyVisible} -> ${newVisibility}`);
        
        const isInSoloMode = this.isInSoloMode();
        
        if (isInSoloMode && newVisibility) {
            console.log(`Adding ${ticker} to solo mode selection`);
            
            if (this.chart.data.datasets) {
                const chartDataset = this.chart.data.datasets.find(d => d.label === ticker);
                if (chartDataset) {
                    chartDataset.hidden = false;
                }
            }
            
            if (this.chartData && this.chartData.datasets) {
                const dataset = this.chartData.datasets.find(d => d.label === ticker);
                if (dataset) {
                    dataset.hidden = false;
                }
            }
            
            const checkbox = document.querySelector(`[data-ticker="${ticker}"]`);
            if (checkbox) {
                checkbox.checked = true;
                const rankingItem = checkbox.closest('.ranking-item');
                rankingItem.classList.add('solo-mode');
                rankingItem.classList.remove('solo-hidden');
            }
            
            this.chart.options.scales.y = this.getYAxisConfig();
            this.chart.update('none');
            this.showToast(`Added ${ticker} to solo view`, 'info');
            
        } else if (isInSoloMode && !newVisibility) {
            console.log(`Removing ${ticker} from solo mode selection`);
            
            if (this.chart.data.datasets) {
                const chartDataset = this.chart.data.datasets.find(d => d.label === ticker);
                if (chartDataset) {
                    chartDataset.hidden = true;
                }
            }
            
            if (this.chartData && this.chartData.datasets) {
                const dataset = this.chartData.datasets.find(d => d.label === ticker);
                if (dataset) {
                    dataset.hidden = true;
                }
            }
            
            const checkbox = document.querySelector(`[data-ticker="${ticker}"]`);
            if (checkbox) {
                checkbox.checked = false;
                const rankingItem = checkbox.closest('.ranking-item');
                rankingItem.classList.remove('solo-mode');
                rankingItem.classList.add('solo-hidden');
            }
            
            this.chart.options.scales.y = this.getYAxisConfig();
            this.chart.update('none');
            this.showToast(`Removed ${ticker} from solo view`, 'info');
            
        } else {
            // Normal mode - standard visibility toggle
            // Update local chart data immediately for responsive UI
            if (this.chartData && this.chartData.datasets) {
                const dataset = this.chartData.datasets.find(d => d.label === ticker);
                if (dataset) {
                    dataset.hidden = !newVisibility;
                }
            }
            
            if (this.chart.data.datasets) {
                const chartDataset = this.chart.data.datasets.find(d => d.label === ticker);
                if (chartDataset) {
                    chartDataset.hidden = !newVisibility;
                }
            }
            
            const checkbox = document.querySelector(`[data-ticker="${ticker}"]`);
            if (checkbox) {
                checkbox.checked = newVisibility;
            }
            
            this.chart.options.scales.y = this.getYAxisConfig();
            this.chart.update('none');
        }
    }

    isInSoloMode() {
        if (!this.chart.data.datasets || this.chart.data.datasets.length === 0) {
            return false;
        }
        
        const soloModeItems = document.querySelectorAll('.ranking-item.solo-mode');
        return soloModeItems.length > 0;
    }

    // New method to handle solo mode (Ctrl+click)
    toggleSoloMode(ticker, shouldActivateSolo) {
        console.log(`Toggling solo mode for ${ticker}: ${shouldActivateSolo}`);
        
        if (this.chartData && this.chartData.datasets) {
            const isCurrentlyInSoloMode = this.isInSoloMode();
            
            if (!isCurrentlyInSoloMode && shouldActivateSolo) {
                // Activate solo mode: hide all except this ticker
                this.chartData.datasets.forEach(dataset => {
                    dataset.hidden = dataset.label !== ticker;
                });
                
                // Update chart datasets as well
                if (this.chart.data.datasets) {
                    this.chart.data.datasets.forEach(dataset => {
                        dataset.hidden = dataset.label !== ticker;
                    });
                }
                
                // Update all checkboxes to reflect state and add solo mode styling
                document.querySelectorAll('.visibility-checkbox').forEach(checkbox => {
                    const checkboxTicker = checkbox.getAttribute('data-ticker');
                    checkbox.checked = checkboxTicker === ticker;
                    
                    // Add visual indicator for solo mode
                    const rankingItem = checkbox.closest('.ranking-item');
                    if (checkboxTicker === ticker) {
                        rankingItem.classList.add('solo-mode');
                        rankingItem.classList.remove('solo-hidden');
                    } else {
                        rankingItem.classList.add('solo-hidden');
                        rankingItem.classList.remove('solo-mode');
                    }
                });
                
                console.log(`Solo mode activated for ${ticker}`);
                this.showToast(`Solo mode: ${ticker}`, 'info');
                
            } else if (isCurrentlyInSoloMode) {
                // Check if we're removing the last visible item or activating on an already solo item
                const checkbox = document.querySelector(`[data-ticker="${ticker}"]`);
                const rankingItem = checkbox ? checkbox.closest('.ranking-item') : null;
                
                if (rankingItem && rankingItem.classList.contains('solo-mode') && 
                    document.querySelectorAll('.ranking-item.solo-mode').length === 1) {
                    // This is the last solo item - exit solo mode completely
                    this.exitSoloMode();
                } else if (rankingItem && !rankingItem.classList.contains('solo-mode')) {
                    // Adding another item to solo mode
                    rankingItem.classList.add('solo-mode');
                    rankingItem.classList.remove('solo-hidden');
                    checkbox.checked = true;
                    
                    // Update chart
                    if (this.chart.data.datasets) {
                        const chartDataset = this.chart.data.datasets.find(d => d.label === ticker);
                        if (chartDataset) {
                            chartDataset.hidden = false;
                        }
                    }
                    
                    this.showToast(`Added ${ticker} to solo view`, 'info');
                }
            }
            
            // Update chart immediately
            this.chart.update('none');
        }
    }

    // Helper method to completely exit solo mode
    exitSoloMode() {
        console.log('Exiting solo mode completely');
        
        // Show all stocks
        if (this.chartData && this.chartData.datasets) {
            this.chartData.datasets.forEach(dataset => {
                dataset.hidden = false;
            });
        }
        
        if (this.chart.data.datasets) {
            this.chart.data.datasets.forEach(dataset => {
                dataset.hidden = false;
            });
        }
        
        // Update all checkboxes and remove solo mode styling
        document.querySelectorAll('.visibility-checkbox').forEach(checkbox => {
            checkbox.checked = true;
            const rankingItem = checkbox.closest('.ranking-item');
            rankingItem.classList.remove('solo-mode', 'solo-hidden');
        });
        
        this.showToast('Solo mode deactivated', 'info');
    }

    // Modified method to handle highlight toggling
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
        
        // Send highlight update to server
        this.socket.emit('toggle_stock_highlight', {
            ticker: ticker,
            highlight: isHighlighted
        });
    }
    
    // Highlights Panel
    updateHighlights(data) {
        const highlightsList = document.getElementById('highlights-list');
        highlightsList.innerHTML = '';
        
        if (!data.datasets) return;
        
        const highlightedStocks = data.datasets
            .filter(stock => stock.highlight)
            .sort((a, b) => a.rank - b.rank);
        
        // Detect new highlights
        const currentHighlights = new Set(highlightedStocks.map(stock => stock.label));
        const newHighlights = [];
        
        currentHighlights.forEach(ticker => {
            if (!this.previousHighlights.has(ticker)) {
                const stock = highlightedStocks.find(s => s.label === ticker);
                if (stock) {
                    newHighlights.push(stock);
                }
            }
        })
        
        // Update previous highlights set
        this.previousHighlights = new Set(currentHighlights);
        
        // Show notifications for new highlights
        newHighlights.forEach(stock => {
            this.showHighlightNotification(stock);
            this.playNotificationSound();

            // Also show toast notification as fallback
            const latestChange = this.getLatestValue(
                stock.data && stock.data.length > 0
                    ? stock.data.map(point => point.y)
                    : []
            );
            const changeText = latestChange >= 0 ? `+${latestChange.toFixed(2)}%` : `${latestChange.toFixed(2)}%`;
            this.showToast(`🔥 New Hot Mover: ${stock.label} (${changeText})`, 'highlight');
        });

        
        if (highlightedStocks.length === 0) {
            highlightsList.innerHTML = '<p style="text-align: center; opacity: 0.7; padding: 20px;">No highlighted stocks</p>';
            return;
        }
        
        highlightedStocks.forEach(stock => {
            const item = document.createElement('div');
            item.className = 'highlight-item'; // 确保使用正确的CSS类名
            
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
            
            // 确保HTML结构与CSS匹配
            item.innerHTML = `
                <div class="highlight-content">
                    <div class="highlight-left">
                        <span class="highlight-ticker">#${stock.rank} ${stock.label}</span>
                        ${velocityIndicator ? `<small style="color: #ffa500;">${velocityIndicator}</small>` : ''}
                    </div>
                    <div class="highlight-right">
                        <span class="highlight-change ${changeClass}">
                            ${changeSymbol}${numericChange.toFixed(2)}%
                        </span>
                    </div>
                </div>
            `;
            
            // Add click handler for stock detail
            const highlightRight = item.querySelector('.highlight-right');
            highlightRight.addEventListener('click', (e) => {
                e.stopPropagation();
                this.requestStockDetail(stock.label);
            });

            // Add click handler to remove from highlights
            const highlightContent = item.querySelector('.highlight-content');
            highlightContent.addEventListener('click', (e) => {
                if (!e.target.closest('.highlight-right')) {
                    this.toggleStockHighlight(stock.label, false); // 取消highlight
                }
            });
            
            highlightsList.appendChild(item);
        });
    }

    // Enhanced showToast method to support highlight type
    showToast(message, type = 'info') {
        const toastContainer = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        
        // Add special styling for highlight notifications
        if (type === 'highlight') {
            toast.style.background = 'linear-gradient(45deg, #ff6b6b, #ffa500)';
            toast.style.color = 'white';
            toast.style.fontWeight = 'bold';
            toast.style.animation = 'pulse 1s ease-in-out 3';
        }
        
        toastContainer.appendChild(toast);
        
        // Auto remove after 5 seconds (longer for highlight notifications)
        const duration = type === 'highlight' ? 8000 : 5000;
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, duration);
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
        
        // 确保canvas有正确的尺寸
        ctx.style.height = '200px';
        ctx.style.maxHeight = '200px';
        
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
                maintainAspectRatio: false, /* 重要：不保持宽高比 */
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
                },
                // 添加resize事件处理，防止无限增长
                onResize: (chart, size) => {
                    if (size.height > 200) {
                        chart.canvas.style.height = '200px';
                        chart.resize(size.width, 200);
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