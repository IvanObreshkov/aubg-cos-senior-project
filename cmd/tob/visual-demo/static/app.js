/**
 * TOB Visual Demo - Frontend JavaScript
 * Handles real-time cluster state updates and message broadcasting
 */

/**
 * Formats a timestamp to show hours:minutes:seconds.milliseconds
 * @param {string} timestamp - ISO timestamp string
 * @returns {string} Formatted time string (HH:MM:SS.mmm)
 */
function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    const seconds = date.getSeconds().toString().padStart(2, '0');
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${hours}:${minutes}:${seconds}.${milliseconds}`;
}

/**
 * Fetches and updates the cluster state display
 * Called every second to refresh the UI
 */
function updateCluster() {
    fetch('/api/state')
        .then(res => res.json())
        .then(data => {
            const view = document.getElementById('clusterView');
            view.innerHTML = data.nodes.map(node => renderNodeCard(node)).join('');
        })
        .catch(err => console.error('Error fetching cluster state:', err));
}

/**
 * Renders a single node card with all its information
 * @param {Object} node - Node state object
 * @returns {string} HTML string for the node card
 */
function renderNodeCard(node) {
    const cardClass = node.isSequencer ? 'node-card sequencer' : 'node-card';
    return `
        <div class="${cardClass}">
            ${renderNodeHeader(node)}
            ${renderNodeInfo(node)}
            ${renderMetrics(node)}
            ${renderRecentMessages(node)}
            ${renderRecentEvents(node)}
        </div>
    `;
}

/**
 * Renders the node header with name and badge
 * @param {Object} node - Node state object
 * @returns {string} HTML string for node header
 */
function renderNodeHeader(node) {
    const nameClass = node.isSequencer ? 'node-name sequencer-name' : 'node-name';
    const badgeClass = node.isSequencer ? 'node-badge badge-sequencer' : 'node-badge badge-node';
    const badgeText = node.isSequencer ? '⭐ Sequencer' : 'Node';

    return `
        <div class="node-header">
            <div class="${nameClass}">${node.id}</div>
            <div class="${badgeClass}">${badgeText}</div>
        </div>
    `;
}

/**
 * Renders node information rows
 * @param {Object} node - Node state object
 * @returns {string} HTML string for node info
 */
function renderNodeInfo(node) {
    let html = `
        <div class="node-info">
            <div class="info-row">
                <span class="info-label">Address:</span>
                <span class="info-value">${node.address}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Messages Sent:</span>
                <span class="info-value">${node.messagesSent || 0}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Messages Delivered:</span>
                <span class="info-value">${node.messagesDelivered || 0}</span>
            </div>
    `;

    if (node.isSequencer) {
        html += `
            <div class="info-row">
                <span class="info-label">Next Sequence #:</span>
                <span class="info-value">${node.nextSequenceNum || 1}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Pending Messages:</span>
                <span class="info-value">${node.pendingMessages || 0}</span>
            </div>
        `;
    }

    html += '</div>';
    return html;
}

/**
 * Renders protocol metrics section
 * @param {Object} node - Node state object
 * @returns {string} HTML string for metrics
 */
function renderMetrics(node) {
    if (!node.metrics) return '';

    return `
        <div class="metrics-section">
            <div class="metrics-title">📊 Protocol Metrics:</div>
            <div class="metrics-grid">
                <div class="metric-item">
                    <div class="metric-label">Total Broadcasts</div>
                    <div class="metric-value">${node.metrics.totalBroadcasts || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Total Deliveries</div>
                    <div class="metric-value">${node.metrics.totalDeliveries || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Avg Latency</div>
                    <div class="metric-value">${(node.metrics.avgLatencyMs || 0).toFixed(2)} ms</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Throughput</div>
                    <div class="metric-value">${(node.metrics.messageThroughput || 0).toFixed(1)} msg/s</div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Renders recent delivered messages
 * @param {Object} node - Node state object
 * @returns {string} HTML string for messages
 */
function renderRecentMessages(node) {
    if (!node.recentMessages || node.recentMessages.length === 0) {
        return '';
    }

    return `
        <div class="messages-section">
            <div class="messages-title">📬 Recent Delivered Messages:</div>
            ${node.recentMessages.slice(0, 5).map(msg => `
                <div class="message-item">
                    <span class="message-seq">#${msg.sequenceNumber}</span>
                    <span class="message-from">from ${msg.from}</span>
                    <div class="message-payload">${msg.payload}</div>
                </div>
            `).join('')}
        </div>
    `;
}

/**
 * Renders recent activity events
 * @param {Object} node - Node state object
 * @returns {string} HTML string for recent events
 */
function renderRecentEvents(node) {
    if (!node.recentEvents || node.recentEvents.length === 0) {
        return '';
    }

    return `
        <div class="events-section">
            <div class="events-title">📋 Recent Activity:</div>
            ${node.recentEvents.map(event => renderEvent(event)).join('')}
        </div>
    `;
}

/**
 * Renders a single event
 * @param {Object} event - Event object
 * @returns {string} HTML string for the event
 */
function renderEvent(event) {
    return `
        <div class="event-item ${event.type.replace(/_/g, '-')}">
            <div class="event-type">${event.type.replace(/_/g, ' ')}</div>
            <div class="event-msg">${event.message}</div>
            <div class="event-time">${formatTimestamp(event.timestamp)}</div>
        </div>
    `;
}

/**
 * Submits a broadcast request
 * @param {Event} event - Form submit event
 */
function submitBroadcast(event) {
    event.preventDefault();

    const nodeIndex = parseInt(document.getElementById('nodeSelect').value);
    const message = document.getElementById('messageInput').value;

    if (!message.trim()) {
        showStatusMessage({ success: false, error: 'Message cannot be empty' });
        return;
    }

    broadcastFromNode(nodeIndex, message, true);
}

/**
 * Broadcast a message from a specific node
 * @param {number} nodeIndex - Index of the node (1-3)
 * @param {string} message - Message to broadcast
 * @param {boolean} clearInput - Whether to clear the input field
 */
function broadcastFromNode(nodeIndex, message, clearInput = false) {
    // Show which node is broadcasting
    showBroadcastingIndicator(nodeIndex);

    fetch('/api/broadcast', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nodeIndex, message })
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            if (clearInput) {
                document.getElementById('messageInput').value = '';
            }
            setTimeout(updateCluster, 300);
        } else {
            showStatusMessage(data);
        }
    })
    .catch(err => {
        showStatusMessage({ success: false, error: err.message });
    });
}

/**
 * Quick broadcast demo - sends 5 messages from different nodes to demonstrate sequencing
 */
async function quickBroadcast() {
    showStatusMessage({ success: true }, 'Quick broadcast started - watch the sequencing!');

    const messages = [
        { node: 1, text: 'Message A from Node-1' },
        { node: 2, text: 'Message B from Node-2' },
        { node: 3, text: 'Message C from Node-3' },
        { node: 1, text: 'Message D from Node-1' },
        { node: 2, text: 'Message E from Node-2' },
    ];

    for (const msg of messages) {
        broadcastFromNode(msg.node, msg.text);
        await sleep(800); // Wait 800ms between messages
    }
}

/**
 * Rapid fire broadcast - sends many messages quickly to show sequencing under load
 */
async function rapidBroadcast() {
    showStatusMessage({ success: true }, 'Rapid fire! Watch how the sequencer maintains order...');

    for (let i = 1; i <= 10; i++) {
        const nodeIndex = ((i - 1) % 3) + 1; // Rotate through nodes 1, 2, 3
        const message = `Rapid-${i}`;
        broadcastFromNode(nodeIndex, message);
        await sleep(200); // Very quick - 200ms between messages
    }
}

/**
 * Shows a visual indicator of which node is broadcasting
 * @param {number} nodeIndex - Index of the broadcasting node
 */
function showBroadcastingIndicator(nodeIndex) {
    // Remove any existing indicator
    const existing = document.getElementById('broadcastingIndicator');
    if (existing) {
        existing.remove();
    }

    // Create new indicator
    const indicator = document.createElement('div');
    indicator.id = 'broadcastingIndicator';
    indicator.className = 'broadcasting-indicator';
    indicator.textContent = `📡 Node-${nodeIndex} broadcasting...`;
    document.body.appendChild(indicator);

    // Highlight the node card
    highlightNodeCard(nodeIndex);

    // Remove after 1 second
    setTimeout(() => {
        indicator.remove();
    }, 1000);
}

/**
 * Highlights a node card briefly
 * @param {number} nodeIndex - Index of the node to highlight (1-3)
 */
function highlightNodeCard(nodeIndex) {
    const cards = document.querySelectorAll('.node-card');
    // nodeIndex 1-3 corresponds to cards[1-3] (card[0] is sequencer)
    if (cards[nodeIndex]) {
        cards[nodeIndex].classList.add('broadcasting');
        setTimeout(() => {
            cards[nodeIndex].classList.remove('broadcasting');
        }, 500);
    }
}

/**
 * Sleep utility for async delays
 * @param {number} ms - Milliseconds to sleep
 */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Displays a status message to the user
 * @param {Object} data - Response data with success flag and optional error
 * @param {string} message - Optional message to display
 */
function showStatusMessage(data, message = '') {
    const statusMsg = document.getElementById('statusMessage');

    if (data.success) {
        statusMsg.className = 'status-message status-success';
        statusMsg.textContent = message || `✓ Message broadcast - Watch it get sequenced and delivered!`;
    } else {
        statusMsg.className = 'status-message status-error';
        statusMsg.textContent = `✗ Failed: ${data.error || 'Unknown error'}`;
    }

    statusMsg.style.display = 'block';
    setTimeout(() => statusMsg.style.display = 'none', 5000);
}

/**
 * Animates the refresh indicator to show live updates
 */
function pulseRefreshIndicator() {
    const indicator = document.getElementById('refreshIndicator');
    indicator.style.opacity = '0.5';
    setTimeout(() => indicator.style.opacity = '1', 200);
}

/**
 * Initialize the application
 * Sets up periodic updates and loads initial state
 */
function init() {
    // Initial load
    updateCluster();

    // Update every second
    setInterval(() => {
        updateCluster();
        pulseRefreshIndicator();
    }, 1000);
}

// Start the application when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}

