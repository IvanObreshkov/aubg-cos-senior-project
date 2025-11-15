/**
 * SWIM Visual Demo - Frontend JavaScript
 * Handles real-time cluster state updates and node crash simulation
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
    return `
        <div class="node-card">
            ${renderNodeHeader(node)}
            ${renderNodeInfo(node)}
            ${renderMetrics(node)}
            ${renderMembers(node)}
            ${renderRecentEvents(node)}
            ${renderCrashButton(node)}
        </div>
    `;
}

/**
 * Renders the node header with name and status badge
 * @param {Object} node - Node state object
 * @returns {string} HTML string for node header
 */
function renderNodeHeader(node) {
    return `
        <div class="node-header">
            <div class="node-name">${node.id}</div>
            <div class="node-status status-${node.status.toLowerCase()}">
                ${node.status}
            </div>
        </div>
    `;
}

/**
 * Renders node information rows
 * @param {Object} node - Node state object
 * @returns {string} HTML string for node info
 */
function renderNodeInfo(node) {
    return `
        <div class="node-info">
            <div class="info-row">
                <span class="info-label">Address:</span>
                <span class="info-value">${node.address}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Incarnation:</span>
                <span class="info-value">${node.incarnation}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Total Members:</span>
                <span class="info-value">${node.memberCount}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Alive / Suspect / Failed:</span>
                <span class="info-value">
                    <span style="color: #10b981;">${node.aliveCount}</span> / 
                    <span style="color: #f59e0b;">${node.suspectCount}</span> / 
                    <span style="color: #ef4444;">${node.failedCount}</span>
                </span>
            </div>
        </div>
    `;
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
                    <div class="metric-label">Probes Sent</div>
                    <div class="metric-value">${node.metrics.probesSent || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Probes Received</div>
                    <div class="metric-value">${node.metrics.probesReceived || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Acks Sent</div>
                    <div class="metric-value">${node.metrics.acksSent || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Acks Received</div>
                    <div class="metric-value">${node.metrics.acksReceived || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Suspects Raised</div>
                    <div class="metric-value">${node.metrics.suspectsRaised || 0}</div>
                </div>
                <div class="metric-item">
                    <div class="metric-label">Failures Detected</div>
                    <div class="metric-value">${node.metrics.failuresDetected || 0}</div>
                </div>
            </div>
        </div>
    `;
}

/**
 * Renders known members list
 * @param {Object} node - Node state object
 * @returns {string} HTML string for members
 */
function renderMembers(node) {
    if (!node.members || node.members.length === 0) {
        return '';
    }

    return `
        <div class="members-section">
            <div class="members-title">👥 Known Members (${node.members.length}):</div>
            ${node.members.slice(0, 5).map(member => `
                <div class="member-item">
                    <div class="member-info">
                        <div class="member-id">${member.id}</div>
                        <div class="member-address">${member.address}</div>
                    </div>
                    <span class="member-status ${member.status.toLowerCase()}">${member.status}</span>
                </div>
            `).join('')}
            ${node.members.length > 5 ? `<div style="color: #666; font-size: 0.85em; margin-top: 5px;">... and ${node.members.length - 5} more</div>` : ''}
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
 * Renders crash button for simulating node failures
 * @param {Object} node - Node state object
 * @returns {string} HTML string for crash button
 */
function renderCrashButton(node) {
    const nodeIndex = parseInt(node.id.split('-')[1]) - 1;

    return `
        <button 
            class="crash-button" 
            onclick="crashNode(${nodeIndex})"
        >
            💥 Simulate Crash
        </button>
    `;
}

/**
 * Simulates a node crash
 * @param {number} nodeIndex - Index of the node to crash
 */
function crashNode(nodeIndex) {
    if (!confirm(`Are you sure you want to crash Node-${nodeIndex + 1}?\n\nThis will simulate an abrupt failure.`)) {
        return;
    }

    fetch('/api/crash-node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nodeIndex })
    })
    .then(res => res.json())
    .then(data => {
        showStatusMessage(data, `Node-${nodeIndex + 1}`);
        if (data.success) {
            // Refresh immediately to see the change
            setTimeout(updateCluster, 500);
        }
    })
    .catch(err => {
        showStatusMessage({ success: false, error: err.message });
    });
}

/**
 * Displays a status message to the user
 * @param {Object} data - Response data with success flag and optional error
 * @param {string} nodeName - Name of the node that was affected
 */
function showStatusMessage(data, nodeName) {
    const statusMsg = document.getElementById('statusMessage');

    if (data.success) {
        statusMsg.className = 'status-message status-success';
        statusMsg.textContent = `✓ ${nodeName} crashed! Watch other nodes detect the failure...`;
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

