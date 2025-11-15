/**
 * Raft Visual Demo - Frontend JavaScript
 * Handles real-time cluster state updates and command submission
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
            view.innerHTML = data.servers.map(server => renderServerCard(server)).join('');
        })
        .catch(err => console.error('Error fetching cluster state:', err));
}

/**
 * Renders a single server card with all its information
 * @param {Object} server - Server state object
 * @returns {string} HTML string for the server card
 */
function renderServerCard(server) {
    return `
        <div class="server-card">
            ${renderServerHeader(server)}
            ${renderServerInfo(server)}
            ${renderLogEntries(server)}
            ${renderStateMachine(server)}
            ${renderRecentEvents(server)}
        </div>
    `;
}

/**
 * Renders the server header with name and state badge
 * @param {Object} server - Server state object
 * @returns {string} HTML string for server header
 */
function renderServerHeader(server) {
    return `
        <div class="server-header">
            <div class="server-name">${server.id}</div>
            <div class="server-state state-${server.state.toLowerCase()}">
                ${server.isLeader ? '👑 ' : ''}${server.state}
            </div>
        </div>
    `;
}

/**
 * Renders server information rows (term, indices, etc.)
 * @param {Object} server - Server state object
 * @returns {string} HTML string for server info
 */
function renderServerInfo(server) {
    return `
        <div class="server-info">
            <div class="info-row">
                <span class="info-label">Address:</span>
                <span class="info-value">${server.address}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Term:</span>
                <span class="info-value">${server.term}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Commit Index:</span>
                <span class="info-value">${server.commitIndex}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Last Applied:</span>
                <span class="info-value">${server.lastApplied}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Last Log Index:</span>
                <span class="info-value">${server.lastLogIndex}</span>
            </div>
        </div>
    `;
}

/**
 * Renders recent log entries section
 * @param {Object} server - Server state object
 * @returns {string} HTML string for log entries
 */
function renderLogEntries(server) {
    if (!server.logEntries || server.logEntries.length === 0) {
        return '';
    }

    return `
        <div class="log-entries">
            <div class="log-entries-title">Recent Log Entries:</div>
            ${server.logEntries.map(entry => `<div class="log-entry">${entry}</div>`).join('')}
        </div>
    `;
}

/**
 * Renders state machine contents (KV store)
 * @param {Object} server - Server state object
 * @returns {string} HTML string for state machine
 */
function renderStateMachine(server) {
    if (!server.stateMachineState) {
        return '';
    }

    const entries = Object.entries(server.stateMachineState);
    const content = entries.length > 0
        ? entries.map(([key, value]) => `
            <div class="kv-pair">
                <span class="kv-key">${key}</span>
                <span class="kv-value">${value}</span>
            </div>
          `).join('')
        : '<div class="state-machine-empty">Empty (no commands applied yet)</div>';

    return `
        <div class="state-machine">
            <div class="state-machine-title">💾 State Machine Contents:</div>
            ${content}
        </div>
    `;
}

/**
 * Renders recent activity events for a server
 * @param {Object} server - Server state object
 * @returns {string} HTML string for recent events
 */
function renderRecentEvents(server) {
    if (!server.recentEvents || server.recentEvents.length === 0) {
        return '';
    }

    return `
        <div class="server-events">
            <div class="server-events-title">📋 Recent Activity:</div>
            ${server.recentEvents.map(event => renderEvent(event)).join('')}
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
        <div class="server-event ${event.type.replace(/_/g, '-')}">
            <div class="server-event-type">${event.type.replace(/_/g, ' ')}</div>
            <div class="server-event-msg">${event.message.replace(event.serverId, '').trim()}</div>
            <div class="server-event-time">${formatTimestamp(event.timestamp)}</div>
        </div>
    `;
}

/**
 * Handles command submission from the form
 * @param {Event} event - Form submit event
 */
function submitCommand(event) {
    event.preventDefault();
    const input = document.getElementById('commandInput');
    const command = input.value.trim();

    if (!command) return;

    fetch('/api/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command })
    })
    .then(res => res.json())
    .then(data => {
        showStatusMessage(data);
        if (data.success) {
            input.value = '';
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
 */
function showStatusMessage(data) {
    const statusMsg = document.getElementById('statusMessage');

    if (data.success) {
        statusMsg.className = 'status-message status-success';
        statusMsg.textContent = `✓ Command submitted successfully! Index: ${data.index}, Leader: ${data.leader}`;
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

