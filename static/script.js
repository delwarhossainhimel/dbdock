// ============================================================
// GLOBAL HELPERS
// ============================================================

document.addEventListener('DOMContentLoaded', function() {

    // --------------------------------------------
    // Test database connection (server cards)
    // --------------------------------------------
    document.querySelectorAll('.test-connection').forEach(button => {
        if (button.dataset.testConnectionWired === '1') return;
        button.dataset.testConnectionWired = '1';

        button.addEventListener('click', function() {
            const serverId = this.dataset.serverId;
            const btn = this;

            btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span> Testing...';
            btn.disabled = true;

            fetch(`/test_connection/${serverId}`)
                .then(response => response.json())
                .then(data => {
                    const alert = document.createElement('div');
                    alert.className = `alert alert-${data.success ? 'success' : 'danger'} alert-dismissible fade show mt-2`;
                    alert.innerHTML = `
                        ${data.message}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    `;

                    btn.closest('.card-body').appendChild(alert);

                    btn.innerHTML = 'Test Connection';
                    btn.disabled = false;

                    setTimeout(() => {
                        alert.remove();
                    }, 5000);
                })
                .catch(error => {
                    console.error('Error testing connection:', error);
                    btn.innerHTML = 'Test Connection';
                    btn.disabled = false;
                });
        });
    });

    // --------------------------------------------
    // Load databases when a server is selected
    // --------------------------------------------
    const serverSelect = document.getElementById('database_server');
    if (serverSelect) {
        serverSelect.addEventListener('change', function() {
            const serverId = this.value;
            const databaseList = document.getElementById('database-list');

            if (!databaseList) return;

            if (!serverId) {
                databaseList.innerHTML = '';
                return;
            }

            databaseList.innerHTML = '<div class="text-center my-3"><div class="spinner-border" role="status"></div></div>';

            fetch(`/get_databases/${serverId}`)
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        databaseList.innerHTML = '';

                        data.databases.forEach(db => {
                            const checkbox = document.createElement('div');
                            checkbox.className = 'form-check';
                            checkbox.innerHTML = `
                                <input class="form-check-input" type="checkbox" name="databases" value="${db}" id="db-${db}">
                                <label class="form-check-label" for="db-${db}">${db}</label>
                            `;
                            databaseList.appendChild(checkbox);
                        });

                        // Add "Select All" option
                        const selectAll = document.createElement('div');
                        selectAll.className = 'form-check mt-2';
                        selectAll.innerHTML = `
                            <input class="form-check-input" type="checkbox" id="select-all">
                            <label class="form-check-label fw-bold" for="select-all">Select All</label>
                        `;
                        databaseList.appendChild(selectAll);

                        document.getElementById('select-all').addEventListener('click', function() {
                            const checkboxes = databaseList.querySelectorAll('input[type="checkbox"]:not(#select-all)');
                            checkboxes.forEach(checkbox => {
                                checkbox.checked = this.checked;
                            });
                        });
                    } else {
                        databaseList.innerHTML = `<div class="alert alert-danger">${data.message}</div>`;
                    }
                })
                .catch(error => {
                    databaseList.innerHTML = `<div class="alert alert-danger">Error loading databases: ${error}</div>`;
                });
        });
    }

    // --------------------------------------------
    // Show/hide schedule options based on selection
    // (legacy single-schedule form — backup_jobs older template)
    // --------------------------------------------
    const scheduleType = document.getElementById('schedule_type');
    if (scheduleType) {
        scheduleType.addEventListener('change', function() {
            document.querySelectorAll('.schedule-option').forEach(el => {
                el.style.display = 'none';
            });

            const selectedOption = document.getElementById(`${this.value}-options`);
            if (selectedOption) {
                selectedOption.style.display = 'block';
            }
        });

        // Trigger on page load
        scheduleType.dispatchEvent(new Event('change'));
    }

    // --------------------------------------------
    // Show/hide storage configuration based on type
    // --------------------------------------------
    const storageType = document.getElementById('type');
    if (storageType) {
        storageType.addEventListener('change', function() {
            document.querySelectorAll('.storage-config').forEach(el => {
                el.style.display = 'none';
            });

            const selectedConfig = document.getElementById(`${this.value}-config`);
            if (selectedConfig) {
                selectedConfig.style.display = 'block';
            }
        });

        // Trigger on page load
        storageType.dispatchEvent(new Event('change'));
    }
});


// ============================================================
// DATABASE SERVER MANAGEMENT
// ============================================================

document.addEventListener('DOMContentLoaded', function() {

    // --------------------------------------------
    // Delete Server
    // --------------------------------------------
    document.querySelectorAll('.delete-server').forEach(button => {
        button.addEventListener('click', function() {
            const serverId = this.dataset.serverId;
            const serverName = this.closest('.card-body').querySelector('.card-title').textContent.trim();

            console.log('🗑️ Delete server clicked:', serverId, serverName);

            if (!confirm(`Are you sure you want to delete server "${serverName}"?`)) {
                return;
            }

            const originalHtml = this.innerHTML;
            this.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span> Deleting...';
            this.disabled = true;

            fetch(`/delete_server/${serverId}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
            })
            .then(response => {
                console.log('Response status:', response.status);
                return response.json();
            })
            .then(data => {
                console.log('Response data:', data);
                if (data.success) {
                    alert(data.message);
                    location.reload();
                } else {
                    alert('Error: ' + data.message);
                    this.innerHTML = originalHtml;
                    this.disabled = false;
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('Error: ' + error);
                this.innerHTML = originalHtml;
                this.disabled = false;
            });
        });
    });

    // --------------------------------------------
    // NOTE: The `.test-connection` handler that used to live here has
    //       been REMOVED — it was duplicating the one in the GLOBAL
    //       HELPERS block above, causing every click to fire TWO
    //       requests to /test_connection/<id> and display TWO alerts.
    //       The GLOBAL HELPERS block handles it (and now also uses
    //       a data-attribute guard so it can't be double-wired).
    // --------------------------------------------

    // --------------------------------------------
    // Auto-refresh server status every 30 seconds
    // --------------------------------------------
    function refreshServerStatus() {
        const serverCards = document.querySelectorAll('.server-card');
        if (serverCards.length === 0) return;

        if (!window.location.pathname.includes('database_servers')) return;

        fetch(window.location.href, {
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.text())
        .then(html => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');

            doc.querySelectorAll('.status-indicator').forEach((newIndicator, index) => {
                const oldIndicator = document.querySelectorAll('.status-indicator')[index];
                if (oldIndicator) {
                    oldIndicator.className = newIndicator.className;
                    oldIndicator.title = newIndicator.title;
                }
            });

            doc.querySelectorAll('.status-info').forEach((newInfo, index) => {
                const oldInfo = document.querySelectorAll('.status-info')[index];
                if (oldInfo) {
                    oldInfo.innerHTML = newInfo.innerHTML;
                }
            });
        })
        .catch(error => console.log('Status refresh error:', error));
    }

    if (window.location.pathname.includes('database_servers')) {
        setInterval(refreshServerStatus, 30000);
        console.log('🔄 Auto-refresh enabled for server status');
    }
});


// ============================================================
// DUMP CLIENT SELECTOR (Add / Edit Database Server)
// ============================================================
// Toggles between the MySQL and PostgreSQL dump-client <select>s
// whenever the Type dropdown changes. Works on:
//   - database_servers.html (Add Server modal)
//   - edit_server.html     (Edit Server form)
//
// Safe on every other page — silently does nothing if the
// three target elements are not present.
// ============================================================
(function initDumpClientSelector() {
    function wire() {
        const typeSelect  = document.getElementById('serverTypeSelect');
        const mysqlSelect = document.getElementById('mysqlDumpClientSelect');
        const pgSelect    = document.getElementById('pgDumpClientSelect');

        // Not on a page that uses the dump-client UI — nothing to do.
        if (!typeSelect || !mysqlSelect || !pgSelect) return;

        // Prevent duplicate listeners if script is loaded twice.
        if (typeSelect.dataset.dumpClientWired === '1') return;
        typeSelect.dataset.dumpClientWired = '1';

        function sync() {
            const isPg = typeSelect.value === 'postgres';

            mysqlSelect.classList.toggle('d-none', isPg);
            mysqlSelect.disabled = isPg;

            pgSelect.classList.toggle('d-none', !isPg);
            pgSelect.disabled = !isPg;
        }

        typeSelect.addEventListener('change', sync);
        sync(); // initial state
        console.log('[dump-client] selector wired');
    }

    // Run as soon as DOM is ready. `readyState` may already be
    // "interactive" if this script is loaded with `defer` or at
    // the bottom of <body>.
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', wire);
    } else {
        wire();
    }
})();


// ============================================================
// DUMP CLIENT INFO PANEL (Edit Backup Job page)
// ============================================================
// Reads the JSON map injected by the template (`server_dump_info`)
// and updates the "Dump client:" badge whenever the Database Server
// dropdown changes, plus once on load.
// ============================================================
(function initDumpClientInfoPanel() {
    function wire() {
        const serverSelect         = document.getElementById('database_server');
        const dumpClientValueEl    = document.getElementById('dump-client-value');
        const dumpClientEditLinkEl = document.getElementById('dump-client-edit-link');

        // Not on the edit-job page — nothing to do.
        if (!serverSelect || !dumpClientValueEl) return;

        // Pull the map from a global set by the template.
        // Fallback to {} so a missing map never throws.
        const serverDumpInfo = window.__serverDumpInfo || {};

        // Debug: log once so we can see what the route passed
        console.log('[dump-client-info] servers map:', serverDumpInfo);

        function sync() {
            const info = serverDumpInfo[serverSelect.value];
            console.log('[dump-client-info] selected server:', serverSelect.value, '→', info);

            if (!info) {
                dumpClientValueEl.textContent = '—';
                dumpClientValueEl.className = 'badge bg-secondary';
                if (dumpClientEditLinkEl) dumpClientEditLinkEl.innerHTML = '';
                return;
            }

            dumpClientValueEl.textContent = info.label || info.dump_client || '—';
            dumpClientValueEl.className = 'badge bg-dark';

            if (dumpClientEditLinkEl) {
                dumpClientEditLinkEl.innerHTML =
                    `<a href="${info.edit_url}" class="link-primary ms-1">Edit server to change</a>`;
            }
        }

        serverSelect.addEventListener('change', sync);
        sync();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', wire);
    } else {
        wire();
    }
})();