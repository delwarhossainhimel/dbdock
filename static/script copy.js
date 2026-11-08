document.addEventListener('DOMContentLoaded', function() {
    // Test database connection
    document.querySelectorAll('.test-connection').forEach(button => {
        button.addEventListener('click', function() {
            const serverId = this.dataset.serverId;
            const button = this;
            
            button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span> Testing...';
            button.disabled = true;
            
            fetch(`/test_connection/${serverId}`)
                .then(response => response.json())
                .then(data => {
                    const alert = document.createElement('div');
                    alert.className = `alert alert-${data.success ? 'success' : 'danger'} alert-dismissible fade show mt-2`;
                    alert.innerHTML = `
                        ${data.message}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    `;
                    
                    button.closest('.card-body').appendChild(alert);
                    
                    button.innerHTML = 'Test Connection';
                    button.disabled = false;
                    
                    setTimeout(() => {
                        alert.remove();
                    }, 5000);
                });
        });
    });

    // Load databases when server is selected
    const serverSelect = document.getElementById('database_server');
    if (serverSelect) {
        serverSelect.addEventListener('change', function() {
            const serverId = this.value;
            const databaseList = document.getElementById('database-list');
            
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
                        
                        // Add select all option
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
                });
        });
    }
    
    // Show/hide schedule options based on selection
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
        
        // Trigger change event on page load
        scheduleType.dispatchEvent(new Event('change'));
    }
    
    // Show/hide storage configuration based on type
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
        
        // Trigger change event on page load
        storageType.dispatchEvent(new Event('change'));
    }
});
// ============================================================
// DATABASE SERVER MANAGEMENT
// ============================================================

document.addEventListener('DOMContentLoaded', function() {
    
    // ============================================
    // Delete Server Functionality
    // ============================================
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
    
    // ============================================
    // Test Connection Functionality
    // ============================================
    document.querySelectorAll('.test-connection').forEach(button => {
        button.addEventListener('click', function() {
            const serverId = this.dataset.serverId;
            const button = this;
            
            button.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span> Testing...';
            button.disabled = true;
            
            fetch(`/test_connection/${serverId}`)
                .then(response => response.json())
                .then(data => {
                    const alert = document.createElement('div');
                    alert.className = `alert alert-${data.success ? 'success' : 'danger'} alert-dismissible fade show mt-2`;
                    alert.innerHTML = `
                        ${data.message}
                        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                    `;
                    
                    button.closest('.card-body').appendChild(alert);
                    
                    button.innerHTML = 'Test Connection';
                    button.disabled = false;
                    
                    setTimeout(() => {
                        alert.remove();
                    }, 5000);
                })
                .catch(error => {
                    console.error('Error testing connection:', error);
                    button.innerHTML = 'Test Connection';
                    button.disabled = false;
                });
        });
    });
    
    // ============================================
    // Auto-refresh server status (every 30 seconds)
    // ============================================
    function refreshServerStatus() {
        const serverCards = document.querySelectorAll('.server-card');
        if (serverCards.length === 0) return;
        
        // Only refresh if on the database servers page
        if (!window.location.pathname.includes('database_servers')) return;
        
        fetch(window.location.href, {
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.text())
        .then(html => {
            // Parse the HTML and update only the status indicators
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
    
    // Auto-refresh every 30 seconds if on database servers page
    if (window.location.pathname.includes('database_servers')) {
        setInterval(refreshServerStatus, 30000);
        console.log('🔄 Auto-refresh enabled for server status');
    }
});