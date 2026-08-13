<!-- Add Chart.js CDN in base.html or dashboard.html -->
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<!-- Add this after the statistics cards -->
<div class="row mt-4">
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Backup Status Distribution (Last 30 Days)</h5>
            </div>
            <div class="card-body">
                <canvas id="statusChart" height="200"></canvas>
            </div>
        </div>
    </div>
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">
                <h5>Job Activity</h5>
            </div>
            <div class="card-body">
                <canvas id="activityChart" height="200"></canvas>
            </div>
        </div>
    </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', function() {
    // Status Distribution Chart
    const ctx1 = document.getElementById('statusChart').getContext('2d');
    new Chart(ctx1, {
        type: 'doughnut',
        data: {
            labels: ['Successful', 'Failed', 'Running'],
            datasets: [{
                data: [{{ successful_runs }}, {{ failed_runs }}, {{ running_jobs }}],
                backgroundColor: ['#28a745', '#dc3545', '#ffc107'],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
    
    // Activity Chart (Last 7 days - you'll need to add this data)
    // This requires additional data from the backend
});
</script>