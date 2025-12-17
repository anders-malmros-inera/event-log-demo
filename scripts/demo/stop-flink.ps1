# Stop Flink job manager to demonstrate queue buildup in Kafka

Write-Host "[$(Get-Date)] Stopping Flink job manager..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:5000/control/flink/stop" -Method POST -UseBasicParsing | Out-Null

Write-Host "[$(Get-Date)] Flink stopped. Monitor Kafka lag at http://localhost:5000/monitor" -ForegroundColor Green
Write-Host "[$(Get-Date)] Expected behavior: Kafka offsets will continue growing while Cassandra count stays static"
