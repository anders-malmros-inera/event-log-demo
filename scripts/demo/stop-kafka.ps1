# Stop Kafka broker to demonstrate producer-side queueing

Write-Host "[$(Get-Date)] Stopping Kafka broker..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:5000/control/kafka/stop" -Method POST -UseBasicParsing | Out-Null

Write-Host "[$(Get-Date)] Kafka stopped. Producer will queue messages in memory." -ForegroundColor Green
Write-Host "[$(Get-Date)] Expected behavior: Producer count increases, Kafka/Cassandra counts remain static"
Write-Host "[$(Get-Date)] Check metrics: http://localhost:5000/monitor"
