# Restart Flink to resume processing backlog

Write-Host "[$(Get-Date)] Restarting Flink job manager..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:5000/control/flink/restart" -Method POST -UseBasicParsing | Out-Null

Write-Host "[$(Get-Date)] Flink restarted. It will now process the backlog." -ForegroundColor Green
Write-Host "[$(Get-Date)] Expected behavior: Cassandra count will start increasing, catching up to Kafka offsets"
