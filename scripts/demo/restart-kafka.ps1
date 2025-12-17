# Restart Kafka to flush queued messages

Write-Host "[$(Get-Date)] Restarting Kafka broker..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:5000/control/kafka/restart" -Method POST -UseBasicParsing | Out-Null

Write-Host "[$(Get-Date)] Kafka restarted. Producer will flush queued messages." -ForegroundColor Green
Write-Host "[$(Get-Date)] Expected behavior: Kafka count will quickly catchup to producer count"
