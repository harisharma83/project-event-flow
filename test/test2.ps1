$ret = Invoke-RestMethod -Uri http://localhost:3000/orders -Method Post -ContentType "application/json" -Body (@{customerId="11111111-1111-1111-1111-111111111111"; totalCents=2599} | ConvertTo-Json)

Write-Host "Order RestMethod return order_id: $ret"

$orderid    = $ret.id

Write-Host "Order RestMethod return order_id: $orderid"

Start-Sleep -Seconds 1

docker compose logs saga-orchestrator --tail 20

Write-Host "http://localhost:3001/orders/$orderid/status"

$ret2 = Invoke-RestMethod -Uri "http://localhost:3001/orders/$orderid/status"

Write-Host "Order status: $ret2"