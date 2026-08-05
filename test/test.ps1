$ret = Invoke-RestMethod -Uri http://localhost:3000/orders -Method Post -ContentType "application/json" -Body (@{customerId="11111111-1111-1111-1111-111111111111"; totalCents=1500} | ConvertTo-Json)

Write-Host "Order RestMethod return order_id: $ret"

$order_id    = $ret.id
$customer_id = $ret.customer_id
$total_cents = $ret.total_cents
$status      = $ret.status
$created_at  = $ret.created_at
Write-Host "Order RestMethod return order_id: $order_id"
Write-Host "Order RestMethod return customer_id: $customer_id"
Write-Host "Order RestMethod return total_cents: $total_cents"
Write-Host "Order RestMethod return status: $status"
Write-Host "Order RestMethod return created_at: $created_at"

docker compose stop poller

docker compose exec postgres psql -U postgres -d eventflow -c "SELECT event_type, published_at FROM outbox WHERE aggregate_id = '$order_id';"

# published_at should be NULL while poller is stopped
docker compose start poller
docker compose logs poller --tail 20
