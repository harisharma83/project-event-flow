# Verify 1 — Confirm baseline CLOSED state:
docker compose exec kafka rpk group describe payment-service
# check lag = 0 or not, set GATEWAY_FAILURE_MODE: "off" in docker-compose.yml, run command: docker compose up -d payment-service
docker compose exec order-service node scripts/floodPayments.js 1

# check logs
#PS C:\work\interview\preparation\project> docker compose exec order-service node scripts/floodPayments.js 1
#{"level":"WARN","timestamp":"2026-08-05T18:06:56.656Z","logger":"kafkajs","message":"KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option \"createPartitioner: Partitioners.LegacyPartitioner\". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable \"KAFKAJS_NO_PARTITIONER_WARNING=1\""}
#Sending 1 distinct ChargePayment commands, 300ms apart...
#  sent ChargePayment for 81728070-67be-4c09-8aaa-a9d7bdbc5d86 (1/1)
#Done.

docker compose logs payment-service --tail 20
# check the payment service log same "sent ChargePayment for 81728070-67be-4c09-8aaa-a9d7bdbc5d86" and "[payment] charged 2599c for order 81728070-67be-4c09-8aaa-a9d7bdbc5d86"
# PS C:\work\interview\preparation\project> docker compose logs payment-service --tail 20
#payment-service-1  | {"level":"WARN","timestamp":"2026-08-05T17:58:15.035Z","logger":"kafkajs","message":"KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option \"createPartitioner: Partitioners.LegacyPartitioner\". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable \"KAFKAJS_NO_PARTITIONER_WARNING=1\""}
#payment-service-1  | [health] listening on :8080 (liveness only)
#payment-service-1  | {"level":"INFO","timestamp":"2026-08-05T17:58:15.059Z","logger":"kafkajs","message":"[Consumer] Starting","groupId":"payment-service"}
#payment-service-1  | {"level":"INFO","timestamp":"2026-08-05T17:58:43.575Z","logger":"kafkajs","message":"[ConsumerGroup] Consumer has joined the group","groupId":"payment-service","memberId":"payment-service-db8b90e5-826e-435d-b28e-1d3d0e25b13d","leaderId":"payment-service-db8b90e5-826e-435d-b28e-1d3d0e25b13d","isLeader":true,"memberAssignment":{"payment-commands":[0]},"groupProtocol":"RoundRobinAssigner","duration":28515}
#payment-service-1  | Payment Service consuming payment-commands (gateway failure mode: off)
#payment-service-1  | [payment] charged 2599c for order 81728070-67be-4c09-8aaa-a9d7bdbc5d86

# Verify 2 — Switch to recovering mode and restart fresh
# set GATEWAY_FAILURE_MODE: "recovering" in docker-compose.yml, run command: docker compose up -d payment-service
docker compose up -d payment-service

# open a live, non-truncated log stream of payment-service
docker compose logs -f --timestamps payment-service
#payment-service-1  | 2026-08-05T18:11:42.718267141Z {"level":"WARN","timestamp":"2026-08-05T18:11:42.716Z","logger":"kafkajs","message":"KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option \"createPartitioner: Partitioners.LegacyPartitioner\". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable \"KAFKAJS_NO_PARTITIONER_WARNING=1\""}
#payment-service-1  | 2026-08-05T18:11:42.720582118Z [health] listening on :8080 (liveness only)
#payment-service-1  | 2026-08-05T18:11:42.752355318Z {"level":"INFO","timestamp":"2026-08-05T18:11:42.751Z","logger":"kafkajs","message":"[Consumer] Starting","groupId":"payment-service"}
#payment-service-1  | 2026-08-05T18:12:06.978769585Z {"level":"INFO","timestamp":"2026-08-05T18:12:06.978Z","logger":"kafkajs","message":"[ConsumerGroup] Consumer has joined the group","groupId":"payment-service","memberId":"payment-service-5dff0283-1f60-4b40-a5ae-e1eedba239ca","leaderId":"payment-service-5dff0283-1f60-4b40-a5ae-e1eedba239ca","isLeader":true,"memberAssignment":{"payment-commands":[0]},"groupProtocol":"RoundRobinAssigner","duration":24225}
#payment-service-1  | 2026-08-05T18:12:06.980000270Z Payment Service consuming payment-commands (gateway failure mode: recovering)

# Verify Step 3 — Trigger the trip to OPEN:
# generate 10 orders
docker compose exec order-service node scripts/floodPayments.js 10

# this since payment-service is in recovery mode half_open, send 1 message and retry three times but failed then 
# move circuit braker from Closed to open

#PS C:\work\interview\preparation\project> docker compose logs -f --timestamps payment-service
#payment-service-1  | 2026-08-05T18:11:42.718267141Z {"level":"WARN","timestamp":"2026-08-05T18:11:42.716Z","logger":"kafkajs","message":"KafkaJS v2.0.0 switched default partitioner. To retain the same partitioning behavior as in previous versions, create the producer with the option \"createPartitioner: Partitioners.LegacyPartitioner\". See the migration guide at https://kafka.js.org/docs/migration-guide-v2.0.0#producer-new-default-partitioner for details. Silence this warning by setting the environment variable \"KAFKAJS_NO_PARTITIONER_WARNING=1\""}
#payment-service-1  | 2026-08-05T18:11:42.720582118Z [health] listening on :8080 (liveness only)
#payment-service-1  | 2026-08-05T18:11:42.752355318Z {"level":"INFO","timestamp":"2026-08-05T18:11:42.751Z","logger":"kafkajs","message":"[Consumer] Starting","groupId":"payment-service"}
#payment-service-1  | 2026-08-05T18:12:06.978769585Z {"level":"INFO","timestamp":"2026-08-05T18:12:06.978Z","logger":"kafkajs","message":"[ConsumerGroup] Consumer has joined the group","groupId":"payment-service","memberId":"payment-service-5dff0283-1f60-4b40-a5ae-e1eedba239ca","leaderId":"payment-service-5dff0283-1f60-4b40-a5ae-e1eedba239ca","isLeader":true,"memberAssignment":{"payment-commands":[0]},"groupProtocol":"RoundRobinAssigner","duration":24225}
#payment-service-1  | 2026-08-05T18:12:06.980000270Z Payment Service consuming payment-commands (gateway failure mode: recovering)
#payment-service-1  | 2026-08-05T18:16:47.167849977Z [retry] charge 5b244f05-355e-4df5-88ac-dcf5824ba4ad: attempt 1 failed (mock gateway timeout), retrying in 678ms
#payment-service-1  | 2026-08-05T18:16:47.896273615Z [retry] charge 5b244f05-355e-4df5-88ac-dcf5824ba4ad: attempt 2 failed (mock gateway timeout), retrying in 1372ms
#payment-service-1  | 2026-08-05T18:16:49.317706249Z [retry] charge 5b244f05-355e-4df5-88ac-dcf5824ba4ad: attempt 3 failed (mock gateway timeout), retrying in 2517ms
#payment-service-1  | 2026-08-05T18:16:51.908668183Z [payment] -> DLQ: order 5b244f05-355e-4df5-88ac-dcf5824ba4ad — mock gateway timeout
#payment-service-1  | 2026-08-05T18:16:51.978321400Z [circuit] CLOSED -> OPEN
#payment-service-1  | 2026-08-05T18:16:51.978448946Z [retry] charge 3743f707-ef49-45bd-9ea8-fd7b20552c7b: attempt 1 failed (mock gateway timeout), retrying in 660ms
#payment-service-1  | 2026-08-05T18:16:52.639689332Z [payment] -> DLQ: order 3743f707-ef49-45bd-9ea8-fd7b20552c7b — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.642046410Z [payment] -> DLQ: order acb6bd0f-05e9-4540-8d9e-01a3000afbc6 — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.645074003Z [payment] -> DLQ: order 2ef738db-3f7f-4b45-b258-777cdd06ea23 — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.647748978Z [payment] -> DLQ: order 57e43db3-6d43-4dd5-8cff-555631b01034 — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.650180838Z [payment] -> DLQ: order 8b2d7cd0-07e7-4e8f-8a58-7426269cf523 — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.652572906Z [payment] -> DLQ: order d05e132e-2dc1-412d-a7a2-8471984a8588 — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.655301322Z [payment] -> DLQ: order 68ca7b05-17ad-4a7b-84bd-60a9029956ae — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.658561109Z [payment] -> DLQ: order 7c0da904-9e6b-42e9-a259-9fe0475e64de — Circuit breaker is OPEN — call rejected without attempting it
#payment-service-1  | 2026-08-05T18:16:52.663126173Z [payment] -> DLQ: order 89b6b48d-56af-447d-b9bf-aa5c6227f49e — Circuit breaker is OPEN — call rejected without attempting it

# Verify Step 4 — Confirm OPEN is actively rejecting (optional but convincing):
docker compose exec order-service node scripts/floodPayments.js 1