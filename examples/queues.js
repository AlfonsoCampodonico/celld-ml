// Orders: a request enqueues a job; the same class consumes the queue.
// Deploy:
//   curl -X PUT --data-binary @examples/queues.js http://10.0.0.11:8080/.well-known/celld/classes/Orders
//   curl -X PUT -d '{"consumer":"Orders","max_batch_size":5,"max_batch_timeout_ms":1000,"max_retries":2,"dead_letter_queue":"orders-dlq"}' \
//        http://10.0.0.11:8080/.well-known/celld/queues/orders
// Use:
//   curl -X POST http://10.0.0.12:8080/do/Orders:shop/order/42      # queued with the write
//   curl -X POST http://10.0.0.12:8080/do/Orders:shop/order/fail    # retried, then dead-lettered
//   curl http://10.0.0.13:8080/.well-known/celld/queues/orders-dlq
function fetch(request, ctx) {
  const m = request.path.match(/^\/order\/(.+)$/);
  if (m && request.method === "POST") {
    ctx.storage.put("orders_placed", (ctx.storage.get("orders_placed") ?? 0) + 1);
    ctx.queue("orders").send({ order: m[1], placed: Date.now() });
    return Response.json({ queued: m[1] });
  }
  return Response.json({ placed: ctx.storage.get("orders_placed") ?? 0 });
}

// Runs in Orders:__consumer, one batch at a time.
function queue(batch, ctx) {
  for (const msg of batch.messages) {
    if (msg.body.order === "fail") throw new Error("payment provider down");
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS shipped(id TEXT PRIMARY KEY, attempts INTEGER)");
    ctx.storage.sql.exec("INSERT OR REPLACE INTO shipped VALUES (?, ?)", msg.body.order, msg.attempts);
    msg.ack();
  }
}
