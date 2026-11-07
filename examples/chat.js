// A chat room cell: messages in SQLite, a typing-indicator alarm.
// Deploy:  curl -X PUT --data-binary @examples/chat.js http://10.0.0.11:8080/.well-known/celld/classes/Chat
// Use:     curl -X POST --data '{"from":"ana","text":"hi"}' http://10.0.0.12:8080/do/Chat:lobby/messages
//          curl http://10.0.0.13:8080/do/Chat:lobby/messages
function fetch(request, ctx) {
  const sql = ctx.storage.sql;
  sql.exec(`CREATE TABLE IF NOT EXISTS messages(
              id INTEGER PRIMARY KEY, sender TEXT NOT NULL, body TEXT NOT NULL, at INTEGER NOT NULL)`);
  if (request.path === "/messages" && request.method === "POST") {
    return request.json().then((m) => {
      if (!m.from || !m.text) return new Response("from and text are required\n", { status: 400 });
      sql.exec("INSERT INTO messages(sender, body, at) VALUES (?, ?, ?)", m.from, m.text, Date.now());
      if (m.text === "boom") throw new Error("this message breaks the handler; the insert above is rolled back");
      ctx.storage.put("last_sender", m.from);
      return Response.json({ ok: true, count: sql.exec("SELECT count(*) AS n FROM messages")[0].n });
    });
  }
  if (request.path === "/messages") {
    return Response.json({
      room: ctx.id,
      last_sender: ctx.storage.get("last_sender") ?? null,
      pings: ctx.storage.get("pings") ?? 0,
      messages: sql.exec("SELECT sender, body FROM messages ORDER BY id"),
    });
  }
  return new Response("try GET or POST /messages\n", { status: 404 });
}

// Fired by PUT /do/Chat:<room>/__alarm?in_ms=N
function alarm(ctx) {
  ctx.storage.put("pings", (ctx.storage.get("pings") ?? 0) + 1);
}
