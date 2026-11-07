// A WebSocket chat room on the Durable Object hibernation API.
// Deploy:  curl -X PUT --data-binary @examples/room.js http://10.0.0.11:8080/.well-known/celld/classes/Room
// Connect: ws://10.0.0.12:8080/do/Room:lobby/ws?name=ana   (any node; the owner terminates the socket)
// History: curl http://10.0.0.13:8080/do/Room:lobby/history
function fetch(request, ctx) {
  ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS log(id INTEGER PRIMARY KEY, who TEXT, text TEXT)");
  if (request.path === "/ws") {
    if (request.headers.upgrade !== "websocket") return new Response("expected a websocket upgrade\n", { status: 426 });
    const [client, server] = Object.values(new WebSocketPair());
    ctx.acceptWebSocket(server, ["room"]);
    server.serializeAttachment({ name: request.query.name || "anon" });
    return new Response(null, { status: 101, webSocket: client });
  }
  if (request.path === "/history") return Response.json(ctx.storage.sql.exec("SELECT who, text FROM log ORDER BY id"));
  return new Response("try /ws or /history\n", { status: 404 });
}

// Socket events get ctx as their last argument.
function webSocketMessage(ws, message, ctx) {
  const who = ws.deserializeAttachment().name;
  ctx.storage.sql.exec("INSERT INTO log(who, text) VALUES (?, ?)", who, String(message));
  for (const peer of ctx.getWebSockets("room")) peer.send(who + ": " + message);
}

function webSocketClose(ws, code, reason, wasClean, ctx) {
  ctx.storage.sql.exec("INSERT INTO log(who, text) VALUES (?, ?)", ws.deserializeAttachment().name, "left (" + code + ")");
}
