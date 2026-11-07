// A heartbeat that records each scheduled run.
// Deploy:
//   curl -X PUT --data-binary @examples/cron.js http://10.0.0.11:8080/.well-known/celld/classes/Heartbeat
//   printf '* * * * *\n' | curl -X PUT --data-binary @- http://10.0.0.11:8080/.well-known/celld/classes/Heartbeat/crons
// Read:
//   curl http://10.0.0.12:8080/do/Heartbeat:__cron/
function scheduled(controller, ctx) {
  ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS beats(at INTEGER, cron TEXT)");
  ctx.storage.sql.exec("INSERT INTO beats VALUES (?, ?)", controller.scheduledTime, controller.cron);
}

function fetch(request, ctx) {
  ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS beats(at INTEGER, cron TEXT)");
  return Response.json(ctx.storage.sql.exec("SELECT at, cron FROM beats ORDER BY at"));
}
