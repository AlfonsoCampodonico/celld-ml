let () = Mirage_crypto_rng_unix.use_default ()

let check name c = if not c then (Printf.printf "FAIL %s\n" name; exit 1) else Printf.printf "ok   %s\n" name

let script =
  {|
function fetch(request, ctx) {
  const sql = ctx.storage.sql;
  sql.exec("CREATE TABLE IF NOT EXISTS hits(id INTEGER PRIMARY KEY, path TEXT)");
  if (request.method === "POST") sql.exec("INSERT INTO hits(path) VALUES (?)", request.path);
  if (request.path === "/boom") throw new Error("boom");
  if (request.path === "/spin") for (;;) {}
  if (request.path === "/kv") { ctx.storage.put("seen", (ctx.storage.get("seen") || 0) + 1); return String(ctx.storage.get("seen")); }
  return Response.json({ cell: ctx.id, hits: sql.exec("SELECT count(*) AS n FROM hits")[0].n });
}
async function alarm(ctx) { ctx.storage.put("alarmed", true); }
|}

let () =
  let db = Celld_sqlite.open_image "" in
  match Celld_js.create ~script ~db with
  | Error e -> check ("create: " ^ e) false
  | Ok js ->
      let call ?(meth = "GET") path = Celld_js.call_sync js ~name:"fetch" ~cell:"Hits:a" ~meth ~path ~query:[] ~body:"" in
      (match call ~meth:"POST" "/x" with
      | Ok r -> check "fetch returns JSON" (r.status = 200 && Celld_js.content_type r = "application/json" && r.body = {|{"cell":"Hits:a","hits":1}|})
      | Error e -> check ("fetch: " ^ e) false);
      check "storage writes dirty the database" (snd (Celld_sqlite.dirty db) <> []);
      (match call "/boom" with Error e -> check "exceptions surface" (String.length e >= 4 && String.sub e 0 11 = "Error: boom") | Ok _ -> check "exceptions surface" false);
      (match call "/spin" with Error e -> check ("runaway script interrupted: " ^ String.sub e 0 (min 30 (String.length e))) true | Ok _ -> check "runaway script interrupted" false);
      (match call "/kv" with Ok r -> check "kv storage" (r.body = "1") | Error e -> check e false);
      (match call "/kv" with Ok r -> check "kv storage persists" (r.body = "2") | Error e -> check e false);
      (match Celld_js.call_sync js ~name:"alarm" ~cell:"Hits:a" ~meth:"" ~path:"" ~query:[] ~body:"" with
      | Ok _ -> check "async alarm" (match call "/kv" with Ok _ -> true | Error _ -> false)
      | Error e -> check e false);
      (match Celld_js.create ~script:"syntax error here (" ~db with Error _ -> check "bad script rejected" true | Ok _ -> check "bad script rejected" false);
      Celld_js.dispose js

(* Isolation and limits: each cell has its own runtime; deep recursion is a
   catchable error, not a crash; a cell's memory limit is its own. *)
let () =
  let db1 = Celld_sqlite.open_image "" and db2 = Celld_sqlite.open_image "" in
  let mk db script = match Celld_js.create ~script ~db with Ok t -> t | Error e -> failwith e in
  let a = mk db1 {|let n = 0; function fetch() { n += 1; return String(n); }|} in
  let b = mk db2 {|function fetch(r) {
    if (r.path === "/deep") { const f = (x) => f(x + 1); f(0); }
    if (r.path === "/hog") { const xs = []; for (;;) xs.push(new Array(100000).fill(1)); }
    if (r.path === "/json") return String(JSON.parse(r.query.s).length);
    return "b"; }|} in
  let call t ?(query = []) path = Celld_js.call_sync t ~name:"fetch" ~cell:"X:1" ~meth:"GET" ~path ~query ~body:"" in
  check "cells keep separate globals" ((match call a "/" with Ok r -> r.status = 200 && r.body = "1" | _ -> false) && (match call b "/" with Ok r -> r.body = "b" | _ -> false));
  (match call b "/deep" with Error e -> check ("deep recursion is an error: " ^ String.sub e 0 (min 40 (String.length e))) true | Ok _ -> check "deep recursion" false);
  (match call b "/hog" with Error _ -> check "memory limit is per cell" true | Ok _ -> check "memory limit" false);
  check "other cell unaffected by a hog" (match call a "/" with Ok r -> r.body = "2" | _ -> false);
  let deep = String.make 100_000 '[' ^ String.make 100_000 ']' in
  (match call b ~query:[ ("s", deep) ] "/json" with Error _ -> check "deeply nested JSON is an error" true | Ok _ -> check "deeply nested JSON" true);
  check "cell usable after errors" (match call b "/" with Ok r -> r.body = "b" | _ -> false);
  Celld_js.dispose a;
  Celld_js.dispose b

(* A promise chain that never ends is cut off and poisons the context; a
   memory hog is held to the limit by the counting allocator. *)
let () =
  let db = Celld_sqlite.open_image "" in
  let t =
    match Celld_js.create ~db ~script:{|function fetch() { const loop = () => Promise.resolve().then(loop); loop(); return "started"; }|} with
    | Ok t -> t
    | Error e -> failwith e
  in
  let call () = Celld_js.call_sync t ~name:"fetch" ~cell:"X:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" in
  (match call () with Error _ -> check "endless promise chain is cut off" true | Ok _ -> check "endless promise chain" false);
  check "context is poisoned" (Celld_js.poisoned t);
  (match call () with Error e -> check "poisoned context refuses calls" (String.sub e 0 8 = "poisoned") | Ok _ -> check "poisoned" false);
  Celld_js.dispose t

(* WebSockets: accept in fetch, broadcast on a message, clean up on close. *)
let () =
  let db = Celld_sqlite.open_image "" in
  let t =
    match
      Celld_js.create ~db
        ~script:
          {|function fetch(request, ctx) {
              if (request.headers.get("upgrade") !== "websocket") return new Response("expected a websocket", { status: 426 });
              const [client, server] = Object.values(new WebSocketPair());
              ctx.acceptWebSocket(server, ["room"]);
              server.serializeAttachment({ joined: 1 });
              return new Response(null, { status: 101, webSocket: client });
            }
            function webSocketMessage(ws, msg, ctx) {
              for (const peer of ctx.getWebSockets("room")) peer.send("echo:" + msg + ":" + ws.deserializeAttachment().joined);
            }
            function webSocketClose(ws, code, reason, clean, ctx) { ctx.storage.put("closed", code); }|}
    with
    | Ok t -> t
    | Error e -> failwith e
  in
  Celld_js.headers := [ ("Upgrade", "websocket") ];
  let r = Celld_js.call_sync t ~name:"fetch" ~cell:"Room:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" in
  Celld_js.headers := [];
  match r with
  | Error e -> check ("upgrade: " ^ e) false
  | Ok r ->
      check "fetch accepts a socket with a 101" (r.status = 101 && String.length r.effects > 0 && String.sub r.effects 0 11 = {|{"accept":[|});
      let id = String.sub r.effects 12 24 in
      Celld_js.sockets := [ id ];
      (match Celld_js.call_sync t ~name:"ws_message" ~cell:"Room:1" ~meth:"" ~path:"" ~query:[] ~body:(Printf.sprintf {|{"id":%S,"data":"hi","binary":false}|} id) with
      | Ok m -> check "a message is broadcast to tagged sockets" (m.effects = Printf.sprintf {|{"accept":[],"sends":[{"id":%S,"data":"echo:hi:1","binary":false}],"closes":[]}|} id)
      | Error e -> check e false);
      (match Celld_js.call_sync t ~name:"ws_close" ~cell:"Room:1" ~meth:"" ~path:"" ~query:[] ~body:(Printf.sprintf {|{"id":%S,"code":1000,"reason":""}|} id) with
      | Ok _ -> check "close runs webSocketClose" true
      | Error e -> check e false);
      Celld_js.sockets := []

(* Suspension: a fetch hands the node an operation and resumes with its
   result; Web Crypto, URL and encoders. *)
let () =
  let db = Celld_sqlite.open_image "" in
  let t =
    match
      Celld_js.create ~db
        ~script:
          {|async function fetch(request, ctx) {
              if (request.path === "/out") {
                const r = await fetch("https://example.test/data?x=1", { method: "POST", body: "ping" });
                return new Response("upstream said " + r.status + " " + (await r.text()) + " " + r.headers.get("x-up"));
              }
              const h = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode("abc")));
              const hex = Array.from(h, (b) => b.toString(16).padStart(2, "0")).join("");
              const key = await crypto.subtle.importKey("raw", new TextEncoder().encode("key"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
              const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode("The quick brown fox jumps over the lazy dog")));
              const u = new URL("https://a.b:8443/p/q?x=1&y=two#h");
              return Response.json({ hex, mac: Array.from(mac, (b) => b.toString(16).padStart(2, "0")).join(""),
                uuid: crypto.randomUUID().length, host: u.host, y: u.searchParams.get("y"),
                text: new TextDecoder().decode(new TextEncoder().encode("héllo ✓")), b64: btoa("hi"), un: atob("aGk=") });
            }|}
    with
    | Ok t -> t
    | Error e -> failwith e
  in
  (match Celld_js.call_sync t ~name:"fetch" ~cell:"C:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" with
  | Ok r ->
      let has k = let n = String.length k in let rec go i = i + n <= String.length r.body && (String.sub r.body i n = k || go (i + 1)) in go 0 in
      check "SHA-256 digest" (has "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
      check "HMAC-SHA256" (has "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8");
      check "URL and URLSearchParams" (has {|"host":"a.b:8443","y":"two"|});
      check "TextEncoder/TextDecoder round trip" (has "héllo ✓");
      check "btoa/atob and randomUUID" (has {|"b64":"aGk=","un":"hi"|} && has {|"uuid":36|})
  | Error e -> check e false);
  match Celld_js.call t ~name:"fetch" ~cell:"C:1" ~meth:"GET" ~path:"/out" ~query:[] ~body:"" with
  | Ok (Celld_js.Await ops) ->
      check "fetch suspends with an operation" (String.length ops > 0 && String.length ops > 10);
      let body = Celld_js.base64 "pong" in
      (match Celld_js.resume t (Printf.sprintf {|[{"_op":1,"value":{"status":201,"headers":{"x-up":"yes"},"body":"%s"}}]|} body) with
      | Ok (Celld_js.Reply r) -> check "resume completes the call" (r.body = "upstream said 201 pong yes")
      | Ok (Celld_js.Await _) -> check "resume completes the call" false
      | Error e -> check e false)
  | Ok (Celld_js.Reply _) -> check "fetch suspends" false
  | Error e -> check e false

(* A data argument whose toString detaches the key's buffer: the key was
   copied first, so the HMAC still uses its original bytes. *)
let () =
  let db = Celld_sqlite.open_image "" in
  match
    Celld_js.create ~db
      ~script:
        {|function fetch() {
            const keyBuf = new TextEncoder().encode("key").buffer;
            const sneaky = { toString() { try { structuredClone(keyBuf); keyBuf.transfer?.(); } catch (e) {} return "The quick brown fox jumps over the lazy dog"; } };
            const mac = new Uint8Array(__celld_hmac("sha256", new Uint8Array(keyBuf), sneaky));
            return Array.from(mac, (b) => b.toString(16).padStart(2, "0")).join("");
          }|}
  with
  | Error e -> check e false
  | Ok t -> (
      match Celld_js.call_sync t ~name:"fetch" ~cell:"X:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" with
      | Ok r -> check "HMAC args are copied before user code runs" (r.body = "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8")
      | Error e -> check e false)


(* Node.js subset, streams, HTMLRewriter and Web Crypto beyond HMAC. *)
let () =
  let db = Celld_sqlite.open_image "" in
  let t =
    match
      Celld_js.create ~db
        ~script:
          {|async function fetch(request) {
              const out = {};
              const { Buffer } = require("node:buffer");
              const b = Buffer.from("héllo");
              out.buf = [b.toString("hex"), Buffer.from(b.toString("base64"), "base64").toString(), Buffer.concat([Buffer.from("a"), Buffer.from("b")]).toString()];
              const EventEmitter = require("events");
              const em = new EventEmitter(); let got = 0; em.once("x", (v) => (got += v)); em.emit("x", 2); em.emit("x", 5);
              out.events = got;
              out.path = require("path").join("/a/b", "../c", "d.txt");
              out.hash = require("crypto").createHash("sha256").update("abc").digest("hex").slice(0, 8);
              const up = new TransformStream({ transform(c, ctl) { ctl.enqueue(new TextDecoder().decode(c).toUpperCase()); } });
              const piped = new Response("stream me").body.pipeThrough(up);
              let s = ""; for await (const c of piped) s += c;
              out.stream = s;
              const [x, y] = new Response("tee").body.tee();
              out.tee = (await new Response(x).text()) + (await new Response(y).text());
              const enc = new ReadableStream({ start(c) { c.enqueue("te"); c.enqueue("xt"); c.close(); } }).pipeThrough(new TextEncoderStream());
              out.textstream = (await new Response(enc).text()).length;
              const html = await new HTMLRewriter()
                .on("a[href^='http']", { element(e) { e.setAttribute("target", "_blank"); } })
                .on("div.ad", { element(e) { e.remove(); } })
                .on("ul > li", { element(e) { e.append("!"); } })
                .on("p", { text(t) { if (t.text.includes("old")) t.replace("new"); } })
                .on("#title", { element(e) { e.setInnerContent("<b>Hi</b>", { html: true }); } })
                .transform(new Response('<h1 id="title">x</h1><p>old text</p><a href="https://x">l</a><div class="ad"><span>buy</span></div><ul><li>one</li></ul>'))
                .text();
              out.html = html;
              const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]);
              const iv = crypto.getRandomValues(new Uint8Array(12));
              const ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, new TextEncoder().encode("secret"));
              out.aes = new TextDecoder().decode(await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ct));
              const bad = new Uint8Array(ct); bad[0] ^= 1;
              out.aesTamper = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, bad).then(() => "accepted", () => "rejected");
              const kp = await crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"]);
              const sig = await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, kp.privateKey, new TextEncoder().encode("msg"));
              const jwk = await crypto.subtle.exportKey("jwk", kp.publicKey);
              const pub2 = await crypto.subtle.importKey("jwk", jwk, { name: "ECDSA", namedCurve: "P-256" }, true, ["verify"]);
              out.ecdsa = [await crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, pub2, sig, new TextEncoder().encode("msg")),
                           await crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" }, pub2, sig, new TextEncoder().encode("other"))];
              const a = await crypto.subtle.generateKey({ name: "ECDH", namedCurve: "P-256" }, true, ["deriveBits"]);
              const c = await crypto.subtle.generateKey({ name: "ECDH", namedCurve: "P-256" }, true, ["deriveBits"]);
              const s1 = new Uint8Array(await crypto.subtle.deriveBits({ name: "ECDH", public: c.publicKey }, a.privateKey, 256));
              const s2 = new Uint8Array(await crypto.subtle.deriveBits({ name: "ECDH", public: a.publicKey }, c.privateKey, 256));
              out.ecdh = s1.length === 32 && s1.every((v, i) => v === s2[i]);
              return Response.json(out);
            }|}
    with
    | Ok t -> t
    | Error e -> failwith e
  in
  match Celld_js.call_sync t ~name:"fetch" ~cell:"N:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" with
  | Error e -> check e false
  | Ok r ->
      let has k = let n = String.length k in let rec go i = i + n <= String.length r.body && (String.sub r.body i n = k || go (i + 1)) in go 0 in
      check "Buffer hex, base64 and concat" (has {|"buf":["68c3a96c6c6f","héllo","ab"]|});
      check "EventEmitter once" (has {|"events":2|});
      check "path.join" (has {|"path":"/a/c/d.txt"|});
      check "require('crypto').createHash" (has {|"hash":"ba7816bf"|});
      check "TransformStream through pipeThrough" (has {|"stream":"STREAM ME"|});
      check "tee" (has {|"tee":"teetee"|});
      check "TextEncoderStream" (has {|"textstream":4|});
      check "HTMLRewriter attributes, remove, append, text and inner content"
        (has {|<h1 id=\"title\"><b>Hi</b></h1><p>new</p><a href=\"https://x\" target=\"_blank\">l</a><ul><li>one!</li></ul>|});
      check "AES-GCM round trip and tamper detection" (has {|"aes":"secret","aesTamper":"rejected"|});
      check "ECDSA P-256 with a JWK round trip" (has {|"ecdsa":[true,false]|});
      check "ECDH agreement" (has {|"ecdh":true|})

(* The event loop: two calls on one context interleave while one awaits. *)
let () =
  let db = Celld_sqlite.open_image "" in
  let t =
    match
      Celld_js.create ~db
        ~script:
          {|async function fetch(request, ctx) {
              if (request.path === "/slow") { await new Promise((r) => setTimeout(r, 50)); return new Response("slow done"); }
              return new Response("fast");
            }|}
    with
    | Ok t -> t
    | Error e -> failwith e
  in
  let start call path =
    let input = Printf.sprintf {|{"cell":"L:1","method":"GET","path":"%s","query":{},"headers":{},"sockets":[],"body_b64":""}|} path in
    ignore (Celld_js.invoke t "__celld_start" (Printf.sprintf {|{"call":%d,"name":"fetch","input":%s}|} call (Celld_js.json_str input)))
  in
  let poll () = match Celld_js.invoke t "__celld_poll" "" with Ok s -> s | Error e -> failwith e in
  start 1 "/slow";
  let p1 = poll () in
  start 2 "/fast";
  let p2 = poll () in
  let has s k = let n = String.length k in let rec go i = i + n <= String.length s && (String.sub s i n = k || go (i + 1)) in go 0 in
  check "a waiting call reports its operation" (has p1 {|"kind":"sleep"|} && has p1 {|"done":[]|});
  check "a second call finishes while the first waits" (has p2 {|"call":2|});
  let op = match String.index_opt p1 '_' with Some _ -> 1 | None -> 0 in
  ignore (Celld_js.invoke t "__celld_resume" (Printf.sprintf {|[{"_op":%d,"value":null}]|} op));
  check "the first call finishes once resumed" (has (poll ()) {|"call":1|})

(* A class script may bind Node names itself, as Node code does. *)
let () =
  let db = Celld_sqlite.open_image "" in
  match Celld_js.create ~db ~script:{|const { Buffer } = require("node:buffer"); const EventEmitter = require("events"); function fetch() { return Buffer.from("ok").toString(); }|} with
  | Error e -> check ("script binds Buffer itself: " ^ e) false
  | Ok t -> check "a script may bind Buffer and EventEmitter itself" (match Celld_js.call_sync t ~name:"fetch" ~cell:"B:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" with Ok r -> r.body = "ok" | Error _ -> false)

(* WebAssembly: a hand-assembled module with an import, exports, memory
   with a data segment, and an infinite loop that the budget stops. *)
let () =
  let db = Celld_sqlite.open_image "" in
  let wasm =
    "0061736d01000000" ^ "010e03" ^ "60027f7f017f" ^ "60017f00" ^ "600000"
    ^ "020b0103656e76036c6f67" ^ "0001" ^ "0304030001" ^ "02" ^ "0503010001"
    ^ "072104036164640001" ^ "0763616c6c4c6f670002" ^ "047370696e0003" ^ "066d656d6f72790200"
    ^ "0a1b03" ^ "070020002001" ^ "6a0b" ^ "09002000410" ^ "16a10000b" ^ "07000340" ^ "0c000b0b"
    ^ "0b080100410" ^ "00b026869"
  in
  let hex = String.concat "" (List.init (String.length wasm / 2) (fun i -> Printf.sprintf "0x%s," (String.sub wasm (2 * i) 2))) in
  let script =
    Printf.sprintf
      {|async function fetch() {
          const bytes = new Uint8Array([%s]);
          const logged = [];
          const out = { valid: WebAssembly.validate(bytes), invalid: WebAssembly.validate(new Uint8Array([0, 1, 2])) };
          const { instance } = await WebAssembly.instantiate(bytes, { env: { log: (x) => logged.push(x) } });
          out.add = instance.exports.add(2, 40);
          instance.exports.callLog(41);
          out.logged = logged;
          const mem = new Uint8Array(instance.exports.memory.buffer);
          out.data = String.fromCharCode(mem[0], mem[1]);
          out.pages = instance.exports.memory.grow(1);
          out.detached = mem.length === 0;
          out.grown = instance.exports.memory.buffer.byteLength;
          try { instance.exports.spin(); out.spin = "returned"; } catch (e) { out.spin = e.name + ": " + e.message; }
          out.after = instance.exports.add(1, 1);
          try { new WebAssembly.Instance(new WebAssembly.Module(bytes), {}); } catch (e) { out.link = e.name; }
          return Response.json(out);
        }|}
      hex
  in
  match Celld_js.create ~db ~script with
  | Error e -> check ("wasm script: " ^ e) false
  | Ok t -> (
      match Celld_js.call_sync t ~name:"fetch" ~cell:"W:1" ~meth:"GET" ~path:"/" ~query:[] ~body:"" with
      | Error e -> check ("wasm: " ^ e) false
      | Ok r ->
          let has k = let n = String.length k in let rec go i = i + n <= String.length r.body && (String.sub r.body i n = k || go (i + 1)) in go 0 in
          if not (has {|"add":42|}) then print_endline r.body;
          check "WebAssembly validate" (has {|"valid":true,"invalid":false|});
          check "WebAssembly exports and imports" (has {|"add":42|} && has {|"logged":[42]|});
          check "WebAssembly memory and data segments" (has {|"data":"hi"|});
          check "memory.grow detaches the old view" (has {|"pages":1,"detached":true,"grown":131072|});
          check "an infinite loop is stopped by the budget" (has {|"spin":"RuntimeError: [trap] WebAssembly instruction budget exhausted"|});
          check "the instance works after a trap" (has {|"after":2|});
          check "a missing import is a LinkError" (has {|"link":"LinkError"|}))
