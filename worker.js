import { DurableObject } from "cloudflare:workers";

const MATCH_SIZE = 2;

export class Lobby extends DurableObject {
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ matched: false });

    this.tryMatch();
    return new Response(null, { status: 101, webSocket: client });
  }

  waiting() {
    return this.ctx
      .getWebSockets()
      .filter((ws) => ws.readyState === 1 && !ws.deserializeAttachment()?.matched);
  }

  tryMatch() {
    let queue = this.waiting();

    while (queue.length >= MATCH_SIZE) {
      const group = queue.slice(0, MATCH_SIZE);
      const room = crypto.randomUUID();
      group.forEach((ws, i) => {
        ws.serializeAttachment({ matched: true });
        ws.send(JSON.stringify({ type: "matched", room, slot: i }));
        ws.close(1000, "matched");
      });
      queue = queue.slice(MATCH_SIZE);
    }

    queue.forEach((ws) =>
      ws.send(JSON.stringify({ type: "waiting", count: queue.length, need: MATCH_SIZE }))
    );
  }

  webSocketClose(ws) {
    try { ws.close(); } catch {}
    this.tryMatch();
  }

  webSocketError(ws) {
    this.webSocketClose(ws);
  }

  webSocketMessage() {}
}

export class GameRoom extends DurableObject {
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }
    if (this.ctx.getWebSockets().length >= MATCH_SIZE) {
      return new Response("Room is full", { status: 403 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const id = crypto.randomUUID().slice(0, 8);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id });

    const others = this.ctx
      .getWebSockets()
      .filter((ws) => ws !== server)
      .map((ws) => ws.deserializeAttachment()?.id);
    server.send(JSON.stringify({ type: "welcome", id, players: others }));
    this.broadcast({ type: "join", id }, server);

    if (this.ctx.getWebSockets().length === MATCH_SIZE) {
      this.broadcast({ type: "start" });
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
    const { id } = ws.deserializeAttachment();
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      return;
    }
    this.broadcast({ type: "msg", from: id, data }, ws);
  }

  webSocketClose(ws) {
    const { id } = ws.deserializeAttachment() || {};
    try { ws.close(); } catch {}
    this.broadcast({ type: "leave", id }, ws);
  }

  webSocketError(ws) {
    this.webSocketClose(ws);
  }

  broadcast(obj, except) {
    const text = JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets()) {
      if (ws !== except) {
        try { ws.send(text); } catch {}
      }
    }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/match") {
      const stub = env.LOBBY.get(env.LOBBY.idFromName("global"));
      return stub.fetch(request);
    }

    if (url.pathname === "/ws") {
      const room = url.searchParams.get("room");
      if (!room) return new Response("Missing room", { status: 400 });
      const stub = env.GAME_ROOM.get(env.GAME_ROOM.idFromName(room));
      return stub.fetch(request);
    }

    return new Response("Game server is running");
  },
};
