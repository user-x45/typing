// Cloudflare Workers code with WebSockets & Durable Objects
// Typing Game Multiplayer Room Hub

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Routing WebSocket upgrade requests to Durable Object
    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected Upgrade: websocket", { status: 426 });
      }

      // Generate a unique player ID for current session connection
      const playerId = crypto.randomUUID();
      const id = env.TYPING_BATTLE.idFromName("LOBBY");
      const stub = env.TYPING_BATTLE.get(id);

      // Forward request directly to Durable Object
      return stub.fetch(new Response(null, {
        headers: {
          "Upgrade": "websocket",
          "X-Player-ID": playerId
        }
      }));
    }

    return new Response("Typing Battle Server Online", { status: 200 });
  }
};

// Words list shared specifically on server logic
const BATTLE_WORDS = [
  { kana: "いぬ", roma: "inu" },
  { kana: "ねこ", roma: "neko" },
  { kana: "うさぎ", roma: "usagi" },
  { kana: "つくえ", roma: "tsukue" },
  { kana: "えんぴつ", roma: "enpitsu" },
  { kana: "おにぎり", roma: "onigiri" },
  { kana: "ぱそこん", roma: "pasokon" },
  { kana: "らいおん", roma: "raion" },
  { kana: "すし", roma: "sushi" },
  { kana: "じてんしゃ", roma: "jitensha" }
];

export class TypingBattle {
  constructor(state, env) {
    this.state = state;
    // Track matching queue
    this.waitingQueue = []; // [{ id, ws, name, mode }]
    this.rooms = new Map(); // roomId -> Room state object
  }

  async fetch(request) {
    const playerId = request.headers.get("X-Player-ID");
    const [client, server] = new WebSocketPair();

    await this.handleSession(server, playerId);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  async handleSession(ws, playerId) {
    ws.accept();

    const playerName = `タイピスト-${playerId.slice(0, 4).toUpperCase()}`;

    ws.on("message", (msgStr) => {
      try {
        const msg = JSON.parse(msgStr);
        this.processMessage(ws, playerId, playerName, msg);
      } catch (e) {
        console.error("Message parse failed: ", e);
      }
    });

    ws.on("close", () => {
      this.handlePlayerLeave(playerId);
    });

    ws.on("error", () => {
      this.handlePlayerLeave(playerId);
    });
  }

  processMessage(ws, playerId, playerName, msg) {
    if (msg.action === 'match') {
      // 1. Remove if already in queue
      this.waitingQueue = this.waitingQueue.filter(p => p.id !== playerId);

      // 2. Look for compatible opponent (same input mode preferred or any)
      const opponent = this.waitingQueue.find(p => p.mode === msg.mode);

      if (opponent) {
        // Match found!
        this.waitingQueue = this.waitingQueue.filter(p => p.id !== opponent.id);
        this.createRoom(playerId, ws, playerName, opponent.id, opponent.ws, opponent.name, msg.mode);
      } else {
        // Enqueue
        this.waitingQueue.push({
          id: playerId,
          ws: ws,
          name: playerName,
          mode: msg.mode
        });
      }
    }

    else if (msg.action === 'sync_preview') {
      // Broadcast current input characters directly to the opponent in real-time
      const room = this.findRoomByPlayer(playerId);
      if (room) {
        const opponentWs = (room.p1.id === playerId) ? room.p2.ws : room.p1.ws;
        opponentWs.send(JSON.stringify({
          type: 'sync_preview',
          text: msg.text
        }));
      }
    }

    else if (msg.action === 'submit') {
      // Verify turn and score the points
      const room = this.findRoomByPlayer(playerId);
      if (room && room.turnPlayerId === playerId) {
        this.handleTurnSubmission(room, playerId, msg.status, msg.time);
      }
    }
  }

  createRoom(p1Id, p1Ws, p1Name, p2Id, p2Ws, p2Name, mode) {
    const roomId = crypto.randomUUID();
    
    // Choose randomly who goes first
    const firstTurnPlayerId = Math.random() < 0.5 ? p1Id : p2Id;

    // Shuffle word list
    const roomWords = [...BATTLE_WORDS].sort(() => Math.random() - 0.5);

    const roomState = {
      id: roomId,
      p1: { id: p1Id, ws: p1Ws, name: p1Name, score: 0 },
      p2: { id: p2Id, ws: p2Ws, name: p2Name, score: 0 },
      words: roomWords,
      round: 1, // Max 10 turns total (5 turns each)
      turnPlayerId: firstTurnPlayerId,
      mode: mode
    };

    this.rooms.set(roomId, roomState);

    // Notify matched event
    p1Ws.send(JSON.stringify({
      type: 'matched',
      opponentId: p2Id,
      opponentName: p2Name,
      firstTurnPlayerId: firstTurnPlayerId
    }));

    p2Ws.send(JSON.stringify({
      type: 'matched',
      opponentId: p1Id,
      opponentName: p1Name,
      firstTurnPlayerId: firstTurnPlayerId
    }));

    // Trigger game loop
    this.sendNextTurn(roomState);
  }

  sendNextTurn(room) {
    const wordIndex = (room.round - 1) % room.words.length;
    const currentWord = room.words[wordIndex];

    const turnPayload = {
      type: 'next_turn',
      round: room.round,
      turnPlayerId: room.turnPlayerId,
      word: currentWord
    };

    room.p1.ws.send(JSON.stringify(turnPayload));
    room.p2.ws.send(JSON.stringify(turnPayload));
  }

  handleTurnSubmission(room, playerId, status, time) {
    let points = 0;
    const player = (room.p1.id === playerId) ? room.p1 : room.p2;

    if (status === 'correct') {
      // Calculate scores dynamically (speed bonus integrated)
      const baseScore = room.mode === 'roma' ? room.words[(room.round - 1) % room.words.length].roma.length * 100 : room.words[(room.round - 1) % room.words.length].kana.length * 100;
      const speedBonus = Math.max(0, Math.floor((15 - time) * 30));
      points = baseScore + speedBonus;
      player.score += points;
    } else if (status === 'skip') {
      points = 0;
    } else {
      // Minor deduction
      player.score = Math.max(0, player.score - 50);
      points = -50;
    }

    // Broadcast round result to both players
    const resultPayload = {
      type: 'turn_result',
      playerId: playerId,
      status: status,
      points: points,
      scores: {
        player1Id: room.p1.id,
        player1: room.p1.score,
        player2Id: room.p2.id,
        player2: room.p2.score
      }
    };

    room.p1.ws.send(JSON.stringify(resultPayload));
    room.p2.ws.send(JSON.stringify(resultPayload));

    // Transition turn
    room.round++;
    if (room.round > 10) {
      // End game
      this.endGame(room);
    } else {
      // Toggle active turn player
      room.turnPlayerId = (room.turnPlayerId === room.p1.id) ? room.p2.id : room.p1.id;
      // Delay slightly for visual pacing
      setTimeout(() => {
        this.sendNextTurn(room);
      }, 1500);
    }
  }

  endGame(room) {
    const endPayload = {
      type: 'game_over',
      player1Id: room.p1.id,
      score1: room.p1.score,
      player2Id: room.p2.id,
      score2: room.p2.score
    };

    room.p1.ws.send(JSON.stringify(endPayload));
    room.p2.ws.send(JSON.stringify(endPayload));

    // Destroy room
    this.rooms.delete(room.id);
  }

  handlePlayerLeave(playerId) {
    // 1. Remove from waiting queue if active
    this.waitingQueue = this.waitingQueue.filter(p => p.id !== playerId);

    // 2. Clear any active room and notify surviving opponent
    const room = this.findRoomByPlayer(playerId);
    if (room) {
      const opponent = (room.p1.id === playerId) ? room.p2 : room.p1;
      try {
        opponent.ws.send(JSON.stringify({
          type: 'opponent_left'
        }));
        opponent.ws.close();
      } catch (e) {}
      this.rooms.delete(room.id);
    }
  }

  findRoomByPlayer(playerId) {
    for (const room of this.rooms.values()) {
      if (room.p1.id === playerId || room.p2.id === playerId) {
        return room;
      }
    }
    return null;
  }
}
