// ============================================================
// ポカポカひらがなタイピング - オンライン対戦サーバー
// Cloudflare Workers + Durable Objects
// ============================================================
//
// エンドポイント:
//   wss://<your-worker>.workers.dev/ws
//
// クライアント -> サーバー メッセージ:
//   { action: 'match', mode: 'kana' | 'roma' }
//   { action: 'sync_preview', text: string }
//   { action: 'submit', status: 'correct' | 'incorrect' | 'skip', time: number }
//
// サーバー -> クライアント メッセージ:
//   { type: 'welcome', id }                                            接続直後に送信される自分のID
//   { type: 'matched', opponentId, opponentName, firstTurnPlayerId }
//   { type: 'next_turn', round, turnPlayerId, word: {kana, roma} }
//   { type: 'sync_preview', text }
//   { type: 'turn_result', playerId, status, points,
//     scores: { player1Id, player1, player2Id, player2 } }
//   { type: 'game_over', player1Id, player2Id, score1, score2 }
//
// ============================================================

const WORD_LIST = [
  { kana: "いぬ", roma: "inu" },
  { kana: "ねこ", roma: "neko" },
  { kana: "うさぎ", roma: "usagi" },
  { kana: "きりん", roma: "kirin" },
  { kana: "らいおん", roma: "raion" },
  { kana: "くま", roma: "kuma" },
  { kana: "ぱんだ", roma: "panda" },
  { kana: "りす", roma: "risu" },
  { kana: "ぞう", roma: "zou" },
  { kana: "きつね", roma: "kitsune" },
  { kana: "りんご", roma: "ringo" },
  { kana: "おにぎり", roma: "onigiri" },
  { kana: "らーめん", roma: "raamen" },
  { kana: "すし", roma: "sushi" },
  { kana: "めろんぱん", roma: "meronpan" },
  { kana: "いちご", roma: "ichigo" },
  { kana: "てんぷら", roma: "tenpura" },
  { kana: "うどん", roma: "udon" },
  { kana: "ぎょうざ", roma: "gyouza" },
  { kana: "ぷりん", roma: "purin" },
  { kana: "つくえ", roma: "tsukue" },
  { kana: "えんぴつ", roma: "enpitsu" },
  { kana: "こうえん", roma: "kouen" },
  { kana: "てれび", roma: "terebi" },
  { kana: "じてんしゃ", roma: "jitensha" },
  { kana: "がっこう", roma: "gakkou" },
  { kana: "でんしゃ", roma: "densha" },
  { kana: "かばん", roma: "kaban" },
  { kana: "とけい", roma: "tokei" },
  { kana: "ほうき", roma: "houki" },
];

const CUTE_NAME_PARTS = ["こむぎ", "みかん", "そら", "ひなた", "つき", "うみ", "もも", "さくら", "こゆき", "はる"];

function randomWord() {
  return WORD_LIST[Math.floor(Math.random() * WORD_LIST.length)];
}

function randomName() {
  const p = CUTE_NAME_PARTS[Math.floor(Math.random() * CUTE_NAME_PARTS.length)];
  const n = Math.floor(Math.random() * 900) + 100;
  return `${p}${n}`;
}

function scoreForSubmit(status, time) {
  if (status !== "correct") return 0;
  // 速く正解するほど高得点。最低10点、最大150点。
  const raw = Math.round(150 - time * 10);
  return Math.max(10, raw);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected WebSocket upgrade", { status: 426 });
      }
      // 全プレイヤーを単一の Arena Durable Object に集約してマッチメイキングする
      const id = env.ARENA.idFromName("global-arena");
      const stub = env.ARENA.get(id);
      return stub.fetch(request);
    }

    return new Response("typing game realtime server", { status: 200 });
  },
};

export class Arena {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    // メモリ上の状態（この Durable Object インスタンスが生きている間だけ保持）
    this.sockets = new Map();  // playerId -> WebSocket
    this.waiting = new Map();  // mode -> playerId (待機中のプレイヤー、モードごとに1人まで)
    this.rooms = new Map();    // playerId -> room object (room は player1/player2 双方から同じ参照を持つ)
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.accept();
    const playerId = crypto.randomUUID();
    this.sockets.set(playerId, server);

    server.send(JSON.stringify({ type: "welcome", id: playerId }));

    server.addEventListener("message", (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch (e) {
        return;
      }
      this.handleMessage(playerId, msg);
    });

    const onClose = () => this.handleDisconnect(playerId);
    server.addEventListener("close", onClose);
    server.addEventListener("error", onClose);

    return new Response(null, { status: 101, webSocket: client });
  }

  send(playerId, payload) {
    const ws = this.sockets.get(playerId);
    if (ws && ws.readyState === WebSocket.READY_STATE_OPEN) {
      try {
        ws.send(JSON.stringify(payload));
      } catch (e) {
        // ignore
      }
    }
  }

  handleMessage(playerId, msg) {
    switch (msg.action) {
      case "match":
        this.handleMatch(playerId, msg);
        break;
      case "sync_preview":
        this.handleSyncPreview(playerId, msg);
        break;
      case "submit":
        this.handleSubmit(playerId, msg);
        break;
    }
  }

  handleMatch(playerId, msg) {
    const mode = msg.mode === "roma" ? "roma" : "kana";
    const waitingId = this.waiting.get(mode);

    if (waitingId && waitingId !== playerId && this.sockets.has(waitingId)) {
      // 待機中の相手が見つかったのでマッチ成立
      this.waiting.delete(mode);

      const player1Id = waitingId;
      const player2Id = playerId;
      const firstTurnPlayerId = Math.random() < 0.5 ? player1Id : player2Id;

      const room = {
        mode,
        player1Id,
        player2Id,
        name1: randomName(),
        name2: randomName(),
        score1: 0,
        score2: 0,
        round: 0,
        turnPlayerId: firstTurnPlayerId,
        currentWord: null,
      };
      this.rooms.set(player1Id, room);
      this.rooms.set(player2Id, room);

      this.send(player1Id, {
        type: "matched",
        opponentId: player2Id,
        opponentName: room.name2,
        firstTurnPlayerId,
      });
      this.send(player2Id, {
        type: "matched",
        opponentId: player1Id,
        opponentName: room.name1,
        firstTurnPlayerId,
      });

      // 少し間を空けてゲーム開始（クライアント側の3,2,1カウントダウンに合わせる）
      setTimeout(() => this.startNextTurn(room), 3300);
    } else {
      // 相手がいなければ待機列に入る
      this.waiting.set(mode, playerId);
    }
  }

  startNextTurn(room) {
    if (room.round >= 10) {
      this.endGame(room);
      return;
    }
    room.round += 1;
    room.currentWord = randomWord();
    // ラウンドごとにターンを交代
    room.turnPlayerId =
      room.round === 1
        ? room.turnPlayerId
        : room.turnPlayerId === room.player1Id
        ? room.player2Id
        : room.player1Id;

    const payload = {
      type: "next_turn",
      round: room.round,
      turnPlayerId: room.turnPlayerId,
      word: room.currentWord,
    };
    this.send(room.player1Id, payload);
    this.send(room.player2Id, payload);
  }

  handleSyncPreview(playerId, msg) {
    const room = this.rooms.get(playerId);
    if (!room) return;
    if (room.turnPlayerId !== playerId) return; // 自分のターンでなければ無視

    const opponentId = playerId === room.player1Id ? room.player2Id : room.player1Id;
    this.send(opponentId, { type: "sync_preview", text: msg.text || "" });
  }

  handleSubmit(playerId, msg) {
    const room = this.rooms.get(playerId);
    if (!room) return;
    if (room.turnPlayerId !== playerId) return; // 自分のターンでなければ無視

    const status = ["correct", "incorrect", "skip"].includes(msg.status) ? msg.status : "incorrect";
    const time = typeof msg.time === "number" ? msg.time : 15;
    const points = scoreForSubmit(status, time);

    if (playerId === room.player1Id) {
      room.score1 += points;
    } else {
      room.score2 += points;
    }

    const resultPayload = {
      type: "turn_result",
      playerId,
      status,
      points,
      scores: {
        player1Id: room.player1Id,
        player1: room.score1,
        player2Id: room.player2Id,
        player2: room.score2,
      },
    };
    this.send(room.player1Id, resultPayload);
    this.send(room.player2Id, resultPayload);

    // 次のターンへ（結果表示のタイムラグを最小限に抑える）
    setTimeout(() => this.startNextTurn(room), 250);
  }

  endGame(room) {
    const payload = {
      type: "game_over",
      player1Id: room.player1Id,
      player2Id: room.player2Id,
      score1: room.score1,
      score2: room.score2,
    };
    this.send(room.player1Id, payload);
    this.send(room.player2Id, payload);

    this.rooms.delete(room.player1Id);
    this.rooms.delete(room.player2Id);
  }

  handleDisconnect(playerId) {
    this.sockets.delete(playerId);

    // 待機列から削除
    for (const [mode, id] of this.waiting.entries()) {
      if (id === playerId) this.waiting.delete(mode);
    }

    // 対戦中だった場合は相手に通知して部屋を破棄
    const room = this.rooms.get(playerId);
    if (room) {
      const opponentId = playerId === room.player1Id ? room.player2Id : room.player1Id;
      this.rooms.delete(room.player1Id);
      this.rooms.delete(room.player2Id);
      // 相手のソケットを閉じることで、クライアント側の socket.onclose ハンドラが
      // 「対戦相手が退室しました」エラーを表示する
      const ws = this.sockets.get(opponentId);
      if (ws) {
        try {
          ws.close(1000, "opponent_disconnected");
        } catch (e) {
          // ignore
        }
      }
    }
  }
}
