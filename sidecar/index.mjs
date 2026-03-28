import net from 'node:net';
import { Readable } from 'node:stream';
import { Client, GatewayIntentBits } from 'discord.js';
import {
  joinVoiceChannel,
  VoiceConnectionStatus,
  entersState,
  createAudioPlayer,
  createAudioResource,
  StreamType,
  AudioPlayerStatus,
  NoSubscriberBehavior,
} from '@discordjs/voice';

// IPC message types — must match pkg/ipc/protocol.go
const MSG_AUDIO_FROM_DISCORD = 0x01;
const MSG_AUDIO_TO_DISCORD = 0x02;
const MSG_USER_JOIN = 0x03;
const MSG_USER_LEAVE = 0x04;
const MSG_READY = 0x05;
const MSG_SHUTDOWN = 0x06;

const SIDECAR_USER_ID = '0';

const SOCKET_PATH = process.env.IPC_SOCKET_PATH || '/tmp/discord-voice-bridge.sock';
const TOKEN = process.env.DISCORD_BOT_TOKEN;
const GUILD_ID = process.env.DISCORD_GUILD_ID;
const CHANNEL_ID = process.env.DISCORD_CHANNEL_ID;

if (!TOKEN || !GUILD_ID || !CHANNEL_ID) {
  console.error('[sidecar] set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID, DISCORD_CHANNEL_ID');
  process.exit(1);
}

// --- IPC Protocol ---

function writeMessage(socket, type, userId, payload) {
  const userBuf = Buffer.alloc(8);
  userBuf.writeBigUInt64LE(BigInt(userId));
  const payloadBuf = payload || Buffer.alloc(0);
  const lenBuf = Buffer.alloc(2);
  lenBuf.writeUInt16LE(payloadBuf.length);
  socket.write(Buffer.concat([Buffer.from([type]), userBuf, lenBuf, payloadBuf]));
}

class IPCReader {
  constructor(onMessage) {
    this.onMessage = onMessage;
    this.buffer = Buffer.alloc(0);
  }

  feed(data) {
    this.buffer = Buffer.concat([this.buffer, data]);
    while (this.buffer.length >= 11) {
      const payloadLen = this.buffer.readUInt16LE(9);
      const totalLen = 11 + payloadLen;
      if (this.buffer.length < totalLen) break;

      const type = this.buffer[0];
      const userId = this.buffer.readBigUInt64LE(1).toString();
      const payload = this.buffer.subarray(11, totalLen);

      this.onMessage(type, userId, payload);
      this.buffer = this.buffer.subarray(totalLen);
    }
  }
}

// --- Opus Stream for Discord Playback ---
// Readable stream that receives raw Opus frames and feeds them to discord.js AudioPlayer.

class OpusFrameStream extends Readable {
  constructor() {
    super({ objectMode: true });
    this._frames = [];
    this._reading = false;
  }

  pushFrame(opusFrame) {
    // Keep max 5 frames (~100ms) to bound latency — drop oldest if full
    while (this._frames.length >= 5) {
      this._frames.shift();
    }
    this._frames.push(opusFrame);
    if (this._reading) {
      this._flush();
    }
  }

  _flush() {
    while (this._frames.length > 0) {
      const frame = this._frames.shift();
      if (!this.push(frame)) {
        this._reading = false;
        return;
      }
    }
    this._reading = true;
  }

  _read() {
    this._reading = true;
    this._flush();
  }
}

// --- Main ---

let discordClient = null;
let ipcSocket = null;
let frameCount = 0;
let opusStream = null;

async function main() {
  discordClient = new Client({
    intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
  });

  // Connect to IPC socket
  let connected = false;
  await new Promise((resolve, reject) => {
    ipcSocket = net.createConnection(SOCKET_PATH, () => {
      connected = true;
      console.log('[sidecar] connected to IPC socket');
      resolve();
    });
    ipcSocket.on('error', (err) => {
      if (!connected) {
        reject(err);
      } else {
        console.error(`[sidecar] IPC socket error: ${err.message}`);
        cleanup();
      }
    });
  });

  // Set up Opus stream for Discord playback
  opusStream = new OpusFrameStream();

  let toDiscordFrames = 0;
  const ipcReader = new IPCReader((type, _userId, payload) => {
    if (type === MSG_SHUTDOWN) {
      console.log('[sidecar] received shutdown');
      cleanup();
    } else if (type === MSG_AUDIO_TO_DISCORD) {
      toDiscordFrames++;
      if (toDiscordFrames % 250 === 1) {
        console.log(`[sidecar] LK→Discord opus frame: ${payload.length} bytes, total=${toDiscordFrames}`);
      }
      opusStream.pushFrame(Buffer.from(payload));
    }
  });

  ipcSocket.on('data', (data) => ipcReader.feed(data));
  ipcSocket.on('close', () => {
    console.log('[sidecar] IPC socket closed');
    cleanup();
  });

  // Login to Discord
  await discordClient.login(TOKEN);
  console.log(`[sidecar] logged in as ${discordClient.user.tag}`);

  await new Promise((resolve) => {
    if (discordClient.isReady()) return resolve();
    discordClient.once('ready', resolve);
  });

  const guild = discordClient.guilds.cache.get(GUILD_ID);
  if (!guild) {
    console.error('[sidecar] guild not found');
    process.exit(1);
  }

  // Join voice channel
  const connection = joinVoiceChannel({
    channelId: CHANNEL_ID,
    guildId: GUILD_ID,
    selfDeaf: false,
    selfMute: false,
    adapterCreator: guild.voiceAdapterCreator,
  });

  await entersState(connection, VoiceConnectionStatus.Ready, 10_000);
  console.log('[sidecar] voice connection ready');

  // Audio player for sending mixed LiveKit audio to Discord
  const player = createAudioPlayer({
    behaviors: { noSubscriber: NoSubscriberBehavior.Play },
  });
  connection.subscribe(player);

  const resource = createAudioResource(opusStream, {
    inputType: StreamType.Opus,
  });
  player.play(resource);
  console.log('[sidecar] audio player started (LiveKit → Discord)');

  player.on('error', (err) => {
    console.error(`[sidecar] player error: ${err.message}`);
  });

  // Receive audio from Discord users
  const receiver = connection.receiver;

  receiver.speaking.on('start', (userId) => {
    writeMessage(ipcSocket, MSG_USER_JOIN, userId, null);

    if (!receiver.subscriptions.has(userId)) {
      console.log(`[sidecar] subscribing to user ${userId}`);
      const stream = receiver.subscribe(userId, { end: { behavior: 0 } });

      stream.on('data', (opusFrame) => {
        frameCount++;
        writeMessage(ipcSocket, MSG_AUDIO_FROM_DISCORD, userId, opusFrame);
      });

      stream.on('error', (err) => {
        console.error(`[sidecar] stream error for ${userId}: ${err.message}`);
      });

      stream.on('close', () => {
        writeMessage(ipcSocket, MSG_USER_LEAVE, userId, null);
        console.log(`[sidecar] user ${userId} stream closed`);
      });
    }
  });

  writeMessage(ipcSocket, MSG_READY, SIDECAR_USER_ID, null);
  console.log('[sidecar] sent READY to bridge');

  // Stats
  const startTime = Date.now();
  setInterval(() => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(`[sidecar] ${elapsed}s uptime, forwarded frames: ${frameCount}, player: ${player.state.status}`);
  }, 10_000);
}

function cleanup() {
  console.log('[sidecar] shutting down');
  if (opusStream) opusStream.push(null); // end stream
  discordClient?.destroy();
  ipcSocket?.end();
  process.exit(0);
}

process.on('SIGINT', () => cleanup());
process.on('SIGTERM', () => cleanup());

main().catch((err) => {
  console.error('[sidecar] fatal:', err);
  process.exit(1);
});
