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
  NoSubscriberBehavior,
  getVoiceConnection,
} from '@discordjs/voice';

// IPC message types — must match pkg/ipc/protocol.go
const MSG_AUDIO_FROM_DISCORD = 0x01;
const MSG_AUDIO_TO_DISCORD = 0x02;
const MSG_USER_JOIN = 0x03;
const MSG_USER_LEAVE = 0x04;
const MSG_READY = 0x05;
const MSG_SHUTDOWN = 0x06;
const MSG_JOIN_CHANNEL = 0x07;
const MSG_LEAVE_CHANNEL = 0x08;
const MSG_VOICE_STATE = 0x09;

const SIDECAR_USER_ID = '0';

const SOCKET_PATH = process.env.IPC_SOCKET_PATH || '/tmp/discord-voice-bridge.sock';
const TOKEN = process.env.DISCORD_BOT_TOKEN;
const GUILD_ID = process.env.DISCORD_GUILD_ID;

if (!TOKEN || !GUILD_ID) {
  console.error('[sidecar] set DISCORD_BOT_TOKEN, DISCORD_GUILD_ID');
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

function channelIdPayload(channelId) {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64LE(BigInt(channelId));
  return buf;
}

function readChannelId(payload) {
  if (payload.length < 8) return null;
  return payload.readBigUInt64LE(0).toString();
}

// VOICE_STATE payload: channel_id(8) + category_id(8) + name_len(2) + name(utf8)
function voiceStatePayload(channelId, channelName, categoryId) {
  const nameBuf = Buffer.from(channelName, 'utf8');
  const buf = Buffer.alloc(8 + 8 + 2 + nameBuf.length);
  buf.writeBigUInt64LE(BigInt(channelId || '0'), 0);
  buf.writeBigUInt64LE(BigInt(categoryId || '0'), 8);
  buf.writeUInt16LE(nameBuf.length, 16);
  nameBuf.copy(buf, 18);
  return buf;
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

class OpusFrameStream extends Readable {
  constructor() {
    super({ objectMode: true });
    this._frames = [];
    this._reading = false;
  }

  pushFrame(opusFrame) {
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
let currentChannelId = null;
let currentConnection = null;
let currentPlayer = null;
let opusStream = null;

async function main() {
  discordClient = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildVoiceStates,
    ],
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

  const ipcReader = new IPCReader((type, _userId, payload) => {
    if (type === MSG_SHUTDOWN) {
      console.log('[sidecar] received shutdown');
      cleanup();
    } else if (type === MSG_AUDIO_TO_DISCORD && opusStream) {
      opusStream.pushFrame(Buffer.from(payload));
    } else if (type === MSG_JOIN_CHANNEL) {
      const channelId = readChannelId(payload);
      if (channelId) {
        console.log(`[sidecar] JOIN_CHANNEL ${channelId}`);
        joinChannel(channelId);
      }
    } else if (type === MSG_LEAVE_CHANNEL) {
      console.log('[sidecar] LEAVE_CHANNEL');
      leaveChannel();
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

  // Watch voice state updates guild-wide
  discordClient.on('voiceStateUpdate', (oldState, newState) => {
    // Ignore bot's own voice state changes
    if (newState.member?.user?.bot && newState.member.user.id === discordClient.user.id) return;

    const userId = newState.id;

    if (oldState.channelId !== newState.channelId) {
      // User moved, joined, or left
      const channelId = newState.channelId || '0';
      const channel = newState.channel;
      const payload = voiceStatePayload(channelId, channel?.name || '', channel?.parentId || '0');
      writeMessage(ipcSocket, MSG_VOICE_STATE, userId, payload);

      if (newState.channelId) {
        console.log(`[sidecar] voice: ${userId} joined "${channel?.name}" (${newState.channelId}) category=${channel?.parentId}`);
      } else {
        console.log(`[sidecar] voice: ${userId} left voice`);
      }
    }
  });

  // Send initial voice states for users already in VCs
  for (const [, state] of guild.voiceStates.cache) {
    if (state.channelId && !state.member?.user?.bot) {
      const channel = guild.channels.cache.get(state.channelId);
      const payload = voiceStatePayload(state.channelId, channel?.name || '', channel?.parentId || '0');
      writeMessage(ipcSocket, MSG_VOICE_STATE, state.id, payload);
    }
  }

  writeMessage(ipcSocket, MSG_READY, SIDECAR_USER_ID, null);
  console.log('[sidecar] sent READY — watching voice states');

  // Stats
  const startTime = Date.now();
  setInterval(() => {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(`[sidecar] ${elapsed}s uptime, forwarded frames: ${frameCount}, channel: ${currentChannelId || 'none'}`);
  }, 10_000);
}

async function joinChannel(channelId) {
  if (currentChannelId === channelId) return;

  // Leave current channel first
  if (currentConnection) {
    leaveChannel();
  }

  const guild = discordClient.guilds.cache.get(GUILD_ID);
  if (!guild) return;

  currentChannelId = channelId;

  const connection = joinVoiceChannel({
    channelId,
    guildId: GUILD_ID,
    selfDeaf: false,
    selfMute: false,
    adapterCreator: guild.voiceAdapterCreator,
  });

  try {
    await entersState(connection, VoiceConnectionStatus.Ready, 10_000);
  } catch (err) {
    console.error(`[sidecar] failed to join channel ${channelId}: ${err.message}`);
    currentChannelId = null;
    return;
  }

  currentConnection = connection;
  console.log(`[sidecar] joined voice channel ${channelId}`);

  // Audio player for sending mixed audio to Discord
  currentPlayer = createAudioPlayer({
    behaviors: { noSubscriber: NoSubscriberBehavior.Play },
  });
  connection.subscribe(currentPlayer);

  opusStream = new OpusFrameStream();
  const resource = createAudioResource(opusStream, {
    inputType: StreamType.Opus,
  });
  currentPlayer.play(resource);

  // Receive audio from Discord users in this channel
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
}

function leaveChannel() {
  if (currentConnection) {
    currentConnection.destroy();
    currentConnection = null;
  }
  if (currentPlayer) {
    currentPlayer.stop();
    currentPlayer = null;
  }
  if (opusStream) {
    opusStream.push(null);
    opusStream = null;
  }
  if (currentChannelId) {
    console.log(`[sidecar] left voice channel ${currentChannelId}`);
    currentChannelId = null;
  }
  frameCount = 0;
}

function cleanup() {
  console.log('[sidecar] shutting down');
  leaveChannel();
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
