import { App, LogLevel } from '@slack/bolt';
import type { GenericMessageEvent, BotMessageEvent } from '@slack/types';

import { ASSISTANT_NAME, TRIGGER_PATTERN } from '../config.js';
import { updateChatName } from '../db.js';
import { readEnvFile } from '../env.js';
import { logger } from '../logger.js';
import { registerChannel, ChannelOpts } from './registry.js';
import {
  Channel,
  OnInboundMessage,
  OnChatMetadata,
  RegisteredGroup,
} from '../types.js';

// Slack's chat.postMessage API limits text to ~4000 characters per call.
// Messages exceeding this are split into sequential chunks.
const MAX_MESSAGE_LENGTH = 4000;

// The message subtypes we process. Bolt delivers all subtypes via app.event('message');
// we filter to regular messages (GenericMessageEvent, subtype undefined) and bot messages
// (BotMessageEvent, subtype 'bot_message') so we can track our own output.
type HandledMessageEvent = GenericMessageEvent | BotMessageEvent;

/**
 * Thread-aware JID encoding for Slack.
 *
 * Format:  slack:{channelId}/{threadTs}
 *
 * - The base JID (`slack:{channelId}`) is used for group registration/lookup.
 * - The thread JID includes the Slack thread timestamp so replies land in the
 *   correct thread.  If the incoming message is already in a thread we use its
 *   `thread_ts`; otherwise we use `msg.ts`, which starts a new thread anchored
 *   to that message.
 *
 * Agent opt-out: include `<no-thread>` anywhere in the response text to send
 * the reply at channel level instead of in a thread.  The tag is stripped
 * before the message is posted.
 */

/** Build the base channel JID (no thread suffix). */
function slackChannelJid(channelId: string): string {
  return `slack:${channelId}`;
}

/** Build the full thread-aware JID. */
function slackThreadJid(channelId: string, threadTs: string): string {
  return `slack:${channelId}/${threadTs}`;
}

/**
 * Parse a Slack JID (with or without thread suffix) into its parts.
 * Returns `{ channelId, threadTs }` where `threadTs` may be undefined.
 */
function parseSlackJid(jid: string): { channelId: string; threadTs: string | undefined } {
  const rest = jid.replace(/^slack:/, '');
  const slashIdx = rest.indexOf('/');
  if (slashIdx === -1) {
    return { channelId: rest, threadTs: undefined };
  }
  return { channelId: rest.slice(0, slashIdx), threadTs: rest.slice(slashIdx + 1) };
}

/**
 * Convert GitHub-flavored Markdown to Slack mrkdwn format.
 *
 * Key differences handled:
 *   **bold**        → *bold*
 *   [text](url)     → <url|text>
 *   ## Heading      → *Heading*
 *   - bullet item   → • bullet item
 *
 * Fenced code blocks and inline code are left untouched.
 */
export function markdownToMrkdwn(text: string): string {
  const placeholders: string[] = [];

  // Protect fenced code blocks (``` ... ```) before any other substitutions
  let result = text.replace(/```[\s\S]*?```/g, (match) => {
    placeholders.push(match);
    return `\x00P${placeholders.length - 1}\x00`;
  });

  // Protect inline code (` ... `)
  result = result.replace(/`[^`\n]+`/g, (match) => {
    placeholders.push(match);
    return `\x00P${placeholders.length - 1}\x00`;
  });

  // Headings: ## Heading → *Heading*
  result = result.replace(/^#{1,6}\s+(.+)$/gm, '*$1*');

  // Bold: **text** → *text*
  result = result.replace(/\*\*([^*\n]+?)\*\*/g, '*$1*');

  // Links: [text](url) → <url|text>
  result = result.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<$2|$1>');

  // Unordered bullets: lines starting with "- " or "* " → "• "
  result = result.replace(/^[ \t]*[-*][ \t]+/gm, '• ');

  // Restore protected code blocks
  result = result.replace(
    /\x00P(\d+)\x00/g,
    (_, i) => placeholders[parseInt(i, 10)],
  );

  return result;
}

/**
 * Format outbound text for Slack: strip the `<no-thread>` directive and
 * convert Markdown to mrkdwn.  Returns the formatted text and whether the
 * agent opted out of threading.
 */
function formatForSlack(text: string): { formatted: string; noThread: boolean } {
  const noThread = text.includes('<no-thread>');
  const cleaned = text.replace(/<no-thread>\s*/g, '');
  return { formatted: markdownToMrkdwn(cleaned), noThread };
}

export interface SlackChannelOpts {
  onMessage: OnInboundMessage;
  onChatMetadata: OnChatMetadata;
  registeredGroups: () => Record<string, RegisteredGroup>;
}

export class SlackChannel implements Channel {
  name = 'slack';

  private app: App;
  private botUserId: string | undefined;
  private connected = false;
  private outgoingQueue: Array<{ jid: string; text: string }> = [];
  private flushing = false;
  private userNameCache = new Map<string, string>();

  private opts: SlackChannelOpts;

  constructor(opts: SlackChannelOpts) {
    this.opts = opts;

    // Read tokens from .env (not process.env — keeps secrets off the environment
    // so they don't leak to child processes, matching NanoClaw's security pattern)
    const env = readEnvFile(['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN']);
    const botToken = env.SLACK_BOT_TOKEN;
    const appToken = env.SLACK_APP_TOKEN;

    if (!botToken || !appToken) {
      throw new Error(
        'SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set in .env',
      );
    }

    this.app = new App({
      token: botToken,
      appToken,
      socketMode: true,
      logLevel: LogLevel.ERROR,
    });

    this.setupEventHandlers();
  }

  private setupEventHandlers(): void {
    // Use app.event('message') instead of app.message() to capture all
    // message subtypes including bot_message (needed to track our own output)
    this.app.event('message', async ({ event }) => {
      // Bolt's event type is the full MessageEvent union (17+ subtypes).
      // We filter on subtype first, then narrow to the two types we handle.
      const subtype = (event as { subtype?: string }).subtype;
      if (subtype && subtype !== 'bot_message') return;

      // After filtering, event is either GenericMessageEvent or BotMessageEvent
      const msg = event as HandledMessageEvent;

      if (!msg.text) return;

      // Base JID (channel only) is used for group registration/lookup so that
      // groups registered as "slack:CHANNEL_ID" continue to match regardless
      // of which thread a message arrived in.
      const baseJid = slackChannelJid(msg.channel);
      const timestamp = new Date(parseFloat(msg.ts) * 1000).toISOString();
      const isGroup = msg.channel_type !== 'im';

      // Always report metadata using the base JID for group discovery
      this.opts.onChatMetadata(baseJid, timestamp, undefined, 'slack', isGroup);

      // Only deliver full messages for registered groups
      const groups = this.opts.registeredGroups();
      if (!groups[baseJid]) return;

      const isBotMessage = !!msg.bot_id || msg.user === this.botUserId;

      let senderName: string;
      if (isBotMessage) {
        senderName = ASSISTANT_NAME;
      } else {
        senderName =
          (msg.user ? await this.resolveUserName(msg.user) : undefined) ||
          msg.user ||
          'unknown';
      }

      // Translate Slack <@UBOTID> mentions into TRIGGER_PATTERN format.
      // Slack encodes @mentions as <@U12345>, which won't match TRIGGER_PATTERN
      // (e.g., ^@<ASSISTANT_NAME>\b), so we prepend the trigger when the bot is @mentioned.
      let content = msg.text;
      if (this.botUserId && !isBotMessage) {
        const mentionPattern = `<@${this.botUserId}>`;
        if (
          content.includes(mentionPattern) &&
          !TRIGGER_PATTERN.test(content)
        ) {
          content = `@${ASSISTANT_NAME} ${content}`;
        }
      }

      // Thread-aware JID: use thread_ts if the message is already in a thread,
      // otherwise use msg.ts to anchor a new thread on this message.
      // The agent's response will be routed back to this JID, landing in the
      // correct thread.  Bot messages use the base JID so they don't start
      // new threads for every outgoing chunk.
      const threadTs = (msg as { thread_ts?: string }).thread_ts ?? msg.ts;
      const messageJid = isBotMessage ? baseJid : slackThreadJid(msg.channel, threadTs);

      // Ensure the thread JID exists in the chats table so the foreign key
      // constraint on messages.chat_jid is satisfied.
      if (messageJid !== baseJid) {
        this.opts.onChatMetadata(messageJid, timestamp, undefined, 'slack', isGroup);
      }

      this.opts.onMessage(messageJid, {
        id: msg.ts,
        chat_jid: messageJid,
        sender: msg.user || msg.bot_id || '',
        sender_name: senderName,
        content,
        timestamp,
        is_from_me: isBotMessage,
        is_bot_message: isBotMessage,
      });
    });
  }

  async connect(): Promise<void> {
    await this.app.start();

    // Get bot's own user ID for self-message detection.
    // Resolve this BEFORE setting connected=true so that messages arriving
    // during startup can correctly detect bot-sent messages.
    try {
      const auth = await this.app.client.auth.test();
      this.botUserId = auth.user_id as string;
      logger.info({ botUserId: this.botUserId }, 'Connected to Slack');
    } catch (err) {
      logger.warn({ err }, 'Connected to Slack but failed to get bot user ID');
    }

    this.connected = true;

    // Flush any messages queued before connection
    await this.flushOutgoingQueue();

    // Sync channel names on startup
    await this.syncChannelMetadata();
  }

  async sendMessage(jid: string, text: string): Promise<void> {
    const { channelId, threadTs } = parseSlackJid(jid);
    const { formatted, noThread } = formatForSlack(text);

    const postBase = {
      channel: channelId,
      ...(threadTs && !noThread ? { thread_ts: threadTs } : {}),
    };

    if (!this.connected) {
      this.outgoingQueue.push({ jid, text });
      logger.info(
        { jid, queueSize: this.outgoingQueue.length },
        'Slack disconnected, message queued',
      );
      return;
    }

    try {
      // Slack limits messages to ~4000 characters; split if needed
      if (formatted.length <= MAX_MESSAGE_LENGTH) {
        await this.app.client.chat.postMessage({ ...postBase, text: formatted });
      } else {
        for (let i = 0; i < formatted.length; i += MAX_MESSAGE_LENGTH) {
          await this.app.client.chat.postMessage({
            ...postBase,
            text: formatted.slice(i, i + MAX_MESSAGE_LENGTH),
          });
        }
      }
      logger.info({ jid, length: formatted.length }, 'Slack message sent');
    } catch (err) {
      this.outgoingQueue.push({ jid, text });
      logger.warn(
        { jid, err, queueSize: this.outgoingQueue.length },
        'Failed to send Slack message, queued',
      );
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  ownsJid(jid: string): boolean {
    return jid.startsWith('slack:');
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    await this.app.stop();
  }

  // Slack does not expose a typing indicator API for bots.
  // This no-op satisfies the Channel interface so the orchestrator
  // doesn't need channel-specific branching.
  async setTyping(_jid: string, _isTyping: boolean): Promise<void> {
    // no-op: Slack Bot API has no typing indicator endpoint
  }

  async addReaction(
    jid: string,
    messageId: string,
    emoji: string,
  ): Promise<void> {
    const { channelId } = parseSlackJid(jid);
    try {
      await this.app.client.reactions.add({
        channel: channelId,
        timestamp: messageId,
        name: emoji,
      });
    } catch (err) {
      // already_reacted is fine — don't log as warning
      if (
        (err as { data?: { error?: string } })?.data?.error ===
        'already_reacted'
      )
        return;
      logger.warn(
        { jid, messageId, emoji, err },
        'Failed to add Slack reaction',
      );
    }
  }

  /**
   * Sync channel metadata from Slack.
   * Fetches channels the bot is a member of and stores their names in the DB.
   */
  async syncChannelMetadata(): Promise<void> {
    try {
      logger.info('Syncing channel metadata from Slack...');
      let cursor: string | undefined;
      let count = 0;

      do {
        const result = await this.app.client.conversations.list({
          types: 'public_channel,private_channel',
          exclude_archived: true,
          limit: 200,
          cursor,
        });

        for (const ch of result.channels || []) {
          if (ch.id && ch.name && ch.is_member) {
            updateChatName(`slack:${ch.id}`, ch.name);
            count++;
          }
        }

        cursor = result.response_metadata?.next_cursor || undefined;
      } while (cursor);

      logger.info({ count }, 'Slack channel metadata synced');
    } catch (err) {
      logger.error({ err }, 'Failed to sync Slack channel metadata');
    }
  }

  private async resolveUserName(userId: string): Promise<string | undefined> {
    if (!userId) return undefined;

    const cached = this.userNameCache.get(userId);
    if (cached) return cached;

    try {
      const result = await this.app.client.users.info({ user: userId });
      const name = result.user?.real_name || result.user?.name;
      if (name) this.userNameCache.set(userId, name);
      return name;
    } catch (err) {
      logger.debug({ userId, err }, 'Failed to resolve Slack user name');
      return undefined;
    }
  }

  private async flushOutgoingQueue(): Promise<void> {
    if (this.flushing || this.outgoingQueue.length === 0) return;
    this.flushing = true;
    try {
      logger.info(
        { count: this.outgoingQueue.length },
        'Flushing Slack outgoing queue',
      );
      while (this.outgoingQueue.length > 0) {
        const item = this.outgoingQueue.shift()!;
        const { channelId, threadTs } = parseSlackJid(item.jid);
        const { formatted, noThread } = formatForSlack(item.text);
        await this.app.client.chat.postMessage({
          channel: channelId,
          ...(threadTs && !noThread ? { thread_ts: threadTs } : {}),
          text: formatted,
        });
        logger.info(
          { jid: item.jid, length: formatted.length },
          'Queued Slack message sent',
        );
      }
    } finally {
      this.flushing = false;
    }
  }
}

registerChannel('slack', (opts: ChannelOpts) => {
  const envVars = readEnvFile(['SLACK_BOT_TOKEN', 'SLACK_APP_TOKEN']);
  if (!envVars.SLACK_BOT_TOKEN || !envVars.SLACK_APP_TOKEN) {
    logger.warn('Slack: SLACK_BOT_TOKEN or SLACK_APP_TOKEN not set');
    return null;
  }
  return new SlackChannel(opts);
});
