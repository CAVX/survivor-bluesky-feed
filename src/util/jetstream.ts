import { WebSocketKeepAlive } from "./websocket-keepalive"; //TODO: fails if using @atproto/xrpc-server/src/stream/websocket-keepalive.ts - why?
import { Subscription } from "@atproto/xrpc-server";
import { isObj, hasProp } from "@atproto/lexicon";
import { Record as PostRecord } from '../lexicon/types/app/bsky/feed/post'
import { Database } from "../db";
import { AtprotoTimerBase, TimerHandler, FeedOperationsByType } from './timer';

// Based on https://gist.github.com/aendra-rininsland/32bc4fa0a9207b2cec8a9da331cab734
export class JetstreamFirehoseSubscription extends AtprotoTimerBase {
  public sub: JetstreamSubscription;

  constructor(public db: Database, 
    public service: string = "wss://jetstream.atproto.tools",
    public collection: string = "app.bsky.feed.post",
    public timerHandler: TimerHandler
  ) {
    super(db, service, timerHandler);

    this.sub = new JetstreamSubscription({
      service: service,
      method: "subscribe",
      getParams: async () => ({
        cursor: await this.getCursor(),
        wantedCollections: collection,
      }),
      validate: (value: unknown) => {
        try {
          return value as JetstreamRecord; // TODO validate??
        } catch (err) {
          console.error("repo subscription skipped invalid message", err);
        }
      },
    });
  }

  async run(subscriptionReconnectDelay: number) {
    let i = 0;
    try {
      for await (const evt of this.sub) {
        if (!isJetstreamCommit(evt)) continue;
        
        const ops = getJetstreamOpsByType(evt as JetstreamEvent);
      
        this.timerHandler.handleEvent(ops);
        i++;
        // update stored cursor every 100 events or so
        if (isJetstreamCommit(evt) && i % 100 === 0) {
          await this.updateCursor(evt.time_us);
          i = 0;
        }
      }
    } catch (err) {
      console.error("repo subscription errored", err);
      setTimeout(
        () => this.run(subscriptionReconnectDelay),
        subscriptionReconnectDelay
      );
    }
  }

  async updateCursor(cursor: number) {
    await this.db
      .updateTable("sub_state")
      .set({ cursor })
      .where("service", "=", this.service)
      .execute();
  }

  async getCursor(): Promise<number | undefined> {
    const res = await this.db
      .selectFrom("sub_state")
      .selectAll()
      .where("service", "=", this.service)
      .executeTakeFirst();
    return res?.cursor;
  }
}
export function isJetstreamCommit(v: unknown): v is JetstreamEvent {
  return isObj(v) && hasProp(v, "kind") && v.kind === "commit";
}

export interface JetstreamEvent {
  did: string;
  time_us: number;
  type: string;
  commit: JetstreamCommit;
}

export interface JetstreamCommit {
  rev: string;
  operation: string;
  collection: string;
  rkey: string;
  record: JetstreamRecord;
  cid: string;
}

export interface JetstreamRecord extends PostRecord {}

class JetstreamSubscription<T = unknown> extends Subscription {
  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    const ws = new WebSocketKeepAlive({
      ...this.opts,
      getUrl: async () => {
        const params = (await this.opts.getParams?.()) ?? {};
        const query = encodeQueryParams(params);
        console.log(`${this.opts.service}/${this.opts.method}?${query}`);
        return `${this.opts.service}/${this.opts.method}?${query}`;
      },
    });
    for await (const chunk of ws) {
      try {
        const record = JSON.parse(Buffer.from(chunk).toString());
        yield record;
      } catch (e) {
        console.error(e);
      }
    }
  }
}

function encodeQueryParams(obj: Record<string, unknown>): string {
  const params = new URLSearchParams();
  Object.entries(obj).forEach(([key, value]) => {
    const encoded = encodeQueryParam(value);
    if (Array.isArray(encoded)) {
      encoded.forEach((enc) => params.append(key, enc));
    } else {
      params.set(key, encoded);
    }
  });
  return params.toString();
}

// Adapted from xrpc, but without any lex-specific knowledge
function encodeQueryParam(value: unknown): string | string[] {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return value.toString();
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (typeof value === "undefined") {
    return "";
  }
  if (typeof value === "object") {
    if (value instanceof Date) {
      return value.toISOString();
    } else if (Array.isArray(value)) {
      return value.flatMap(encodeQueryParam);
    } else if (!value) {
      return "";
    }
  }
  throw new Error(`Cannot encode ${typeof value}s into query params`);
}

export const getJetstreamOpsByType = (evt: JetstreamEvent) : FeedOperationsByType => {
  const opsByType: FeedOperationsByType = {
    posts: { creates: [], deletes: [] },
    reposts: { creates: [], deletes: [] },
    likes: { creates: [], deletes: [] },
    follows: { creates: [], deletes: [] }
  };

  if (
    evt?.commit?.collection === "app.bsky.feed.post" &&
    evt?.commit?.operation === "create" &&
    evt?.commit?.record
  ) {
    opsByType.posts.creates.push({ text: evt.commit.record.text, uri: `at://${evt.did}/app.bsky.feed.post/${evt.commit.rkey}`, cid: evt.commit.cid, indexedAt: evt.commit.record.createdAt });
  }

  return opsByType;
};