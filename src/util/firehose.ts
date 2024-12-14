import { readCar, cborToLexRecord } from '@atproto/repo';
import { Subscription } from '@atproto/xrpc-server';
import { Database } from '../db';
import { ids, lexicons } from '../lexicon/lexicons';
import { OutputSchema as RepoEvent, isCommit, Commit } from '../lexicon/types/com/atproto/sync/subscribeRepos';
import { AtprotoTimerBase, TimerHandler, FeedOperationsByType } from './timer';
import { Record as PostRecord } from '../lexicon/types/app/bsky/feed/post'
import { Record as RepostRecord } from '../lexicon/types/app/bsky/feed/repost'
import { Record as LikeRecord } from '../lexicon/types/app/bsky/feed/like'
import { Record as FollowRecord } from '../lexicon/types/app/bsky/graph/follow'
import { BlobRef } from '@atproto/lexicon'

export class FirehoseSubscription extends AtprotoTimerBase {
  public sub: Subscription<RepoEvent>;

  constructor(public db: Database, public service: string, public timerHandler: TimerHandler) {
    super(db, service, timerHandler);
    this.sub = new Subscription({
      service: service,
      method: ids.ComAtprotoSyncSubscribeRepos,
      getParams: () => this.getCursor(),
      validate: (value: unknown) => {
        try {
          return lexicons.assertValidXrpcMessage<RepoEvent>(
            ids.ComAtprotoSyncSubscribeRepos,
            value
          );
        } catch (err) {
          console.error('repo subscription skipped invalid message', err);
        }
      },
    });
  }

  async run(subscriptionReconnectDelay: number) {
    try {
      for await (const evt of this.sub) {
        if (!isCommit(evt)) continue;
        const ops = await this.getOpsByType(evt);

        this.timerHandler.handleEvent(ops).catch((err) => {
          console.error('repo subscription could not handle message', err);
        });
        // update stored cursor every 20 events or so
        if (isCommit(evt) && evt.seq % 20 === 0) {
          await this.updateCursor(evt.seq);
        }
      }
    } catch (err) {
      console.error('repo subscription errored', err);
      setTimeout(
        () => this.run(subscriptionReconnectDelay),
        subscriptionReconnectDelay
      );
    }
  }

  async updateCursor(cursor: number) {
    await this.db
      .updateTable('sub_state')
      .set({ cursor })
      .where('service', '=', this.service)
      .execute();
  }

  async getCursor(): Promise<{ cursor?: number; }> {
    const res = await this.db
      .selectFrom('sub_state')
      .selectAll()
      .where('service', '=', this.service)
      .executeTakeFirst();
    return res ? { cursor: res.cursor } : {};
  }

  async getOpsByType(evt: Commit): Promise<FeedOperationsByType> {
    const car = await readCar(evt.blocks);
    const opsByType: FeedOperationsByType = {
      posts: { creates: [], deletes: [] },
      reposts: { creates: [], deletes: [] },
      likes: { creates: [], deletes: [] },
      follows: { creates: [], deletes: [] }
    };

    for (const op of evt.ops) {
      const uri = `at://${evt.repo}/${op.path}`;
      const [collection] = op.path.split('/');

      if (op.action === 'update') continue; // updates not supported yet

      if (op.action === 'create') {
        if (!op.cid) continue;
        const recordBytes = car.blocks.get(op.cid);
        if (!recordBytes) continue;

        const record = cborToLexRecord(recordBytes);
        const strCid = op.cid.toString();
        const indexedAt = new Date().toISOString();

        if (collection === ids.AppBskyFeedPost && isPost(record)) {
          opsByType.posts.creates.push({ text: record.text, uri, cid: strCid, indexedAt });
        } else if (collection === ids.AppBskyFeedRepost && isRepost(record)) {
          opsByType.reposts.creates.push({ text: '', uri, cid: strCid, indexedAt });
        } else if (collection === ids.AppBskyFeedLike && isLike(record)) {
          opsByType.likes.creates.push({ text: '', uri, cid: strCid, indexedAt });
        } else if (collection === ids.AppBskyGraphFollow && isFollow(record)) {
          opsByType.follows.creates.push({ text: '', uri, cid: strCid, indexedAt });
        }
      }

      if (op.action === 'delete') {
        if (collection === ids.AppBskyFeedPost) {
          opsByType.posts.deletes.push({ uri });
        } else if (collection === ids.AppBskyFeedRepost) {
          opsByType.reposts.deletes.push({ uri });
        } else if (collection === ids.AppBskyFeedLike) {
          opsByType.likes.deletes.push({ uri });
        } else if (collection === ids.AppBskyGraphFollow) {
          opsByType.follows.deletes.push({ uri });
        }
      }
    }

    return opsByType;
  }
}

export type CreateOp<T> = {
  uri: string
  cid: string
  author: string
  record: T
}

export type DeleteOp = {
  uri: string
}

export const isPost = (obj: unknown): obj is PostRecord => {
  return isType(obj, ids.AppBskyFeedPost)
}

export const isRepost = (obj: unknown): obj is RepostRecord => {
  return isType(obj, ids.AppBskyFeedRepost)
}

export const isLike = (obj: unknown): obj is LikeRecord => {
  return isType(obj, ids.AppBskyFeedLike)
}

export const isFollow = (obj: unknown): obj is FollowRecord => {
  return isType(obj, ids.AppBskyGraphFollow)
}

const isType = (obj: unknown, nsid: string) => {
  try {
    lexicons.assertValidRecord(nsid, fixBlobRefs(obj))
    return true
  } catch (err) {
    return false
  }
}

// @TODO right now record validation fails on BlobRefs
// simply because multiple packages have their own copy
// of the BlobRef class, causing instanceof checks to fail.
// This is a temporary solution.
const fixBlobRefs = (obj: unknown): unknown => {
  if (Array.isArray(obj)) {
    return obj.map(fixBlobRefs)
  }
  if (obj && typeof obj === 'object') {
    if (obj.constructor.name === 'BlobRef') {
      const blob = obj as BlobRef
      return new BlobRef(blob.ref, blob.mimeType, blob.size, blob.original)
    }
    return Object.entries(obj).reduce((acc, [key, val]) => {
      return Object.assign(acc, { [key]: fixBlobRefs(val) })
    }, {} as Record<string, unknown>)
  }
  return obj
}