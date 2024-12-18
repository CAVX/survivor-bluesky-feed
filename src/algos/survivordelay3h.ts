import { QueryParams } from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import { AppContext } from '../config'

// max 15 chars
export const shortname = 'survivordelay3h'

export const handler = async (ctx: AppContext, params: QueryParams) => {
  let builder = ctx.db
    .selectFrom('post')
    .selectAll()
    .where('post.indexedAt', '<', new Date(Date.now() - 3600 * 3 * 1000).toISOString()) // Ensure posts are at least an hour old
    .orderBy('indexedAt', 'desc')
    .orderBy('cid', 'desc')
    .limit(params.limit)

  //Pin an explanation post
  const pinnedPostUri = 'at://did:plc:fpeawo3ch4ypy66kxj57s2w4/app.bsky.feed.post/3ldlyyc55ec2h';

  if (params.cursor) {
    const timeStr = new Date(parseInt(params.cursor, 10)).toISOString()
    builder = builder.where('post.indexedAt', '<', timeStr)
  } 

  const res = await builder.execute()

  const feed: { post: string }[] = [];

  if (!params.cursor) {
    feed.push({ post: pinnedPostUri });
  }

  feed.push(
    ...res
      .filter((row) => row.uri !== pinnedPostUri)
      .map((row) => ({
        post: row.uri,
      }))
  );

  let cursor: string | undefined
  const last = res.at(-1)
  if (last) {
    cursor = new Date(last.indexedAt).getTime().toString(10)
  }

  return {
    cursor,
    feed,
  }
}
