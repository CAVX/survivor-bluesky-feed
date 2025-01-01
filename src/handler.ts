import { TimerHandler, FeedOperationsByType, FeedCreateEvent } from './util/timer'
import { HandleResolver } from '@atproto/identity';
import { Database } from './db'
import fs from 'fs/promises';
import axios from 'axios';

export class FeedHandler implements TimerHandler {
  private lastFileCheck: Record<string, number> = {};
  private readonly fileCheckInterval = 60 * 1000; // 1 minute
  private db : Database;

  async Initialize(db: Database): Promise<void> {
    this.db = db;
  }
  
  async handleEvent(ops: FeedOperationsByType) {
    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => { return this.filterPost(create.text) })
      .map((create) => {
        return {
          uri: create.uri,
          cid: create.cid,
          indexedAt: new Date().toISOString(),
        }
      });
    
    const now = Date.now();

    if (!this.lastFileCheck['deletePosts.txt'] || now - this.lastFileCheck['deletePosts.txt'] >= this.fileCheckInterval) {
      this.lastFileCheck['deletePosts.txt'] = now;
      postsToDelete.push(...await this.processFile('deletePosts.txt'));
    }

    if (!this.lastFileCheck['insertPosts.txt'] || now - this.lastFileCheck['insertPosts.txt'] >= this.fileCheckInterval) {
      this.lastFileCheck['insertPosts.txt'] = now;
      const insertUris: string[] = [];
      insertUris.push(...await this.processFile('insertPosts.txt'));
      const insertPosts = await this.fetchPosts(insertUris);
      postsToCreate.push(...insertPosts
        .filter((post) => { return post.uri && this.filterPost(post.text) }));
    }

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
  
  filterPost(postText) {
    // Match posts containing "#survivor" and exclude outlier hashtags
    const text = postText.toLowerCase()
    
    // Match posts containing "#survivor" followed by optional digits (e.g., #survivor, #survivor7, #survivorcbs)
    const includeHashtagsRegex = /#survivor(\d*|cbs)?/i;
    const excludeHashtagsRegex = /#survivorseries|#survivorgameplay|#deadbydaylight|#survivors|#survivorslike|#rainworld|#survivorlike|#survivorsguilt|#survivorguilt|#csasurvivor|#survivorsempowered|#mentalhealth|#excult|#traffickingsurvivor|#survivorcoach|#survivormusic|#abortion|#csa|#sa|#cptsd|#iptv/i;

    // Only include posts that have #survivor followed by optional numbers or 'cbs', and do not contain excluded hashtags
    if (includeHashtagsRegex.test(text)) {
      if (excludeHashtagsRegex.test(text)) {
        console.log(`Post matched include RegEx, but also matched exclude RegEx: ${text}`);
        return false;
      }
      console.log(text);
      return true;
    }
    return false;
  }
  
  async processFile(filename: string): Promise<string[]> {
    try {
      const fileContent = await fs.readFile(filename, 'utf-8');
      const urls = fileContent.split('\n').filter((line) => line.trim() !== '');

      const uris = await Promise.all(urls.map((url) => this.urlToUri(url)));

      // Empty the file by writing an empty string
      await fs.writeFile(filename, '');

      return uris;
    } catch (err) {
      if (err.code === 'ENOENT') {
        // File does not exist, return an empty array
        return [];
      }
      throw err;
    }
  }

  async urlToUri(postUrl: string): Promise<string> {
    console.log(`Processing URI ${postUrl}...`);
    const urlRegex = /https:\/\/bsky\.app\/profile\/([^/]+)\/post\/([^/]+)/;
    const match = postUrl.match(urlRegex);

    if (!match) {
      throw new Error(`Invalid post URL: ${postUrl}`);
    }

    const handle = match[1];
    const postId = match[2];

    const handleResolver = new HandleResolver();
    const did = await handleResolver.resolve(handle);

    console.log(`Performing manual processing of feed item with DID of ${did} and post ID of ${postId}.`);

    return `at://${did}/app.bsky.feed.post/${postId}`;
  }

  async fetchPosts(uris: string[]): Promise<FeedCreateEvent[]> {
    try {
      const responses = await Promise.all(
        uris.map(async (uri) => {
          try {
            const response = await axios.get(
              `https://public.api.bsky.app/xrpc/app.bsky.feed.getPostThread`,
              { params: { uri, depth: 1 } }
            );
            console.log(`CID: ${response.data.thread.post.cid}`);
            
            return {
              text: response.data.thread.post.record.text,
              uri,
              cid: response.data.thread.post.cid,
              indexedAt: response.data.thread.post.indexedAt
            }
          } catch (err) {
            console.error(`Error fetching CID and message for URI: ${uri}`, err);
            return {text: '', uri: '', cid: '', indexedAt: ''};
          }
        })
      );
      return responses;
    } catch (err) {
      console.error('Error fetching CIDs:', err);
      return []; // Return empty CIDs on error
    }
  }

}
