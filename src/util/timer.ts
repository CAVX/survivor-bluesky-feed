import { Database } from '../db'

export interface TimerHandler{
  Initialize(db : Database): Promise<void>
  handleEvent(ops: FeedOperationsByType): Promise<void>
}

export abstract class AtprotoTimerBase {
  constructor(public db: Database, public service: string, public timerHandler: TimerHandler) {
    timerHandler.Initialize(db);
  }

  abstract run(subscriptionReconnectDelay: number) : Promise<void>
}

export type FeedEvent = {
  uri: string
}

export type FeedCreateEvent =  FeedEvent & {
  cid: string,
  indexedAt: string,
  text: string
}

export type FeedOperationsByType = {
  posts: FeedOperation
  reposts: FeedOperation
  likes: FeedOperation
  follows: FeedOperation
}

export type FeedOperation = {
  creates: FeedCreateEvent[]
  deletes: FeedEvent[]
}