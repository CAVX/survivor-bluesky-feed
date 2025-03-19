import { AppContext } from '../config'
import {
  QueryParams,
  OutputSchema as AlgoOutput,
} from '../lexicon/types/app/bsky/feed/getFeedSkeleton'
import * as survivor from './survivor'
import * as survivordelay from './survivordelay'
import * as survivordelay2h from './survivordelay2h'
import * as survivordelay3h from './survivordelay3h'

type AlgoHandler = (ctx: AppContext, params: QueryParams) => Promise<AlgoOutput>

const algos: Record<string, AlgoHandler> = {
  [survivor.shortname]: survivor.handler,
  [survivordelay.shortname]: survivordelay.handler,
  [survivordelay2h.shortname]: survivordelay2h.handler,
  [survivordelay3h.shortname]: survivordelay3h.handler
}

export default algos
