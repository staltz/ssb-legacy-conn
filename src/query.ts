import ConnDB = require('ssb-conn-db');
import ConnHub = require('ssb-conn-hub');
import ConnStaging = require('ssb-conn-staging');
import {Address, ConnectionData} from 'ssb-conn-hub/lib/types';
import {StagedData} from 'ssb-conn-staging/lib/types';

export type Peer = {
  address: string;
  key?: string;
  source:
    | 'seed'
    | 'pub'
    | 'manual'
    | 'friends'
    | 'local'
    | 'dht'
    | 'bt'
    | 'stored';
  state?: undefined | 'connecting' | 'connected';
  stateChange?: number;
  failure?: number;
  client?: boolean;
  duration?: {
    mean: number;
  };
  ping?: {
    rtt: {
      mean: number;
    };
    skew: number;
    fail?: any;
  };
  announcers?: number;
};

/**
 * Convert ssb-conn-staging modes to legacy gossip 'source'
 * @param mode
 */
function modeToSource(mode: StagedData['mode']): Peer['source'] {
  if (mode === 'lan') return 'local';
  if (mode === 'bt') return 'bt';
  if (mode === 'internet') return 'pub';
  return 'manual';
}

type HubEntry = [Address, ConnectionData];

// From hub: connected or connecting or in-connection
// From db or staging: connectable
// Filter by field check
// Filter by field check relative to an updateTimestamp
// Filter by exponential backoff
// (Async) filter by blocking status
// (Convert to array) Get the max or min of all
// (Convert to array) Sort by stateChange
// (Convert to array) Get length

export class ConnQuery {
  private db: ConnDB;
  private hub: ConnHub;
  private staging: ConnStaging;

  constructor(db: ConnDB, hub: ConnHub, staging: ConnStaging) {
    this.db = db;
    this.hub = hub;
    this.staging = staging;
  }

  private _hubEntryToPeer([address, hubData]: HubEntry): Peer {
    const peer = this.db.has(address)
      ? {address, ...this.db.get(address)}
      : {address, source: 'manual'};
    if (hubData.key) peer.key = hubData.key;
    return peer;
  }

  public peersConnected(): Array<Peer> {
    return Array.from(this.hub.entries())
      .filter(([_address, data]: HubEntry) => data.state === 'connected')
      .map((e: HubEntry) => this._hubEntryToPeer(e));
  }

  public peersConnecting(): Array<Peer> {
    return Array.from(this.hub.entries())
      .filter(([_address, data]: HubEntry) => data.state === 'connecting')
      .map((e: HubEntry) => this._hubEntryToPeer(e));
  }

  public peersInConnection(): Array<Peer> {
    return Array.from(this.hub.entries())
      .filter(
        ([_address, data]: HubEntry) =>
          data.state === 'connected' || data.state === 'connecting',
      )
      .map((e: HubEntry) => this._hubEntryToPeer(e));
  }

  public peersConnectable(
    pool: 'db' | 'staging' | 'dbAndStaging' = 'db',
  ): Array<Peer> {
    const useDB = pool === 'db' || pool === 'dbAndStaging';
    const useStaging = pool === 'staging' || pool === 'dbAndStaging';

    return ([] as Array<[string, any]>)
      .concat(useDB ? Array.from(this.db.entries()) : [])
      .concat(useStaging ? Array.from(this.staging.entries()) : [])
      .filter(([address]) => {
        const state = this.hub.getState(address);
        return state !== 'connected' && state !== 'connecting';
      })
      .map(([address, data]) => {
        if (!data.source && data.mode) {
          return {address, source: modeToSource(data.mode), ...data};
        } else {
          return {address, ...data};
        }
      });
  }

  public peersAll(): Array<Peer> {
    return this.peersConnectable().concat(this.peersInConnection());
  }

  static passesExpBackoff(
    step: number,
    max: number,
    timestamp: number = Date.now(),
  ) {
    return (peer: Peer) => {
      const prevAttempt = peer.stateChange! | 0;
      const F = peer.failure! | 0;
      const Q = step;
      const M = max;
      const expBackoff = Math.min(Math.pow(2, F) * Q, M || Infinity);
      const nextAttempt = prevAttempt + expBackoff;
      return nextAttempt < timestamp;
    };
  }

  static passesGroupDebounce(groupMin: number, timestamp: number = Date.now()) {
    return (group: Array<Peer>) => {
      const newestStateChange = group.reduce(
        (M: number, p: Peer) => Math.max(M, p.stateChange || 0),
        0,
      );
      const minTimeThreshold: number = newestStateChange + groupMin;
      if (timestamp < minTimeThreshold) return [];
      else return group;
    };
  }

  /**
   * Answers whether a peer has not had a connection attempt yet.
   */
  static hasNoAttempts(p: Peer): boolean {
    return !p.stateChange;
  }

  /**
   * Answers whether a peer has never been successfully connected to yet, but
   * has been tried.
   */
  static hasOnlyFailedAttempts(p: Peer): boolean {
    return !!p.stateChange && (!p.duration || p.duration.mean == 0);
  }

  /**
   * Answers whether a peer has had some successful connection in the past.
   */
  static hasSuccessfulAttempts(p: Peer): boolean {
    return !!p.duration && p.duration.mean > 0;
  }

  /**
   * Answers whether a peer has successfully pinged us in the past.
   */
  static hasPinged(p: Peer): boolean {
    return !!p.ping && !!p.ping.rtt && p.ping.rtt.mean > 0;
  }

  /**
   * Sorts peers from oldest 'stateChange' timestamp to newest.
   */
  static sortByStateChange(peers: Array<Peer>) {
    return peers.sort((a, b) => a.stateChange! - b.stateChange!);
  }
}
