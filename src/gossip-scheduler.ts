import ConnDB = require('ssb-conn-db');
import ConnHub = require('ssb-conn-hub');
import {ListenEvent as HubEvent} from 'ssb-conn-hub/lib/types';
import ConnStaging = require('ssb-conn-staging');
import {Peer} from './types';
import {Msg} from 'ssb-typescript';
import {plugin, muxrpc} from 'secret-stack-decorators';
const pull = require('pull-stream');
const ip = require('ip');
const onWakeup = require('on-wakeup');
const onNetwork = require('on-change-network');
const hasNetwork = require('./has-network-debounced');
const ref = require('ssb-ref');

function noop() {}

function not(fn: Function) {
  return function(e: any) {
    return !fn(e);
  };
}

function and(...args: Array<any>) {
  return (value: any) => args.every((fn: Function) => fn.call(null, value));
}

function filterWithExpBackoff(opts: any, ts?: number) {
  const timestamp = ts || Date.now();
  return (peer: Peer) => {
    const prevAttempt = peer.stateChange! | 0;
    const F = peer.failure! | 0;
    const Q = opts.backoffStep;
    const M = opts.backoffMax;
    const expBackoff = Math.min(Math.pow(2, F) * Q, M || Infinity);
    const nextAttempt = prevAttempt + expBackoff;
    return nextAttempt < timestamp;
  };
}

//detect if not connected to wifi or other network
//(i.e. if there is only localhost)

function isOffline(p: Peer) {
  if (ip.isLoopback(p.host) || p.host == 'localhost') return false;
  else if (p.source === 'bt') return false;
  else return !hasNetwork();
}

const canBeConnected = not(isOffline);

function isLocal(p: Peer) {
  // don't rely on private ip address, because
  // cjdns creates fake private ip addresses.
  // ignore localhost addresses, because sometimes they get broadcast.
  return !ip.isLoopback(p.host) && ip.isPrivate(p.host) && p.source === 'local';
}

function isFriend(p: Peer) {
  return p.source === 'friends';
}

function isUnattempted(p: Peer) {
  return !p.stateChange;
}

//select peers which have never been successfully connected to yet,
//but have been tried.
function isInactive(p: Peer) {
  return p.stateChange && (!p.duration || p.duration.mean == 0);
}

function isLongterm(p: Peer) {
  return p.ping && p.ping.rtt && p.ping.rtt.mean > 0;
}

//peers which we can connect to, but are not upgraded.
//select peers which we can connect to, but are not upgraded to LT.
//assume any peer is legacy, until we know otherwise...
function isLegacy(peer: Peer) {
  return (
    peer.duration &&
    (peer.duration && peer.duration.mean > 0) &&
    !isLongterm(peer)
  );
}

// Sort from oldest to newest
function sortByOldestStateChange(peers: Array<Peer>) {
  return peers.sort((a, b) => a.stateChange! - b.stateChange!);
}

// Take the first n elements
function take<T>(arr: Array<T>, n: number) {
  return arr.slice(0, Math.max(n, 0));
}

function convertModeToLegacySource(
  mode: 'lan' | 'bt' | 'internet',
): Peer['source'] {
  if (mode === 'lan') return 'local';
  if (mode === 'bt') return 'bt';
  if (mode === 'internet') return 'pub';
  return 'manual';
}

const minute = 60e3;
const hour = 60 * 60e3;

@plugin('1.0.0')
export class GossipScheduler {
  private ssb: any;
  private config: any;
  private db: ConnDB;
  private hub: ConnHub;
  private staging: ConnStaging;
  private closed: boolean;
  private lastMessageAt: number;
  private updateTimestamp: number;
  private hasScheduledAnUpdate: boolean;

  constructor(ssb: any, config: any) {
    this.ssb = ssb;
    this.config = config;
    this.db = this.ssb.gossip.db();
    this.hub = this.ssb.gossip.hub();
    this.staging = this.ssb.gossip.staging();
    this.closed = false;
    this.lastMessageAt = 0;
    this.updateTimestamp = 0;
    this.hasScheduledAnUpdate = false;

    this.ssb.post((data: any) => {
      if (data.value.author != this.ssb.id) {
        this.lastMessageAt = Date.now();
      }
    });
  }

  // Utility to pick from config, with some defaults
  private conf(name: any, def: any) {
    if (this.config.gossip == null) return def;
    const value = this.config.gossip[name];
    return value == null || value === '' ? def : value;
  }

  private isCurrentlyDownloading() {
    // don't schedule gossip if currently downloading messages
    return this.lastMessageAt && this.lastMessageAt > Date.now() - 500;
  }

  private inConnection(p: Peer) {
    const state = this.hub.getState(p.address!);
    return state === 'connected' || state === 'connecting';
  }

  private selectConnectedExcess(peers: Array<Peer>, opts: any) {
    const connectedPeers = peers.filter(this.inConnection);
    const numConnected = connectedPeers.length;

    if (numConnected > opts.quota * 2) {
      const excess = numConnected - opts.quota;
      return take(sortByOldestStateChange(connectedPeers), excess);
    } else {
      return [];
    }
  }

  //opts: { quota, groupMin, min, backoffStep, max }
  private selectDisconnected(peers: Array<Peer>, opts: any) {
    const connectedPeers = peers.filter(this.inConnection);
    const connectablePeers = peers.filter(
      and(not(this.inConnection), canBeConnected),
    );

    const newestStateChange = connectablePeers.reduce(
      (M: number, peer: Peer) => Math.max(M, peer.stateChange || 0),
      0,
    );
    const timeThreshold: number = newestStateChange + opts.groupMin;
    if (this.updateTimestamp < timeThreshold) return [];

    const count = Math.max(opts.quota - connectedPeers.length, 0);
    const candidatesWithinTimeFrame = connectablePeers.filter(
      filterWithExpBackoff(opts, this.updateTimestamp),
    );
    return take(sortByOldestStateChange(candidatesWithinTimeFrame), count);
  }

  // Utility to connect to bunch of peers, or disconnect if over quota
  private updateTheseConnections(peers: Array<Peer>, opts: any) {
    this.selectConnectedExcess(peers, opts).forEach(peer =>
      this.hub.disconnect(peer.address!).then(noop, noop),
    );

    this.selectDisconnected(peers, opts).forEach(peer =>
      this.hub.connect(peer.address!).then(noop, noop),
    );
  }

  private updateConnectionsNow() {
    // Respect some limits: don't attempt to connect while migration is running
    if (!this.ssb.ready() || this.isCurrentlyDownloading()) return;

    this.updateTimestamp = Date.now();

    // The total database, from which to sample
    const peers: Array<Peer> = ([] as Array<Peer>)
      .concat(
        Array.from(this.db.entries()).map(([address, data]) => ({
          ...data,
          address,
        })),
      )
      .concat(
        Array.from(this.staging.entries()).map(([address, data]) => ({
          ...data,
          address,
          source: convertModeToLegacySource(data.mode) as Peer['source'],
        })),
      );

    const numOfConnected = peers.filter(
      and(this.inConnection, not(isLocal), not(isFriend)),
    ).length;

    const numOfConnectedFriends = peers.filter(and(this.inConnection, isFriend))
      .length;

    if (this.conf('friends', true))
      this.updateTheseConnections(peers.filter(isFriend), {
        quota: 3,
        backoffStep: 2e3,
        backoffMax: 10 * minute,
        groupMin: 1e3,
      });

    if (this.conf('seed', true))
      this.updateTheseConnections(peers.filter(p => p.source === 'seed'), {
        quota: 3,
        backoffStep: 2e3,
        backoffMax: 10 * minute,
        groupMin: 1e3,
      });

    if (this.conf('local', true))
      this.updateTheseConnections(peers.filter(isLocal), {
        quota: 3,
        backoffStep: 2e3,
        backoffMax: 10 * minute,
        groupMin: 1e3,
      });

    if (this.conf('global', true)) {
      // prioritize friends
      this.updateTheseConnections(peers.filter(and(isFriend, isLongterm)), {
        quota: 2,
        backoffStep: 10e3,
        backoffMax: 10 * minute,
        groupMin: 5e3,
      });

      if (numOfConnectedFriends < 2) {
        this.updateTheseConnections(
          peers.filter(and(isFriend, isUnattempted)),
          {
            quota: 1,
            backoffStep: 0,
            backoffMax: 0,
            groupMin: 0,
          },
        );
      }

      this.updateTheseConnections(peers.filter(and(isFriend, isInactive)), {
        quota: 3,
        backoffStep: minute,
        backoffMax: 3 * hour,
        groupMin: 5 * minute,
      });

      // standard longterm peers
      this.updateTheseConnections(
        peers.filter(and(isLongterm, not(isFriend), not(isLocal))),
        {
          quota: 2,
          backoffStep: 10e3,
          backoffMax: 10 * minute,
          groupMin: 5e3,
        },
      );

      if (numOfConnected === 0) {
        this.updateTheseConnections(peers.filter(isUnattempted), {
          quota: 1,
          backoffStep: 0,
          backoffMax: 0,
          groupMin: 0,
        });
      }

      //quota, groupMin, min, backoffStep, max
      this.updateTheseConnections(peers.filter(isInactive), {
        quota: 3,
        backoffStep: 5 * minute,
        backoffMax: 3 * hour,
        groupMin: 5 * 50e3,
      });

      const longterm = peers.filter(this.inConnection).filter(isLongterm)
        .length;

      this.updateTheseConnections(peers.filter(isLegacy), {
        quota: 3 - longterm,
        backoffStep: 5 * minute,
        backoffMax: 3 * hour,
        groupMin: 5 * minute,
      });
    }

    // Purge some ongoing frustrating connection attempts
    peers
      .filter(this.inConnection)
      .filter(p => {
        const permanent = isLongterm(p) || isLocal(p);
        return !permanent || this.hub.getState(p.address!) === 'connecting';
      })
      .filter(p => p.stateChange! + 10e3 < this.updateTimestamp)
      .forEach(p => this.hub.disconnect(p.address!));
  }

  private updateConnectionsSoon() {
    if (this.closed) return;

    if (this.hasScheduledAnUpdate) return;
    else this.hasScheduledAnUpdate = true;

    // Add some time randomization to avoid deadlocks with remote peers
    const timer = setTimeout(() => {
      this.updateConnectionsNow();
      this.hasScheduledAnUpdate = false;
    }, 1000 * Math.random());
    if (timer.unref) timer.unref();
  }

  @muxrpc('sync')
  public start = () => {
    // Upon wakeup, trigger hard reconnect
    onWakeup(() => this.hub.reset());

    // Upon network changes, trigger hard reconnect
    onNetwork(() => this.hub.reset());

    // Upon some disconnection, attempt to make connections
    pull(
      this.hub.listen(),
      pull.filter((ev: HubEvent) => ev.type === 'disconnected'),
      pull.drain(
        () => this.updateConnectionsSoon(),
        (...args: Array<any>) =>
          console.warn(
            '[gossip/dc] warning: this can happen if the database closes',
            args,
          ),
      ),
    );

    // Upon regular time intervals, attempt to make connections
    const int = setInterval(this.updateConnectionsSoon, 2e3);
    if (int.unref) int.unref();

    // Upon init, attempt to make some connections
    this.updateConnectionsSoon();

    // Upon init, populate with seeds and pubs
    if (this.config.offline) {
      console.log('Running in offline mode: gossip disabled');
    } else {
      // Populate gossip table with configured seeds (mainly used in testing)
      const seeds = this.config.seeds;
      (Array.isArray(seeds) ? seeds : [seeds]).filter(Boolean).forEach(addr => {
        this.ssb.gossip.add(addr, 'seed');
      });

      // Populate gossip table with pub announcements on the feed
      // (allow this to be disabled via config)
      if (
        !this.config.gossip ||
        (this.config.gossip.autoPopulate !== false &&
          this.config.gossip.pub !== false)
      ) {
        type PubContent = {address?: string};
        pull(
          this.ssb.messagesByType({type: 'pub', live: true, keys: false}),
          pull.filter((msg: any) => !msg.sync),
          pull.filter(
            (msg: Msg<PubContent>['value']) =>
              msg.content &&
              msg.content.address &&
              ref.isAddress(msg.content.address),
          ),
          pull.drain(
            (msg: Msg<PubContent>['value']) => {
              this.ssb.gossip.add(msg.content.address, 'pub');
            },
            (...args: Array<any>) => {
              console.warn(
                '[gossip] warning: this can happen if the database closes',
                args,
              );
            },
          ),
        );
      }
    }

    // Upon init, populate gossip table from the database
    for (let [address, data] of this.db.entries()) {
      if (data.source === 'dht') {
        this.ssb.gossip.add(address, 'dht');
      } else if (data.source !== 'local' && data.source !== 'bt') {
        this.ssb.gossip.add(address, 'stored');
      }
    }
  };

  @muxrpc('sync')
  public stop = () => {
    this.closed = true;
  };
}
