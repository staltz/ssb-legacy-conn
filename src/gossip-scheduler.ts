import ConnDB = require('ssb-conn-db');
import ConnHub = require('ssb-conn-hub');
import {ListenEvent as HubEvent} from 'ssb-conn-hub/lib/types';
import ConnStaging = require('ssb-conn-staging');
import {Peer} from './types';
import {plugin, muxrpc} from 'secret-stack-decorators';
const pull = require('pull-stream');
const ip = require('ip');
const onWakeup = require('on-wakeup');
const onNetwork = require('on-change-network');
const hasNetwork = require('./has-network-debounced');
const ref = require('ssb-ref');

function not(fn: Function) {
  return function(e: any) {
    return !fn(e);
  };
}

function and(...args: Array<any>) {
  return function(value: any) {
    return args.every(function(fn: Function) {
      return fn.call(null, value);
    });
  };
}

//min delay (delay since last disconnect of most recent peer in unconnected set)
//unconnected filter delay peer < min delay
function delay(failures: number, factor: number, max: number) {
  return Math.min(Math.pow(2, failures) * factor, max || Infinity);
}

function maxStateChange(M: number, peer: Peer) {
  return Math.max(M, peer.stateChange || 0);
}

function peerNext(peer: Peer, opts: any) {
  return (
    (peer.stateChange! | 0) + delay(peer.failure! | 0, opts.factor, opts.max)
  );
}

//detect if not connected to wifi or other network
//(i.e. if there is only localhost)

function isOffline(p: Peer) {
  if (ip.isLoopback(p.host) || p.host == 'localhost') return false;
  else if (p.source === 'bt') return false;
  else return !hasNetwork();
}

var isOnline = not(isOffline);

function isLocal(p: Peer) {
  // don't rely on private ip address, because
  // cjdns creates fake private ip addresses.
  // ignore localhost addresses, because sometimes they get broadcast.
  return !ip.isLoopback(p.host) && ip.isPrivate(p.host) && p.source === 'local';
}

function isSeed(p: Peer) {
  return p.source === 'seed';
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

// Sort from oldest to newest, then take first n
function earliest(peers: Array<Peer>, n: number) {
  return peers
    .sort((a, b) => a.stateChange! - b.stateChange!)
    .slice(0, Math.max(n, 0));
}

function convertModeToLegacySource(
  mode: 'lan' | 'bt' | 'internet',
): Peer['source'] {
  if (mode === 'lan') return 'local';
  if (mode === 'bt') return 'bt';
  if (mode === 'internet') return 'pub';
  return 'stored';
}

const min = 60e3;
const hour = 60 * 60e3;

@plugin('1.0.0')
export class GossipScheduler {
  private ssb: any;
  private config: any;
  private closed: boolean;
  private lastMessageAt: number;
  private connecting: boolean;

  constructor(ssb: any, config: any) {
    this.ssb = ssb;
    this.config = config;
    this.closed = false;
    this.lastMessageAt = 0;
    this.connecting = false;

    this.ssb.post((data: any) => {
      if (data.value.author != this.ssb.id) {
        this.lastMessageAt = Date.now();
      }
    });
  }

  // Utility to pick from config, with some defaults
  private conf(name: any, def: any) {
    if (this.config.gossip == null) return def;
    var value = this.config.gossip[name];
    return value == null || value === '' ? def : value;
  }

  private isCurrentlyDownloading() {
    // don't schedule gossip if currently downloading messages
    return this.lastMessageAt && this.lastMessageAt > Date.now() - 500;
  }

  private isConnect(p: Peer) {
    const state = (this.ssb.gossip.hub() as ConnHub).getState(p.address!);
    return state === 'connected' || state === 'connecting';
  }

  private select(peers: Array<Peer>, ts: number, filter: any, opts: any) {
    if (opts.disable) return [];
    //opts: { quota, groupMin, min, factor, max }
    var type = peers.filter(filter);
    var unconnect = type.filter(not(this.isConnect));
    var count = Math.max(opts.quota - type.filter(this.isConnect).length, 0);
    var min = unconnect.reduce(maxStateChange, 0) + opts.groupMin;
    if (ts < min) return [];

    return earliest(unconnect.filter(peer => peerNext(peer, opts) < ts), count);
  }

  // Utility to connect to bunch of peers, or disconnect if over quota
  private connectToSome(
    peers: Array<Peer>,
    ts: number,
    name: string,
    filter: any,
    opts: any,
  ) {
    opts.group = name;
    const connected = peers.filter(this.isConnect).filter(filter);

    //disconnect if over quota
    if (connected.length > opts.quota * 2) {
      return earliest(connected, connected.length - opts.quota).forEach(
        peer => {
          (this.ssb.gossip.hub() as ConnHub)
            .disconnect(peer.address!)
            .then(() => {}, _err => {});
        },
      );
    }

    //will return [] if the quota is full
    const selected = this.select(peers, ts, and(filter, isOnline), opts);
    selected.forEach(peer => {
      (this.ssb.gossip.hub() as ConnHub)
        .connect(peer.address!)
        .then(() => {}, _err => {});
    });
  }

  private attemptNewConnections() {
    if (this.connecting || this.closed) return;
    this.connecting = true;
    // Add some time randomization
    var timer = setTimeout(() => {
      this.connecting = false;

      // Respect some limits: don't attempt to connect while migration is running
      if (!this.ssb.ready() || this.isCurrentlyDownloading()) return;

      const ts = Date.now();
      const db = this.ssb.gossip.db() as ConnDB;
      const hub = this.ssb.gossip.hub() as ConnHub;
      const staging = this.ssb.gossip.staging() as ConnStaging;

      // The total database, from which to sample
      const peers: Array<Peer> = ([] as Array<Peer>)
        .concat(
          Array.from(db.entries()).map(([address, data]) => ({
            ...data,
            address,
          })),
        )
        .concat(
          Array.from(staging.entries()).map(([address, data]) => ({
            ...data,
            address,
            source: convertModeToLegacySource(data.mode) as Peer['source'],
          })),
        );

      const numOfConnected = peers.filter(
        and(this.isConnect, not(isLocal), not(isFriend)),
      ).length;

      const numOfConnectedFriends = peers.filter(and(this.isConnect, isFriend))
        .length;

      if (this.conf('friends', true))
        this.connectToSome(peers, ts, 'friends', isFriend, {
          quota: 3,
          factor: 2e3,
          max: 10 * min,
          groupMin: 1e3,
        });

      if (this.conf('seed', true))
        this.connectToSome(peers, ts, 'seeds', isSeed, {
          quota: 3,
          factor: 2e3,
          max: 10 * min,
          groupMin: 1e3,
        });

      if (this.conf('local', true))
        this.connectToSome(peers, ts, 'local', isLocal, {
          quota: 3,
          factor: 2e3,
          max: 10 * min,
          groupMin: 1e3,
        });

      if (this.conf('global', true)) {
        // prioritize friends
        this.connectToSome(peers, ts, 'friends', and(isFriend, isLongterm), {
          quota: 2,
          factor: 10e3,
          max: 10 * min,
          groupMin: 5e3,
        });

        if (numOfConnectedFriends < 2)
          this.connectToSome(
            peers,
            ts,
            'attemptFriend',
            and(isFriend, isUnattempted),
            {
              min: 0,
              quota: 1,
              factor: 0,
              max: 0,
              groupMin: 0,
            },
          );

        this.connectToSome(
          peers,
          ts,
          'retryFriends',
          and(isFriend, isInactive),
          {
            min: 0,
            quota: 3,
            factor: 60e3,
            max: 3 * 60 * 60e3,
            groupMin: 5 * 60e3,
          },
        );

        // standard longterm peers
        this.connectToSome(
          peers,
          ts,
          'longterm',
          and(isLongterm, not(isFriend), not(isLocal)),
          {
            quota: 2,
            factor: 10e3,
            max: 10 * min,
            groupMin: 5e3,
          },
        );

        if (numOfConnected === 0)
          this.connectToSome(peers, ts, 'attempt', isUnattempted, {
            min: 0,
            quota: 1,
            factor: 0,
            max: 0,
            groupMin: 0,
          });

        //quota, groupMin, min, factor, max
        this.connectToSome(peers, ts, 'retry', isInactive, {
          min: 0,
          quota: 3,
          factor: 5 * 60e3,
          max: 3 * 60 * 60e3,
          groupMin: 5 * 50e3,
        });

        var longterm = peers.filter(this.isConnect).filter(isLongterm).length;

        this.connectToSome(peers, ts, 'legacy', isLegacy, {
          quota: 3 - longterm,
          factor: 5 * min,
          max: 3 * hour,
          groupMin: 5 * min,
        });
      }

      // Purge some ongoing frustrating connection attempts
      peers.filter(this.isConnect).forEach(p => {
        const permanent = isLongterm(p) || isLocal(p);
        if (
          (!permanent || hub.getState(p.address!) === 'connecting') &&
          p.stateChange! + 10e3 < ts
        ) {
          hub.disconnect(p.address!);
        }
      });
    }, 1000 * Math.random());
    if (timer.unref) timer.unref();
  }

  @muxrpc('sync')
  public start = () => {
    // Upon wakeup, trigger hard reconnect
    onWakeup(() => (this.ssb.gossip.hub() as ConnHub).reset());
    // Upon network changes, trigger hard reconnect
    onNetwork(() => (this.ssb.gossip.hub() as ConnHub).reset());

    // Upon some disconnection, attempt to make connections
    pull(
      (this.ssb.gossip.hub() as ConnHub).listen(),
      pull.drain(
        (ev: HubEvent) => {
          if (ev.type == 'disconnected') this.attemptNewConnections();
        },
        (...args: Array<any>) => {
          console.warn(
            '[gossip/dc] warning: this can happen if the database closes',
            args,
          );
        },
      ),
    );

    // Upon regular time intervals, attempt to make connections
    var int = setInterval(this.attemptNewConnections, 2e3);
    if (int.unref) int.unref();

    // Upon init, attempt to make some connections
    this.attemptNewConnections();

    // Upon init, populate with seeds and pubs
    if (this.config.offline) {
      console.log('Running in offline mode: gossip disabled');
    } else {
      // populate peertable with configured seeds (mainly used in testing)
      var seeds = this.config.seeds;
      (Array.isArray(seeds) ? seeds : [seeds]).filter(Boolean).forEach(addr => {
        this.ssb.gossip.add(addr, 'seed');
      });

      // populate peertable with pub announcements on the feed
      // (allow this to be disabled via config)
      if (
        !this.config.gossip ||
        (this.config.gossip.autoPopulate !== false &&
          this.config.gossip.pub !== false)
      )
        pull(
          this.ssb.messagesByType({
            type: 'pub',
            live: true,
            keys: false,
          }),
          pull.drain(
            (msg: any) => {
              if (msg.sync) return;
              if (!msg.content.address) return;
              if (ref.isAddress(msg.content.address))
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

    // Upon init, populate from the database
    for (let [address, data] of (this.ssb.gossip.db() as ConnDB).entries()) {
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
