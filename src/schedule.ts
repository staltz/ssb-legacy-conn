import ConnDB = require('ssb-conn-db');
import ConnHub = require('ssb-conn-hub');
import {ListenEvent as HubEvent} from 'ssb-conn-hub/lib/types';
import ConnStaging = require('ssb-conn-staging');
import {Peer} from './types';
const pull = require('pull-stream');
const ip = require('ip');
const onWakeup = require('on-wakeup');
const onNetwork = require('on-change-network');
const hasNetwork = require('./has-network-debounced');

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

function makeIsConnect(connHub: ConnHub) {
  return function isConnect(p: Peer) {
    const state = connHub.getState(p.address!);
    return state === 'connected' || state === 'connecting';
  };
}

//sort oldest to newest then take first n
function earliest(peers: Array<Peer>, n: number) {
  return peers
    .sort((a, b) => a.stateChange! - b.stateChange!)
    .slice(0, Math.max(n, 0));
}

function makeSelect(connHub: ConnHub) {
  const isConnect = makeIsConnect(connHub);
  return function select(
    peers: Array<Peer>,
    ts: number,
    filter: any,
    opts: any,
  ) {
    if (opts.disable) return [];
    //opts: { quota, groupMin, min, factor, max }
    var type = peers.filter(filter);
    var unconnect = type.filter(not(isConnect));
    var count = Math.max(opts.quota - type.filter(isConnect).length, 0);
    var min = unconnect.reduce(maxStateChange, 0) + opts.groupMin;
    if (ts < min) return [];

    return earliest(unconnect.filter(peer => peerNext(peer, opts) < ts), count);
  };
}

function convertModeToLegacySource(
  mode: 'lan' | 'bt' | 'internet',
): Peer['source'] {
  if (mode === 'lan') return 'local';
  if (mode === 'bt') return 'bt';
  if (mode === 'internet') return 'pub';
  return 'stored';
}

export = function Schedule(
  config: any,
  server: any,
  connDB: ConnDB,
  connHub: ConnHub,
  connStaging: ConnStaging,
) {
  const min = 60e3;
  const hour = 60 * 60e3;
  let closed = false;

  // Upon wakeup, trigger hard reconnect
  onWakeup(() => connHub.reset());
  // Upon network changes, trigger hard reconnect
  onNetwork(() => connHub.reset());

  // Utility to pick from config, with some defaults
  function conf(name: any, def: any) {
    if (config.gossip == null) return def;
    var value = config.gossip[name];
    return value == null || value === '' ? def : value;
  }

  const select = makeSelect(connHub);
  const isConnect = makeIsConnect(connHub);

  // Utility to connect to bunch of peers, or disconnect if over quota
  function connectToSome(
    peers: Array<Peer>,
    ts: number,
    name: string,
    filter: any,
    opts: any,
  ) {
    opts.group = name;
    const connected = peers.filter(isConnect).filter(filter);

    //disconnect if over quota
    if (connected.length > opts.quota * 2) {
      return earliest(connected, connected.length - opts.quota).forEach(
        peer => {
          connHub.disconnect(peer.address!).then(() => {}, _err => {});
        },
      );
    }

    //will return [] if the quota is full
    const selected = select(peers, ts, and(filter, isOnline), opts);
    selected.forEach(peer => {
      connHub.connect(peer.address!).then(() => {}, _err => {});
    });
  }

  // Additional info that aids making queries
  var lastMessageAt: any;
  server.post(function(data: any) {
    if (data.value.author != server.id) lastMessageAt = Date.now();
  });

  function isCurrentlyDownloading() {
    // don't schedule gossip if currently downloading messages
    return lastMessageAt && lastMessageAt > Date.now() - 500;
  }

  var connecting = false;
  function attemptNewConnections() {
    if (connecting || closed) return;
    connecting = true;
    // Add some time randomization
    var timer = setTimeout(function() {
      connecting = false;

      // Respect some limits: don't attempt to connect while migration is running
      if (!server.ready() || isCurrentlyDownloading()) return;

      const ts = Date.now();

      // The total database, from which to sample
      const peers: Array<Peer> = ([] as Array<Peer>)
        .concat(
          Array.from(connDB.entries()).map(([address, data]) => ({
            ...data,
            address,
          })),
        )
        .concat(
          Array.from(connStaging.entries()).map(([address, data]) => ({
            ...data,
            address,
            source: convertModeToLegacySource(data.mode) as Peer['source'],
          })),
        );

      const numOfConnected = peers.filter(
        and(isConnect, not(isLocal), not(isFriend)),
      ).length;

      const numOfConnectedFriends = peers.filter(and(isConnect, isFriend))
        .length;

      if (conf('friends', true))
        connectToSome(peers, ts, 'friends', isFriend, {
          quota: 3,
          factor: 2e3,
          max: 10 * min,
          groupMin: 1e3,
        });

      if (conf('seed', true))
        connectToSome(peers, ts, 'seeds', isSeed, {
          quota: 3,
          factor: 2e3,
          max: 10 * min,
          groupMin: 1e3,
        });

      if (conf('local', true))
        connectToSome(peers, ts, 'local', isLocal, {
          quota: 3,
          factor: 2e3,
          max: 10 * min,
          groupMin: 1e3,
        });

      if (conf('global', true)) {
        // prioritize friends
        connectToSome(peers, ts, 'friends', and(isFriend, isLongterm), {
          quota: 2,
          factor: 10e3,
          max: 10 * min,
          groupMin: 5e3,
        });

        if (numOfConnectedFriends < 2)
          connectToSome(
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

        connectToSome(peers, ts, 'retryFriends', and(isFriend, isInactive), {
          min: 0,
          quota: 3,
          factor: 60e3,
          max: 3 * 60 * 60e3,
          groupMin: 5 * 60e3,
        });

        // standard longterm peers
        connectToSome(
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
          connectToSome(peers, ts, 'attempt', isUnattempted, {
            min: 0,
            quota: 1,
            factor: 0,
            max: 0,
            groupMin: 0,
          });

        //quota, groupMin, min, factor, max
        connectToSome(peers, ts, 'retry', isInactive, {
          min: 0,
          quota: 3,
          factor: 5 * 60e3,
          max: 3 * 60 * 60e3,
          groupMin: 5 * 50e3,
        });

        var longterm = peers.filter(isConnect).filter(isLongterm).length;

        connectToSome(peers, ts, 'legacy', isLegacy, {
          quota: 3 - longterm,
          factor: 5 * min,
          max: 3 * hour,
          groupMin: 5 * min,
        });
      }

      // Purge some ongoing frustrating connection attempts
      peers.filter(isConnect).forEach(p => {
        const permanent = isLongterm(p) || isLocal(p);
        if (
          (!permanent || connHub.getState(p.address!) === 'connecting') &&
          p.stateChange! + 10e3 < ts
        ) {
          connHub.disconnect(p.address!);
        }
      });
    }, 1000 * Math.random());
    if (timer.unref) timer.unref();
  }

  // Upon some disconnection, attempt to make connections
  pull(
    connHub.listen(),
    pull.drain(
      (ev: HubEvent) => {
        if (ev.type == 'disconnected') attemptNewConnections();
      },
      function() {
        console.warn(
          '[gossip/dc] warning: this can happen if the database closes',
          arguments,
        );
      },
    ),
  );

  // Upon regular time intervals, attempt to make connections
  var int = setInterval(attemptNewConnections, 2e3);
  if (int.unref) int.unref();

  // Upon init, attempt to make some connections
  attemptNewConnections();

  return function onClose() {
    closed = true;
  };
};
