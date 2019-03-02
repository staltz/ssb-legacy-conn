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
  // var args = [].slice.call(arguments);
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

function maxStateChange(M: number, e: any) {
  return Math.max(M, e.stateChange || 0);
}

function peerNext(peer: any, opts: any) {
  return (
    (peer.stateChange | 0) + delay(peer.failure | 0, opts.factor, opts.max)
  );
}

//detect if not connected to wifi or other network
//(i.e. if there is only localhost)

function isOffline(e: any) {
  if (ip.isLoopback(e.host) || e.host == 'localhost') return false;
  return !hasNetwork();
}

var isOnline = not(isOffline);

function isLocal(e: any) {
  // don't rely on private ip address, because
  // cjdns creates fake private ip addresses.
  // ignore localhost addresses, because sometimes they get broadcast.
  return !ip.isLoopback(e.host) && ip.isPrivate(e.host) && e.source === 'local';
}

function isSeed(e: any) {
  return e.source === 'seed';
}

function isFriend(e: any) {
  return e.source === 'friends';
}

function isUnattempted(e: any) {
  return !e.stateChange;
}

//select peers which have never been successfully connected to yet,
//but have been tried.
function isInactive(e: any) {
  return e.stateChange && (!e.duration || e.duration.mean == 0);
}

function isLongterm(e: any) {
  return e.ping && e.ping.rtt && e.ping.rtt.mean > 0;
}

//peers which we can connect to, but are not upgraded.
//select peers which we can connect to, but are not upgraded to LT.
//assume any peer is legacy, until we know otherwise...
function isLegacy(peer: any) {
  return (
    peer.duration &&
    (peer.duration && peer.duration.mean > 0) &&
    !isLongterm(peer)
  );
}

function isConnect(e: any) {
  return 'connected' === e.state || 'connecting' === e.state;
}

//sort oldest to newest then take first n
function earliest(peers: any, n: number) {
  return peers
    .sort(function(a: any, b: any) {
      return a.stateChange - b.stateChange;
    })
    .slice(0, Math.max(n, 0));
}

function select(peers: Array<any>, ts: number, filter: any, opts: any) {
  if (opts.disable) return [];
  //opts: { quota, groupMin, min, factor, max }
  var type = peers.filter(filter);
  var unconnect = type.filter(not(isConnect));
  var count = Math.max(opts.quota - type.filter(isConnect).length, 0);
  var min = unconnect.reduce(maxStateChange, 0) + opts.groupMin;
  if (ts < min) return [];

  return earliest(
    unconnect.filter(function(peer) {
      return peerNext(peer, opts) < ts;
    }),
    count,
  );
}

export = function Schedule(gossip: any, config: any, server: any) {
  var min = 60e3,
    hour = 60 * 60e3,
    closed = false;

  //trigger hard reconnect after suspend or local network changes
  onWakeup(gossip.reconnect);
  onNetwork(gossip.reconnect);

  function conf(name: any, def: any) {
    if (config.gossip == null) return def;
    var value = config.gossip[name];
    return value == null || value === '' ? def : value;
  }

  function connect(
    peers: Array<any>,
    ts: number,
    name: any,
    filter: any,
    opts: any,
  ) {
    opts.group = name;
    var connected = peers.filter(isConnect).filter(filter);

    //disconnect if over quota
    if (connected.length > opts.quota) {
      return earliest(connected, connected.length - opts.quota).forEach(
        function(peer: any) {
          gossip.disconnect(peer, function() {});
        },
      );
    }

    //will return [] if the quota is full
    var selected = select(peers, ts, and(filter, isOnline), opts);
    selected.forEach(function(peer: any) {
      gossip.connect(peer, function() {});
    });
  }

  var lastMessageAt: any;
  server.post(function(data: any) {
    if (data.value.author != server.id) lastMessageAt = Date.now();
  });

  function isCurrentlyDownloading() {
    // don't schedule gossip if currently downloading messages
    return lastMessageAt && lastMessageAt > Date.now() - 500;
  }

  var connecting = false;
  function connections() {
    if (connecting || closed) return;
    connecting = true;
    var timer = setTimeout(function() {
      connecting = false;

      // don't attempt to connect while migration is running
      if (!server.ready() || isCurrentlyDownloading()) return;

      var ts = Date.now();
      var peers = gossip.peers();

      var connected = peers.filter(and(isConnect, not(isLocal), not(isFriend)))
        .length;

      var connectedFriends = peers.filter(and(isConnect, isFriend)).length;

      if (conf('friends', true))
        connect(
          peers,
          ts,
          'friends',
          isFriend,
          {
            quota: 3,
            factor: 2e3,
            max: 10 * min,
            groupMin: 1e3,
          },
        );

      if (conf('seed', true))
        connect(
          peers,
          ts,
          'seeds',
          isSeed,
          {
            quota: 3,
            factor: 2e3,
            max: 10 * min,
            groupMin: 1e3,
          },
        );

      if (conf('local', true))
        connect(
          peers,
          ts,
          'local',
          isLocal,
          {
            quota: 3,
            factor: 2e3,
            max: 10 * min,
            groupMin: 1e3,
          },
        );

      if (conf('global', true)) {
        // prioritize friends
        connect(
          peers,
          ts,
          'friends',
          and(isFriend, isLongterm),
          {
            quota: 2,
            factor: 10e3,
            max: 10 * min,
            groupMin: 5e3,
          },
        );

        if (connectedFriends < 2)
          connect(
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

        connect(
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
        connect(
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

        if (!connected)
          connect(
            peers,
            ts,
            'attempt',
            isUnattempted,
            {
              min: 0,
              quota: 1,
              factor: 0,
              max: 0,
              groupMin: 0,
            },
          );

        //quota, groupMin, min, factor, max
        connect(
          peers,
          ts,
          'retry',
          isInactive,
          {
            min: 0,
            quota: 3,
            factor: 5 * 60e3,
            max: 3 * 60 * 60e3,
            groupMin: 5 * 50e3,
          },
        );

        var longterm = peers.filter(isConnect).filter(isLongterm).length;

        connect(
          peers,
          ts,
          'legacy',
          isLegacy,
          {
            quota: 3 - longterm,
            factor: 5 * min,
            max: 3 * hour,
            groupMin: 5 * min,
          },
        );
      }

      peers.filter(isConnect).forEach(function(e: any) {
        var permanent = isLongterm(e) || isLocal(e);
        if (
          (!permanent || e.state === 'connecting') &&
          e.stateChange + 10e3 < ts
        ) {
          gossip.disconnect(e, function() {});
        }
      });
    }, 1000 * Math.random());
    if (timer.unref) timer.unref();
  }

  pull(
    gossip.changes(),
    pull.drain(
      function(ev: any) {
        if (ev.type == 'disconnect') connections();
      },
      function() {
        console.warn(
          '[gossip/dc] warning: this can happen if the database closes',
          arguments,
        );
      },
    ),
  );

  var int = setInterval(connections, 2e3);
  if (int.unref) int.unref();

  connections();

  return function onClose() {
    closed = true;
  };
};
