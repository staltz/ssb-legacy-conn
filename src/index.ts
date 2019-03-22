import ConnDB = require('ssb-conn-db');
import ConnHub = require('ssb-conn-hub');
import ConnStaging = require('./staging/index');
import {ListenEvent as HubEvent} from 'ssb-conn-hub/lib/types';
import Schedule = require('./schedule');
import Init = require('./init');
import {Callback, Peer} from './types';
const pull = require('pull-stream');
const Notify = require('pull-notify');
const ref = require('ssb-ref');
const ping = require('pull-ping');
const stats = require('statistics');
const fs = require('fs');
const path = require('path');

function isPeerObject(o: any): o is Peer {
  return o && 'object' == typeof o;
}

function toBase64(s: any): string {
  if (typeof s === 'string') return s.substring(1, s.indexOf('.'));
  else return s.toString('base64'); //assume a buffer
}

function toAddressString(address: Peer | string): string {
  if (isPeerObject(address)) {
    if (ref.isAddress(address.address)) return address.address!;
    if (address.source === 'dht') {
      return (
        ['dht', address.host].join(':') + '~' + 'noauth'
        // ['shs', toBase64(address.key)].join(':')
      );
    } else if (address.source === 'bt') {
      return (
        ['bt', address.host].join(':') +
        '~' +
        ['shs', toBase64(address.key)].join(':')
      );
    }
    let protocol = 'net';
    if (address.host && address.host.endsWith('.onion')) protocol = 'onion';
    return (
      [protocol, address.host, address.port].join(':') +
      '~' +
      ['shs', toBase64(address.key)].join(':')
    );
  }
  return address;
}

function isDhtAddress(addr: any) {
  return typeof addr === 'string' && addr.substr(0, 4) === 'dht:';
}

function parseDhtAddress(addr: string): Peer {
  const [transport /*, transform */] = addr.split('~');
  const [dhtTag, seed, remoteId] = transport.split(':');
  if (dhtTag !== 'dht') throw new Error('Invalid DHT address ' + addr);
  return {
    host: seed + ':' + remoteId,
    port: 0,
    key: remoteId[0] === '@' ? remoteId : '@' + remoteId,
    source: 'dht',
  };
}

function parseAddress(address: string) {
  if (isDhtAddress(address)) {
    return parseDhtAddress(address);
  } else {
    return ref.parseAddress(address);
  }
}

function simplifyPeerForStatus(peer: Peer) {
  return {
    address: peer.address || toAddressString(peer),
    source: peer.source,
    state: peer.state,
    stateChange: peer.stateChange,
    failure: peer.failure,
    client: peer.client,
    stats: {
      duration: peer.duration || undefined,
      rtt: peer.ping ? peer.ping.rtt : undefined,
      skew: peer.ping ? peer.ping.skew : undefined,
    },
  };
}

function validateAddr(addr: Peer | string): [string, any] {
  if (!addr || (typeof addr !== 'object' && typeof addr !== 'string')) {
    throw new Error('address should be an object or string');
  }
  const addressString = typeof addr === 'string' ? addr : toAddressString(addr);
  const parsed = typeof addr === 'object' ? addr : parseAddress(addressString);
  if (!parsed.key) throw new Error('address must have ed25519 key');
  if (!ref.isFeed(parsed.key)) throw new Error('key must be ed25519 public id');
  return [addressString, parsed];
}

module.exports = {
  name: 'gossip',
  version: '1.0.0',
  manifest: {
    add: 'sync',
    connect: 'async',
    remove: 'sync',
    reconnect: 'sync',
    peers: 'sync',
    changes: 'source',
    ping: 'duplex',
    enable: 'sync',
    disable: 'sync',
  },
  permissions: {
    anonymous: {allow: ['ping']},
  },
  init: function(server: any, config: any) {
    const notify = Notify();
    let closeScheduler: any;

    const connDB = new ConnDB({path: config.path, writeTimeout: 10e3});
    const connHub = new ConnHub(server);
    const connStaging = new ConnStaging(connHub);

    const status: Record<string, Peer> = {};

    // Add peer metadata (for all peers) to the ssb-server status API
    server.status.hook(function(fn: Function) {
      const _status = fn();
      _status.gossip = status;
      for (let [address, data] of connDB.entries()) {
        const state = connHub.getState(address);
        if (state === 'connected' || data.stateChange! + 3e3 > Date.now()) {
          if (data.key) {
            status[data.key] = simplifyPeerForStatus({...data, state});
          }
        }
      }
      return _status;
    });

    server.close.hook(function(this: any, fn: Function, args: Array<any>) {
      closeScheduler && closeScheduler();
      connDB.close();
      return fn.apply(this, args);
    });

    function setConfig(name: string, value: any) {
      // Update in-memory config
      config.gossip = config.gossip || {};
      config.gossip[name] = value;

      // Update file system config
      const cfgPath = path.join(config.path, 'config');
      let configInFS: any = {};
      try {
        configInFS = JSON.parse(fs.readFileSync(cfgPath, 'utf-8'));
      } catch (e) {}
      configInFS.gossip = configInFS.gossip || {};
      configInFS.gossip[name] = value;
      fs.writeFileSync(cfgPath, JSON.stringify(configInFS, null, 2), 'utf-8');
    }

    var gossip = {
      wakeup: 0,

      peers: function() {
        return Array.from(connDB.entries()).map(([address, data]) => {
          return {...data, address, state: connHub.getState(address)};
        });
      },

      // Is this API used 'externally' somehow? We don't use this internally,
      // but it's still used in tests so let's implement it anyway.
      get: function(addr: Peer | string) {
        if (ref.isFeed(addr)) {
          for (let [address, data] of connDB.entries()) {
            if (data.key === addr) {
              return {...data, address};
            }
          }
          return undefined;
        }
        const [addressString] = validateAddr(addr);
        const peer = connDB.get(addressString);
        if (!peer) return undefined;
        else {
          const [address, data] = peer;
          return {...data, address, state: connHub.getState(address)};
        }
      },

      connect: (addr: Peer | string, cb: Callback<any>) => {
        let addressString: string;
        try {
          [addressString] = validateAddr(addr);
        } catch (err) {
          return cb(err);
        }

        gossip.add(addressString, 'manual');

        connHub
          .connect(addressString)
          .then(result => cb && cb(null, result), err => cb && cb(err));
      },

      disconnect: (addr: Peer | string, cb: any) => {
        let addressString: string;
        try {
          [addressString] = validateAddr(addr);
        } catch (err) {
          return cb(err);
        }

        connHub
          .disconnect(addressString)
          .then(() => cb && cb(), err => cb && cb(err));
      },

      changes: function() {
        return notify.listen();
      },

      add: (addr: Peer | string, source: Peer['source']) => {
        const [addressString, parsed] = validateAddr(addr);
        if (parsed.key === server.id) return;

        if (source === 'local') {
          connStaging.stage(addressString, {
            mode: 'lan',
            host: parsed.host,
            port: parsed.port,
            key: parsed.key,
            address: addressString,
            announcers: 1,
            duration: 0,
          });
          notify({type: 'discover', peer: parsed, source: source || 'manual'});
          return parsed;
        }

        const existingPeer = connDB.get(addressString);
        if (!existingPeer) {
          connDB.set(addressString, {
            host: parsed.host,
            port: parsed.port,
            key: parsed.key,
            address: addressString,
            source: source,
            announcers: 1,
            duration: 0,
          });
          notify({type: 'discover', peer: parsed, source: source || 'manual'});
          return connDB.get(addressString) || parsed;
        } else {
          // Upgrade the priority to friend
          if (source === 'friends') {
            connDB.update(addressString, {source});
          } else {
            connDB.update(addressString, (prev: any) => ({
              announcers: prev.announcers + 1,
            }));
          }
          return connDB.get(addressString);
        }
      },

      remove: (addr: Peer | string) => {
        const [addressString] = validateAddr(addr);

        // TODO are we sure that connHub.disconnect() mirrors ssb-gossip?
        connHub.disconnect(addressString);
        connStaging.unstage(addressString);

        const peer = connDB.get(addressString);
        if (!peer) return;
        connDB.delete(addressString);
        notify({type: 'remove', peer: peer});
      },

      ping: () => {
        let timeout = (config.timers && config.timers.ping) || 5 * 60e3;
        //between 10 seconds and 30 minutes, default 5 min
        timeout = Math.max(10e3, Math.min(timeout, 30 * 60e3));
        return ping({timeout});
      },

      reconnect: () => {
        connHub.reset();
        return (gossip.wakeup = Date.now());
      },

      enable: (type: string) => {
        if (!!type && typeof type !== 'string') {
          throw new Error('enable() expects an optional string as argument');
        }

        const actualType = type || 'global';
        setConfig(actualType, true);
        if (actualType === 'local' && server.local && server.local.init) {
          server.local.init();
        }
        return 'enabled gossip type ' + actualType;
      },

      disable: (type: string) => {
        if (!!type && typeof type !== 'string') {
          throw new Error('disable() expects an optional string as argument');
        }

        const actualType = type || 'global';
        setConfig(actualType, false);
        return 'disabled gossip type ' + actualType;
      },
    };

    function setupPing(address: string, rpc: any) {
      const PING_TIMEOUT = 5 * 6e4; // 5 minutes
      const pp = ping({serve: true, timeout: PING_TIMEOUT}, () => {});
      connDB.update(address, {ping: {rtt: pp.rtt, skew: pp.skew}});
      pull(
        pp,
        rpc.gossip.ping({timeout: PING_TIMEOUT}, (err: any) => {
          if (err.name === 'TypeError') {
            connDB.update(address, (prev: any) => ({
              ping: {...(prev.ping || {}), fail: true},
            }));
          }
        }),
        pp,
      );
    }

    function onConnecting(ev: HubEvent) {
      connDB.update(ev.address, {
        stateChange: Date.now(),
      });
      server.emit('log:info', ['ssb-server', ev.address, 'CONNECTING']);
    }

    function onConnectingFailed(ev: HubEvent) {
      connDB.update(ev.address, (prev: any) => ({
        failure: (prev.failure || 0) + 1,
        stateChange: Date.now(),
        duration: stats(prev.duration, 0),
      }));
      const peer = {
        state: ev.type,
        address: ev.address,
        key: ev.key,
        ...connDB.get(ev.address),
      };
      notify({type: 'connect-failure', peer});
      const err = (ev.details && ev.details.message) || ev.details;
      server.emit('log:info', ['ssb-server', ev.address, 'ERR', err]);
    }

    function onConnected(ev: HubEvent) {
      connDB.update(ev.address, {
        stateChange: Date.now(),
        failure: 0,
      });
      const peer = {
        state: ev.type,
        address: ev.address,
        key: ev.key,
        ...connDB.get(ev.address),
      };
      if (ev.key) {
        status[ev.key] = simplifyPeerForStatus(peer);
      }
      server.emit('log:info', ['ssb-server', ev.address, 'PEER JOINED']);
      notify({type: 'connect', peer});

      if (ev.details.isClient) setupPing(ev.address, ev.details.rpc);
    }

    function onDisconnecting(ev: HubEvent) {
      connDB.update(ev.address, {
        stateChange: Date.now(),
      });
    }

    function onDisconnectingFailed(_ev: HubEvent) {
      // ssb-gossip does not handle this case
    }

    function onDisconnected(ev: HubEvent) {
      connDB.update(ev.address, (prev: any) => ({
        stateChange: Date.now(),
        duration: stats(prev.duration, Date.now() - prev.stateChange),
      }));
      const peer = {
        state: ev.type,
        address: ev.address,
        key: ev.key,
        ...connDB.get(ev.address),
      };
      if (ev.key) {
        delete status[ev.key];
      }
      server.emit('log:info', [
        'ssb-server',
        ev.address,
        [
          'DISCONNECTED. state was',
          peer.state,
          'for',
          (Date.now() - peer.stateChange!) / 1000,
          'seconds',
        ].join(' '),
      ]);
      notify({type: 'disconnect', peer});
    }

    pull(
      connHub.listen(),
      pull.drain((ev: HubEvent) => {
        if (ev.type === 'connecting') onConnecting(ev);
        if (ev.type === 'connecting-failed') onConnectingFailed(ev);
        if (ev.type === 'connected') onConnected(ev);
        if (ev.type === 'disconnecting') onDisconnecting(ev);
        if (ev.type === 'disconnecting-failed') onDisconnectingFailed(ev);
        if (ev.type === 'disconnected') onDisconnected(ev);
      }),
    );

    connDB.loaded().then(() => {
      closeScheduler = Schedule(config, server, connDB, connHub, connStaging);
      Init(gossip, config, server);
      for (let [address, data] of connDB.entries()) {
        if (data.source === 'dht') {
          gossip.add(address, 'dht');
        } else if (data.source !== 'local' && data.source !== 'bt') {
          gossip.add(address, 'stored');
        }
      }
    });

    return gossip;
  },
};
