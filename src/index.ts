const pull = require('pull-stream');
const Notify = require('pull-notify');
const valid = require('muxrpc-validation')({});
const ref = require('ssb-ref');
const ping = require('pull-ping');
const stats = require('statistics');
const AtomicFile = require('atomic-file');
const fs = require('fs');
const path = require('path');
const deepEqual = require('deep-equal');
import Schedule = require('./schedule');
import Init = require('./init');
import {Callback, Peer} from './types';

function isFunction(f: any): f is Function {
  return 'function' === typeof f;
}

function stringify(peer: Peer | string): string {
  //#region MODIFIED
  if (typeof peer === 'string') return peer;
  return [peer.source, peer.host, peer.port, peer.key].join(':');
  //#endregion
}

function isPeerObject(o: any): o is Peer {
  return o && 'object' == typeof o;
}

function toBase64(s: any): string {
  if (isString(s)) return s.substring(1, s.indexOf('.'));
  else return s.toString('base64'); //assume a buffer
}

function isString(s: any): s is string {
  return 'string' == typeof s;
}

function toAddressString(address: Peer | string): string {
  if (isPeerObject(address)) {
    if (ref.isAddress(address.address)) return address.address!;
    //#region MODIFIED
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
    //#endregion
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

/*
Peers : [{
  //modern:
  address: <multiserver address>,


  //legacy
  key: id,
  host: ip,
  port: int,

  //to be backwards compatible with patchwork...
  announcers: {length: int}
  //TODO: availability
  //availability: 0-1, //online probability estimate

  //where this peer was added from. TODO: remove "pub" peers.
  source: 'pub'|'manual'|'local'
}]
*/

//#region MODIFIED
function isDhtAddress(addr: any) {
  return typeof addr === 'string' && addr.substr(0, 4) === 'dht:';
}

function isBluetoothAddress(addr: any) {
  return typeof addr === 'string' && addr.substr(0, 3) === 'bt:';
}

function parseDhtAddress(addr: string): Peer {
  const [transport /*, transform */] = addr.split('~');
  const [dhtTag, seed, remoteId] = transport.split(':');
  if (dhtTag !== 'dht') throw new Error('Invalid DHT address ' + addr);
  return {
    host: seed + ':' + remoteId,
    port: 0,
    key: '@' + remoteId,
    source: 'dht',
  };
}

function parseBluetoothAddress(addr: string): Peer {
  const [transport, transform] = addr.split('~');

  const [btTag, addrWithoutColons] = transport.split(':');
  const [shsTag, remoteId] = transform.split(':');
  if (btTag !== 'bt') throw new Error('Invalid BT address ' + addr);
  if (shsTag !== 'shs') throw new Error('Invalid BT (SHS) address ' + addr);

  return {host: addrWithoutColons, port: 0, key: '@' + remoteId, source: 'bt'};
}
//#endregion

module.exports = {
  name: 'gossip',
  version: '1.0.0',
  manifest: {
    add: 'sync',
    connect: 'async',
    remove: 'sync',
    reconnect: 'sync',
    enable: 'sync',
    disable: 'sync',
    peers: 'sync',
    changes: 'source',
    ping: 'duplex',
  },
  permissions: {
    anonymous: {allow: ['ping']},
  },
  init: function(server: any, config: any) {
    const notify = Notify();
    let closeScheduler: any;

    const stateFile = AtomicFile(path.join(config.path, 'gossip.json'));

    const status: Record<string, Peer> = {};

    //Known Peers
    const peers: Array<Peer> = [];

    function getPeer(id: string): Peer | undefined {
      return peers.find(e => e && e.key === id);
    }

    function simplify(peer: Peer) {
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

    server.status.hook(function(fn: Function) {
      const _status = fn();
      _status.gossip = status;
      peers.forEach(peer => {
        if (peer.stateChange! + 3e3 > Date.now() || peer.state === 'connected')
          status[peer.key!] = simplify(peer);
      });
      return _status;
    });

    server.close.hook(function(this: any, fn: Function, args: Array<any>) {
      closeScheduler();
      for (let id in server.peers)
        server.peers[id].forEach((peer: any) => {
          peer.close(true);
        });
      return fn.apply(this, args);
    });

    const timer_ping = 5 * 6e4;

    function setConfig(name: string, value: any) {
      config.gossip = config.gossip || {};
      config.gossip[name] = value;

      const cfgPath = path.join(config.path, 'config');
      let existingConfig: any = {};

      // load ~/.ssb/config
      try {
        existingConfig = JSON.parse(fs.readFileSync(cfgPath, 'utf-8'));
      } catch (e) {}

      // update the plugins config
      existingConfig.gossip = existingConfig.gossip || {};
      existingConfig.gossip[name] = value;

      // write to disc
      fs.writeFileSync(
        cfgPath,
        JSON.stringify(existingConfig, null, 2),
        'utf-8',
      );
    }

    var gossip = {
      wakeup: 0,
      peers: function() {
        return peers;
      },
      get: function(addr: Peer | string) {
        //addr = ref.parseAddress(addr)
        if (ref.isFeed(addr)) {
          return getPeer(addr as string);
        } else if (ref.isFeed((addr as Peer).key!)) {
          return getPeer((addr as Peer).key!);
        } else {
          throw new Error('must provide id:' + JSON.stringify(addr));
        }
      },
      connect: function(addr: Peer | string, cb: Callback<any>) {
        server.emit('log:info', ['ssb-server', stringify(addr), 'CONNECTING']);
        if (ref.isFeed(addr)) addr = gossip.get(addr)!;
        //#region MODIFIED
        if (isDhtAddress(addr)) {
          addr = parseDhtAddress(addr as string);
        } else if (isBluetoothAddress(addr)) {
          addr = parseBluetoothAddress(addr as string);
        } else if (typeof addr === 'string') {
          addr = ref.parseAddress(addr);
        }
        if (!addr || typeof addr != 'object')
          return cb(new Error('first param must be an address'));
        //#endregion

        if (!addr.key) return cb(new Error('address must have ed25519 key'));
        // add peer to the table, incase it isn't already.
        gossip.add(addr, addr.source || 'manual');
        const maybePeer = gossip.get(addr);
        if (!maybePeer) return cb();
        const p = maybePeer!;

        p.stateChange = Date.now();
        p.state = 'connecting';
        server.connect(toAddressString(p), (err: any, rpc: any) => {
          //#region MODIFIED
          if (err && err.message && /Already connected/.test(err.message)) {
            delete p.error;
            p.state = 'connected';
            p.failure = 0;
            notify({type: 'connect', peer: p});
          } else if (err) {
            //#endregion
            p.error = err.stack;
            p.state = undefined;
            p.failure = (p.failure || 0) + 1;
            p.stateChange = Date.now();
            notify({type: 'connect-failure', peer: p});
            server.emit('log:info', [
              'ssb-server',
              stringify(p),
              'ERR',
              err.message || err,
            ]);
            p.duration = stats(p.duration, 0);
            return cb && cb(err);
          } else {
            delete p.error;
            p.state = 'connected';
            p.failure = 0;
            //#region MODIFIED
            notify({type: 'connect', peer: p});
            //#endregion
          }
          cb && cb(null, rpc);
        });
      },

      disconnect: valid.async(function(addr: Peer | string, cb: any) {
        const peer = gossip.get(addr);
        if (!peer) return;

        peer.state = 'disconnecting';
        peer.stateChange = Date.now();
        if (!peer || !peer.disconnect) {
          cb && cb();
        } else {
          peer.disconnect(true, (_err: any) => {
            peer.stateChange = Date.now();
            cb && cb();
          });
        }
      }, 'string|object'),

      changes: function() {
        return notify.listen();
      },
      //add an address to the peer table.
      add: valid.sync(
        function(addr: Peer | string, source: Peer['source']) {
          //#region MODIFIED
          const addressString = toAddressString(addr);
          if (isDhtAddress(addr)) {
            addr = parseDhtAddress(addr as string);
          } else if (isBluetoothAddress(addr)) {
            addr = parseBluetoothAddress(addr as string);
          } else if (typeof addr === 'string') {
            addr = ref.parseAddress(addr);
          } else if (!addr || (addr.source !== 'dht' && addr.source !== 'bt')) {
            if (!ref.isAddress(addr))
              throw new Error('not a valid address:' + JSON.stringify(addr));
          }
          //#endregion
          const peerToAdd = addr as Peer;
          peerToAdd.address = addressString;
          // check that this is a valid address, and not pointing at self.

          if (peerToAdd.key === server.id) return;

          const existingPeer = gossip.get(addr);

          if (!existingPeer) {
            // new peer
            peerToAdd.source = source;
            peerToAdd.announcers = 1;
            peerToAdd.duration = peerToAdd.duration || (0 as any);
            peers.push(peerToAdd);
            notify({
              type: 'discover',
              peer: peerToAdd,
              source: source || 'manual',
            });
            return peerToAdd;
          } else {
            if (source === 'friends' || source === 'local') {
              // this peer is a friend or local,
              // override old source to prioritize gossip
              existingPeer.source = source;
            } else if (existingPeer.source !== 'local') {
              //don't count local over and over
              existingPeer.announcers!++;
            }

            return existingPeer;
          }
        },
        'string|object',
        'string?',
      ),
      remove: function(addr: Peer | string) {
        const peer = gossip.get(addr);
        const index = peers.indexOf(peer!);
        if (~index) {
          peers.splice(index, 1);
          notify({type: 'remove', peer: peer});
        }
      },
      ping: function() {
        let timeout = (config.timers && config.timers.ping) || 5 * 60e3;
        //between 10 seconds and 30 minutes, default 5 min
        timeout = Math.max(10e3, Math.min(timeout, 30 * 60e3));
        return ping({timeout: timeout});
      },
      reconnect: function() {
        for (var id in server.peers)
          if (id !== server.id)
            //don't disconnect local client
            server.peers[id].forEach((peer: any) => {
              peer.close(true);
            });
        return (gossip.wakeup = Date.now());
      },
      enable: valid.sync(function(type: string) {
        type = type || 'global';
        setConfig(type, true);
        if (type === 'local' && server.local && server.local.init) {
          server.local.init();
        }
        return 'enabled gossip type ' + type;
      }, 'string?'),
      disable: valid.sync(function(type: string) {
        type = type || 'global';
        setConfig(type, false);
        return 'disabled gossip type ' + type;
      }, 'string?'),
    };

    closeScheduler = Schedule(gossip, config, server);
    Init(gossip, config, server);
    //get current state

    server.on('rpc:connect', function onRpcConnect(
      rpc: any,
      isClient: boolean,
    ) {
      // if we're not ready, close this connection immediately
      if (!server.ready() && rpc.id !== server.id) return rpc.close();

      const peer = getPeer(rpc.id);
      //#region MODIFIED
      rpc._connectRetries = rpc._connectRetries || 0;
      if (!peer && isClient && rpc._connectRetries < 4) {
        setTimeout(() => {
          onRpcConnect(rpc, isClient);
        }, 200);
        rpc._connectRetries += 1;
        return;
      }
      //#endregion
      //don't track clients that connect, but arn't considered peers.
      //maybe we should though?
      else if (!peer) {
        if (rpc.id !== server.id) {
          server.emit('log:info', ['ssb-server', rpc.id, 'Connected']);
          rpc.on('closed', () => {
            server.emit('log:info', ['ssb-server', rpc.id, 'Disconnected']);
          });
        }
        return;
      }

      status[rpc.id] = simplify(peer);

      server.emit('log:info', ['ssb-server', stringify(peer), 'PEER JOINED']);
      //means that we have created this connection, not received it.
      peer.client = !!isClient;
      peer.state = 'connected';
      peer.stateChange = Date.now();
      peer.disconnect = (err: any, cb: Callback<any>) => {
        if (isFunction(err)) (cb = err), (err = null);
        rpc.close(err, cb);
      };

      if (isClient) {
        //default ping is 5 minutes...
        const pp = ping({serve: true, timeout: timer_ping}, () => {});
        peer.ping = {rtt: pp.rtt, skew: pp.skew};
        pull(
          pp,
          rpc.gossip.ping({timeout: timer_ping}, (err: any) => {
            if (err.name === 'TypeError') peer.ping!.fail = true;
          }),
          pp,
        );
      }

      rpc.on('closed', () => {
        delete status[rpc.id];
        server.emit('log:info', [
          'ssb-server',
          stringify(peer),
          [
            'DISCONNECTED. state was',
            peer.state,
            'for',
            (Date.now() - peer.stateChange!) / 1000,
            'seconds',
          ].join(' '),
        ]);
        //track whether we have successfully connected.
        //or how many failures there have been.
        const since = peer.stateChange!;
        peer.stateChange = Date.now();
        //      if(peer.state === 'connected') //may be "disconnecting"
        peer.duration = stats(peer.duration, peer.stateChange - since);
        peer.state = undefined;
        notify({type: 'disconnect', peer: peer});
      });

      notify({type: 'connect', peer: peer});
    });

    let last: any;
    stateFile.get((_err: any, ary: any) => {
      last = ary || [];
      if (Array.isArray(ary))
        ary.forEach(v => {
          delete v.state;
          // don't add local peers (wait to rediscover)
          // adding peers back this way means old format gossip.json
          // will be updated to having proper address values.
          //#region MODIFIED
          if (v.source === 'dht') {
            gossip.add(v, 'dht');
          } else if (v.source === 'bt') {
            gossip.add(v, 'bt');
          } else if (v.source !== 'local') {
            gossip.add(v, 'stored');
          }
          //#region MODIFIED
        });
    });

    const int = setInterval(() => {
      const copy: Array<Peer> = JSON.parse(JSON.stringify(peers));
      copy
        .filter(e => e.source !== 'local')
        .forEach(e => {
          delete e.state;
        });
      if (deepEqual(copy, last)) return;
      last = copy;
      stateFile.set(copy, (err: any) => {
        if (err) console.log(err);
      });
    }, 10 * 1000);

    if (int.unref) int.unref();

    if (server.conn) {
      throw new Error(
        'Cannot register ssb-legacy-conn because ' +
          'an existing conn plugin is already registered.',
      );
    } else {
      server.conn = gossip;
    }
    return gossip;
  },
};
