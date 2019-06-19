import ConnDB = require('ssb-conn-db');
import ConnHub = require('ssb-conn-hub');
import ConnStaging = require('ssb-conn-staging');
import {ListenEvent as HubEvent} from 'ssb-conn-hub/lib/types';
import Schedule = require('./schedule');
import Init = require('./init');
import {Callback, Peer} from './types';
import {plugin, muxrpc} from 'secret-stack-decorators';
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
  }
  const legacyParsing = ref.parseAddress(address);
  if (legacyParsing) {
    return legacyParsing;
  } else if (ref.isAddress(address)) {
    return {key: ref.getKeyFromAddress(address)};
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

function inferSource(address: string): Peer['source'] {
  // We ASSUME this `address` is NOT in conn-db and is NOT a pub
  return address.startsWith('net:')
    ? 'local'
    : address.startsWith('bt:')
    ? 'bt'
    : 'manual';
}

@plugin('1.0.0')
class gossip {
  public wakeup: number;
  private ssb: any;
  private config: any;
  private status: Record<string, Peer>;
  private notify: any;
  private connDB: ConnDB;
  private connHub: ConnHub;
  private connStaging: ConnStaging;
  private closeScheduler: any;

  constructor(ssb: any, cfg: any) {
    this.ssb = ssb;
    this.config = cfg;
    this.wakeup = 0;
    this.status = {};
    this.notify = Notify();
    this.connDB = new ConnDB({
      path: this.config.path,
      writeTimeout: 10e3,
    });
    this.connHub = new ConnHub(this.ssb);
    this.connStaging = new ConnStaging(this.connHub);
    this.closeScheduler = null;

    this.setupStatusHook();
    this.setupCloseHook();
    this.setupConnectionListeners();
    this.setupInitialization();
  }

  /**
   * Add peer metadata (for all peers) to the ssb-server status API
   */
  private setupStatusHook() {
    const connDB = this.connDB;
    const connHub = this.connHub;
    const status = this.status;
    this.ssb.status.hook(function(fn: Function) {
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
  }

  private setupCloseHook() {
    const that = this;
    this.ssb.close.hook(function(this: any, fn: Function, args: Array<any>) {
      if (that.closeScheduler) that.closeScheduler();
      that.connDB.close();
      that.connHub.close();
      return fn.apply(this, args);
    });
  }

  private setupConnectionListeners() {
    pull(
      this.connHub.listen(),
      pull.drain((ev: HubEvent) => {
        if (ev.type === 'connecting') this.onConnecting(ev);
        if (ev.type === 'connecting-failed') this.onConnectingFailed(ev);
        if (ev.type === 'connected') this.onConnected(ev);
        if (ev.type === 'disconnecting') this.onDisconnecting(ev);
        if (ev.type === 'disconnecting-failed') this.onDisconnectingFailed(ev);
        if (ev.type === 'disconnected') this.onDisconnected(ev);
      }),
    );
  }

  private setupInitialization() {
    this.connDB.loaded().then(() => {
      this.closeScheduler = Schedule(
        this.config,
        this.ssb,
        this.connDB,
        this.connHub,
        this.connStaging,
      );
      Init(this, this.config, this.ssb);
      for (let [address, data] of this.connDB.entries()) {
        if (data.source === 'dht') {
          this.add(address, 'dht');
        } else if (data.source !== 'local' && data.source !== 'bt') {
          this.add(address, 'stored');
        }
      }
    });
  }

  private setConfig(name: string, value: any) {
    // Update in-memory config
    this.config.gossip = this.config.gossip || {};
    this.config.gossip[name] = value;

    // Update file system config
    const cfgPath = path.join(this.config.path, 'config');
    let configInFS: any = {};
    try {
      configInFS = JSON.parse(fs.readFileSync(cfgPath, 'utf-8'));
    } catch (e) {}
    configInFS.gossip = configInFS.gossip || {};
    configInFS.gossip[name] = value;
    fs.writeFileSync(cfgPath, JSON.stringify(configInFS, null, 2), 'utf-8');
  }

  private setupPing(address: string, rpc: any) {
    const PING_TIMEOUT = 5 * 6e4; // 5 minutes
    const pp = ping({serve: true, timeout: PING_TIMEOUT}, () => {});
    this.connDB.update(address, {ping: {rtt: pp.rtt, skew: pp.skew}});
    pull(
      pp,
      rpc.gossip.ping({timeout: PING_TIMEOUT}, (err: any) => {
        if (err && err.name === 'TypeError') {
          this.connDB.update(address, (prev: any) => ({
            ping: {...(prev.ping || {}), fail: true},
          }));
        }
      }),
      pp,
    );
  }

  private onConnecting(ev: HubEvent) {
    this.connDB.update(ev.address, {
      stateChange: Date.now(),
    });
    this.ssb.emit('log:info', ['ssb-server', ev.address, 'CONNECTING']);
  }

  private onConnectingFailed(ev: HubEvent) {
    this.connDB.update(ev.address, (prev: any) => ({
      failure: (prev.failure || 0) + 1,
      stateChange: Date.now(),
      duration: stats(prev.duration, 0),
    }));
    const peer = {
      state: ev.type,
      address: ev.address,
      key: ev.key,
      ...this.connDB.get(ev.address),
    };
    this.notify({type: 'connect-failure', peer});
    const err = (ev.details && ev.details.message) || ev.details;
    this.ssb.emit('log:info', ['ssb-server', ev.address, 'ERR', err]);
  }

  private onConnected(ev: HubEvent) {
    this.connDB.update(ev.address, {
      stateChange: Date.now(),
      failure: 0,
    });
    const peer = {
      state: ev.type,
      address: ev.address,
      key: ev.key,
      ...this.connDB.get(ev.address),
    };
    if (!this.connDB.has(ev.address)) peer.source = inferSource(ev.address);
    if (ev.key) {
      this.status[ev.key] = simplifyPeerForStatus(peer);
    }
    this.ssb.emit('log:info', ['ssb-server', ev.address, 'PEER JOINED']);
    this.notify({type: 'connect', peer});
    if (ev.details.isClient) this.setupPing(ev.address, ev.details.rpc);
  }

  private onDisconnecting(ev: HubEvent) {
    this.connDB.update(ev.address, {
      stateChange: Date.now(),
    });
  }

  private onDisconnectingFailed(_ev: HubEvent) {
    // ssb-gossip does not handle this case
  }

  private onDisconnected(ev: HubEvent) {
    this.connDB.update(ev.address, (prev: any) => ({
      stateChange: Date.now(),
      duration: stats(prev.duration, Date.now() - prev.stateChange),
    }));
    const peer = {
      state: ev.type,
      address: ev.address,
      key: ev.key,
      ...this.connDB.get(ev.address),
    };
    if (ev.key) {
      delete this.status[ev.key];
    }
    this.ssb.emit('log:info', [
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
    this.notify({type: 'disconnect', peer});
  }

  private idToAddr(id: any) {
    const addr = this.connDB.getAddressForId(id as string) as string;
    if (!addr) {
      throw new Error('no known address for peer:' + id);
    }
    return addr;
  }

  @muxrpc('sync')
  public peers = () => {
    const peers = Array.from(this.connDB.entries()).map(([address, data]) => {
      return {
        ...data,
        address,
        state: this.connHub.getState(address),
      };
    });

    // Add peers that are connected but are not in the cold database
    for (const [address, data] of this.connHub.entries()) {
      if (!this.connDB.has(address)) {
        const [, parsed] = validateAddr(address);
        peers.push({
          ...data,
          ...parsed,
          address,
          source: inferSource(address),
        });
      }
    }

    return peers;
  };

  // Is this API used 'externally' somehow? We don't use this internally,
  // but it's still used in tests and it's in the manifest
  @muxrpc('sync')
  public get = (addr: Peer | string) => {
    if (ref.isFeed(addr)) {
      for (let [address, data] of this.connDB.entries()) {
        if (data.key === addr) {
          return {...data, address};
        }
      }
      return undefined;
    }
    const [addressString] = validateAddr(addr);
    const peer = this.connDB.get(addressString);
    if (!peer) return undefined;
    else {
      const [address, data] = peer;
      return {
        ...data,
        address,
        state: this.connHub.getState(address),
      };
    }
  };

  @muxrpc('async')
  public connect = (addr: Peer | string, cb: Callback<any>) => {
    let addressString: string;
    try {
      const inputAddr = ref.isFeed(addr) ? this.idToAddr(addr) : addr;
      [addressString] = validateAddr(inputAddr);
    } catch (err) {
      return cb(err);
    }

    this.add(addressString, 'manual');

    this.connHub
      .connect(addressString)
      .then(result => cb && cb(null, result), err => cb && cb(err));
  };

  @muxrpc('async')
  public disconnect = (addr: Peer | string, cb: any) => {
    let addressString: string;
    try {
      const inputAddr = ref.isFeed(addr) ? this.idToAddr(addr) : addr;
      [addressString] = validateAddr(inputAddr);
    } catch (err) {
      return cb(err);
    }

    this.connHub
      .disconnect(addressString)
      .then(() => cb && cb(), err => cb && cb(err));
  };

  @muxrpc('source')
  public changes = () => {
    return this.notify.listen();
  };

  @muxrpc('sync')
  public add = (addr: Peer | string, source: Peer['source']) => {
    const [addressString, parsed] = validateAddr(addr);
    if (parsed.key === this.ssb.id) return;

    if (source === 'local') {
      this.connStaging.stage(addressString, {
        mode: 'lan',
        host: parsed.host,
        port: parsed.port,
        key: parsed.key,
        address: addressString,
        announcers: 1,
        duration: 0,
      });
      this.notify({
        type: 'discover',
        peer: {
          ...parsed,
          state: this.connHub.getState(addressString),
          source: source || 'manual',
        },
        source: source || 'manual',
      });
      return parsed;
    }

    const existingPeer = this.connDB.get(addressString);
    if (!existingPeer) {
      this.connDB.set(addressString, {
        host: parsed.host,
        port: parsed.port,
        key: parsed.key,
        address: addressString,
        source: source,
        announcers: 1,
        duration: 0,
      });
      this.notify({
        type: 'discover',
        peer: {
          ...parsed,
          state: this.connHub.getState(addressString),
          source: source || 'manual',
        },
        source: source || 'manual',
      });
      return this.connDB.get(addressString) || parsed;
    } else {
      // Upgrade the priority to friend
      if (source === 'friends') {
        this.connDB.update(addressString, {source});
      } else {
        this.connDB.update(addressString, (prev: any) => ({
          announcers: prev.announcers + 1,
        }));
      }
      return this.connDB.get(addressString);
    }
  };

  @muxrpc('sync')
  public remove = (addr: Peer | string) => {
    const [addressString] = validateAddr(addr);

    // TODO are we sure that connHub.disconnect() mirrors ssb-gossip?
    this.connHub.disconnect(addressString);
    this.connStaging.unstage(addressString);

    const peer = this.connDB.get(addressString);
    if (!peer) return;
    this.connDB.delete(addressString);
    this.notify({type: 'remove', peer: peer});
  };

  @muxrpc('duplex', {anonymous: 'allow'})
  public ping = () => {
    let timeout = (this.config.timers && this.config.timers.ping) || 5 * 60e3;
    //between 10 seconds and 30 minutes, default 5 min
    timeout = Math.max(10e3, Math.min(timeout, 30 * 60e3));
    return ping({timeout});
  };

  @muxrpc('sync')
  public reconnect = () => {
    this.connHub.reset();
    return (this.wakeup = Date.now());
  };

  @muxrpc('sync')
  public enable = (type: string) => {
    if (!!type && typeof type !== 'string') {
      throw new Error('enable() expects an optional string as argument');
    }

    const actualType = type || 'global';
    this.setConfig(actualType, true);
    if (actualType === 'local' && this.ssb.local && this.ssb.local.init) {
      this.ssb.local.init();
    }
    return 'enabled gossip type ' + actualType;
  };

  @muxrpc('sync')
  public disable = (type: string) => {
    if (!!type && typeof type !== 'string') {
      throw new Error('disable() expects an optional string as argument');
    }

    const actualType = type || 'global';
    this.setConfig(actualType, false);
    return 'disabled gossip type ' + actualType;
  };
}

module.exports = gossip;
