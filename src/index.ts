import {Gossip} from './gossip';
import {ConnScheduler} from './conn-scheduler';
const CONN = require('ssb-conn/core');

module.exports = [CONN, Gossip, ConnScheduler];
