const hasNetwork = require('has-network');

let lastCheck = 0;
let lastValue: any = null;

export = function hasNetworkDebounced() {
  if (lastCheck + 1e3 < Date.now()) {
    lastCheck = Date.now();
    lastValue = hasNetwork();
  }
  return lastValue;
};
