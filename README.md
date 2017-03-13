# Colored Coins Block Parser
[![Build Status](https://travis-ci.org/Colored-Coins/Parser.svg?branch=master)](https://travis-ci.org/Colored-Coins/Parser) [![Coverage Status](https://coveralls.io/repos/Colored-Coins/Parser/badge.svg?branch=master)](https://coveralls.io/r/Colored-Coins/Parser?branch=master) [![npm version](https://badge.fury.io/js/cc-block-parser.svg)](http://badge.fury.io/js/cc-block-parser) [![npm version](http://slack.coloredcoins.org/badge.svg)](http://slack.coloredcoins.org)

[![js-standard-style](https://cdn.rawgit.com/feross/standard/master/badge.svg)](https://github.com/feross/standard)

Connects to Bitcoind (Json RPC) and parse the blockchain into mongodb including Colored-Coins asset data, utxo's for address and much more.

### Installation

```sh
$ npm i cc-block-parser
```


### Settings

```js
var settings = {
  debug:
  properties:
  logger:
  next_hash:
  last_hash:
  last_block:
  last_fully_parsed_block:
  Blocks:
  RawTransactions:
  Utxo:
  AddressesTransactions:
  AddressesUtxos:
  AssetsTransactions:
  AssetsUtxos:
  AssetsAddresses:
  host:
  port:
  user:
  pass:
  path:
  timeout:
}
```

License
----

[Apache-2.0](http://www.apache.org/licenses/LICENSE-2.0)

[mocha]:https://www.npmjs.com/package/mocha