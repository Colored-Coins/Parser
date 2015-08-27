# AssetID-Encoder
[![Build Status](https://travis-ci.org/Colored-Coins/AssetId.svg?branch=master)](https://travis-ci.org/Colored-Coins/AssetId) [![Coverage Status](https://coveralls.io/repos/Colored-Coins/AssetId/badge.svg?branch=master)](https://coveralls.io/r/Colored-Coins/AssetId?branch=master) [![npm version](https://badge.fury.io/js/cc-assetid-encoder.svg)](http://badge.fury.io/js/cc-assetid-encoder) [![npm version](http://slack.coloredcoins.org/badge.svg)](http://slack.coloredcoins.org)

Issuance-Encoder provides the encode/decode functions between a Colored Coins issuance Object to buffer

### Installation

```sh
$ npm install cc-assetid-encoder
```


### Encode Asset ID

Params:
    - BitcoinTransaction - An Object with contains at least the following properties:

```js
{
  'cc_data': [{
    'type': 'issuance',
    'lockStatus': false,
  }],
  'vin': [{
    'txid': '0f45f38a8bcd8331877267e0f3f5f8a4b3c716165e40db4eee34d52759ad954f',
    'vout': 2,

    // The following properties are optional but at least one is required in
    // case 'lockStatus' is FALSE
    'scriptSig': {
      'asm': '3045022100daf8f8d65ea908a28d90f700dc932ecb3b68f402b04ba92f987e8abd7080fcad02205ce81b698b8013b86813c9edafc9e79997610626c9dd1bfb49f60abee9daa43801 029b622e5f0f87f2be9f23c4d82f818a73e258a11c26f01f73c8b595042507a574',
    }
    'address': 'mxNTyQ3WdFMQE7SGVpSQGXnSDevGMLq7dg'
  }]
}


```

Returns a new Buffer holding the encoded issuance.

##### Example:

```js
var assetIdEncoder = require('cc-assetid-encoder')

var bitcoinTransaction = {
  'cc_data': [{
    'type': 'issuance',
    'lockStatus': false,
  }],
  'vin': [{
    'txid': '0f45f38a8bcd8331877267e0f3f5f8a4b3c716165e40db4eee34d52759ad954f',
    'vout': 2,
    'scriptSig': {
      'asm': '3045022100daf8f8d65ea908a28d90f700dc932ecb3b68f402b04ba92f987e8abd7080fcad02205ce81b698b8013b86813c9edafc9e79997610626c9dd1bfb49f60abee9daa43801 029b622e5f0f87f2be9f23c4d82f818a73e258a11c26f01f73c8b595042507a574',
    }
  }]
}

assetIdEncoder(bitcoinTransaction)
// Will print 'UeqXdjE86VRj4USYSn7hR95c9xqhmjb4Dv'

```


### Testing

In order to test you need to install [mocha] globaly on your machine

```sh
$ cd /"module-path"/cc-assetid-Encoder
$ mocha
```


License
----

MIT

[mocha]:https://www.npmjs.com/package/mocha