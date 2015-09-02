var async = require('async')
var util = require('util')
var events = require('events')
var _ = require('lodash')
var CCTransaction = require('cc-transaction')
var assetIdencoder = require('cc-assetid-encoder')
var bitcoin = require('bitcoin-async')

var properties
var bitcoin_rpc
var debug

var Blocks = require(__dirname + '/model/blocks')
var RawTransactions = require(__dirname + '/model/rawtransactions')
var Utxo = require(__dirname + '/model/utxo')
var AddressesTransactions = require(__dirname + '/model/addressestransactions')
var AddressesUtxos = require(__dirname + '/model/addressesutxos')
var AssetsTransactions = require(__dirname + '/model/assetstransactions')
var AssetsUtxos = require(__dirname + '/model/assetsutxos')
var AssetsAddresses = require(__dirname + '/model/assetsaddresses')

function Scanner (settings, db) {
  debug = settings.debug
  properties = settings.properties
  this.next_hash = settings.next_hash
  this.last_hash = settings.last_hash
  this.last_block = settings.last_block
  this.last_fully_parsed_block = settings.last_fully_parsed_block
  bitcoin_rpc = new bitcoin.Client(settings.rpc_settings)

  db.model('blocks', Blocks)
  db.model('rawtransactions', RawTransactions)
  db.model('utxo', Utxo)
  db.model('addressestransactions', AddressesTransactions)
  db.model('addressesutxos', AddressesUtxos)
  db.model('assetstransactions', AssetsTransactions)
  db.model('assetsutxos', AssetsUtxos)
  db.model('assetsaddresses', AssetsAddresses)

  this.on('newblock', function (newblock) {
    process.send({newblock: newblock})
  })
  this.on('newtransaction', function (newtransaction) {
    process.send({newtransaction: newtransaction})
  })
  this.on('newcctransaction', function (newcctransaction) {
    process.send({newcctransaction: newcctransaction})
  })
  this.on('revertedblock', function (revertedblock) {
    process.send({revertedblock: revertedblock})
  })
  this.on('revertedtransaction', function (revertedtransaction) {
    process.send({revertedtransaction: revertedtransaction})
  })
  this.on('revertedcctransaction', function (revertedcctransaction) {
    process.send({revertedcctransaction: revertedcctransaction})
  })
  this.on('mempool', function () {
    process.send({mempool: true})
  })
}

util.inherits(Scanner, events.EventEmitter)

Scanner.prototype.scan_blocks = function (err) {
  var self = this
  if (err) {
    console.error(err)
    return self.scan_blocks()
  }
  var job
  var next_block
  var last_hash

  async.waterfall([
    function (cb) {
      self.get_next_new_block(cb)
    },
    function (l_next_block, l_last_hash, cb) {
      last_hash = l_last_hash
      next_block = l_next_block || 0
      self.get_raw_block(next_block, cb)
    },
    function (raw_block_data, cb) {
      if (!cb) {
        cb = raw_block_data
        setTimeout(function () {
          if (debug) {
            job = 'mempool_scan'
            console.time(job)
          }
          self.parse_new_mempool(cb)
        }, 500)
      } else {
        if (raw_block_data.height === next_block - 1 && raw_block_data.hash === last_hash) {
          setTimeout(function () {
            if (debug) {
              job = 'mempool_scan'
              console.time(job)
            }
            self.parse_new_mempool(cb)
          }, 500)
        } else if (!raw_block_data.previousblockhash || raw_block_data.previousblockhash === last_hash) {
          // logger.debug('parsing block')
          if (debug) {
            job = 'parse_new_block'
            console.time(job)
          }
          self.parse_new_block(raw_block_data, cb)
        } else {
          if (debug) {
            job = 'reverting_block'
            console.time(job)
          }
          self.revert_block(next_block - 1, cb)
        }
      }
    }
  ], function (err) {
    if (debug && job) console.timeEnd(job)
    self.scan_blocks(err)
  })
}

Scanner.prototype.revert_block = function (block_height, callback) {
  var self = this
  console.log('Reverting block: ' + block_height)
  var conditions = {
    height: block_height
  }
  Blocks.findOne(conditions).exec(function (err, block_data) {
    if (err) return callback(err)
    if (!block_data || !block_data.tx) return callback()
    var block_id = {
      height: block_data.height,
      hash: block_data.hash
    }
    var utxo_bulk = Utxo.collection.initializeOrderedBulkOp()
    // var holders_bulk = Holders.collection.initializeOrderedBulkOp()
    var addresses_transactions_bulk = AddressesTransactions.collection.initializeOrderedBulkOp()
    var addresses_utxos_bulk = AddressesUtxos.collection.initializeOrderedBulkOp()
    var assets_transactions_bulk = AssetsTransactions.collection.initializeOrderedBulkOp()
    var assets_utxos_bulk = AssetsUtxos.collection.initializeOrderedBulkOp()
    var raw_transaction_bulk = RawTransactions.collection.initializeOrderedBulkOp()

    // logger.debug('reverting '+block_data.tx.length+' txs.')
    var txids = []
    var colored_txids = []
    async.eachSeries(block_data.tx.reverse(), function (txid, cb) {
      txids.push(txid)
      self.revert_tx(txid, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, raw_transaction_bulk, function (err, colored) {
        if (err) return cb(err)
        if (colored) {
          colored_txids.push(txid)
        }
        cb()
      })
    },
    function (err) {
      if (err) return callback(err)
      // logger.debug('executing bulks')
      execute_bulks([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, raw_transaction_bulk], function (err) {
        if (err) return callback(err)
        txids.forEach(function (txid) {
          self.emit('revertedtransaction', {txid: txid})
        })
        colored_txids.forEach(function (txid) {
          self.emit('revertedcctransaction', {txid: txid})
        })
        // logger.debug('deleting block')
        Blocks.remove(conditions).exec(function (err) {
          if (err) return callback(err)
          // logger.debug('setting hashes')
          self.set_last_hash(block_data.previousblockhash)
          self.set_last_block(block_data.height - 1)
          self.set_next_hash(null)
          // logger.debug('done reverting')
          callback()
        })
      })
    })
  })
}

Scanner.prototype.revert_tx = function (txid, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, raw_transaction_bulk, callback) {
  // var self = this
  var conditions = {
    txid: txid
  }
  // logger.debug('reverting tx '+txid)
  RawTransactions.findOne(conditions).exec(function (err, tx) {
    if (err) return callback(err)
    if (!tx) return callback()
    async.waterfall([
      function (cb) {
        // logger.debug('reverting vin')
        revert_vin(txid, tx.vin, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, cb)
      },
      function (cb) {
        // logger.debug('reverting vout')
        revert_vout(tx.txid, tx.vout, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk)
        // logger.debug('vout reverted')
        cb()
      }
    ],
    function (err) {
      if (err) return callback(err)
      raw_transaction_bulk.find(conditions).remove()
      // logger.debug('tx '+txid+' reverted.')
      callback(null, tx.colored)
    })
  })
}

var revert_vin = function (txid, vins, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, callback) {
  if (!vins || !vins.length || vins[0].coinbase) return callback()
  var conditions = []
  vins.forEach(function (vin) {
    conditions.push({
      txid: vin.txid,
      index: vin.vout,
      used: true
    })
  })
  conditions = {
    $or: conditions
  }
  Utxo.find(conditions).exec(function (err, useds) {
    if (err) return callback(err)
    if (!useds || !useds.length) return callback()
    useds.forEach(function (used) {
      if (used.addresses) {
        used.addresses.forEach(function (address) {
          var address_tx = {
            address: address,
            txid: txid
          }
          addresses_transactions_bulk.find(address_tx).remove()

          if (used.assets && used.assets.length) {
            used.assets.forEach(function (asset) {
              var asset_tx = {
                assetId: asset.assetId,
                txid: txid
              }
              assets_transactions_bulk.find(asset_tx).remove()
            })
          }
        })
      }
      var cond = {
        txid: used.txid,
        index: used.index
      }
      utxo_bulk.find(cond).updateOne({
        $set: {
          used: false,
          usedBlockheight: null,
          usedTxid: null
        }
      })
    })
    callback()
  })
}

var revert_vout = function (txid, vouts, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk) {
  if (!vouts || !vouts.length) return false
  // var to_remove = []
  vouts.forEach(function (vout) {
    var conditions = {
      txid: txid,
      index: vout.n
    }
    utxo_bulk.find(conditions).remove()
    if (vout.addresses) {
      vout.addresses.forEach(function (address) {
        addresses_utxos_bulk.find({
          address: address,
          utxo: txid + ':' + vout.n
        }).remove()
        addresses_transactions_bulk.find({
          address: address,
          txid: txid
        }).remove()
      })
    }
    if (vout.assets && vout.assets.length) {
      vout.assets.forEach(function (asset) {
        var asset_tx = {
          assetId: asset.assetId,
          txid: txid
        }
        assets_transactions_bulk.find(asset_tx).remove()

        var asset_utxo = {
          assetId: asset.assetId,
          utxo: txid + ':' + vout.n
        }
        assets_utxos_bulk.find(asset_utxo).remove()
      })
    }
  })
}

Scanner.prototype.scan_mempol_only = function (err) {
  var self = this
  // logger.debug('scanning mempool')
  if (err) {
    console.error(err)
    return self.scan_mempol_only()
  }
  self.parse_new_mempool(function (err) {
    setTimeout(function () {
      self.scan_mempol_only(err)
    }, 500)
  })
}

Scanner.prototype.fix_blocks = function (err, callback) {
  var self = this
  if (err) {
    console.error(err)
    return self.fix_blocks(null, callback)
  }
  var emits = []
  callback = callback || function (err) {
    self.fix_blocks(err)
  }
  self.get_next_block_to_fix(50, function (err, raw_block_datas) {
    if (err) return self.fix_blocks(err)
    if (!raw_block_datas || !raw_block_datas.length) {
      return setTimeout(function () {
        callback()
      }, 500)
    }
    var first_block = raw_block_datas[0].height
    var num_of_blocks = raw_block_datas.length
    var last_block = raw_block_datas[num_of_blocks - 1].height
    // if (last_block - first_block + 1 !== num_of_blocks) {
    //   return callback('num_of_blocks: '+num_of_blocks+' first_block: '+first_block+' last_block: '+last_block)
    // }
    // var next_block = raw_block_data.height

    var close_blocks = function (err, empty) {
      if (debug) console.timeEnd('vin_insert_bulks')
      if (err) return self.fix_blocks(err)
      emits.forEach(function (emit) {
        self.emit(emit[0], emit[1])
      })
      if (empty) {
        var bulk = Blocks.collection.initializeUnorderedBulkOp()
        raw_block_datas.forEach(function (raw_block_data) {
          bulk.find({hash: raw_block_data.hash}).updateOne({
            $set: {
              txsparsed: true,
              ccparsed: false
            }
          })
        })
        if (bulk.length) {
          bulk.execute(callback)
        } else {
          callback(err)
        }
      } else {
        callback(err)
      }
    }

    var utxo_bulk = Utxo.collection.initializeUnorderedBulkOp()
    utxo_bulk.bulk_name = 'utxo_bulk'
    var addresses_transactions_bulk = AddressesTransactions.collection.initializeUnorderedBulkOp()
    addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
    var addresses_utxos_bulk = AddressesUtxos.collection.initializeUnorderedBulkOp()
    addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
    var raw_transaction_bulk = RawTransactions.collection.initializeUnorderedBulkOp()
    raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
    var assets_utxos_bulk = AssetsUtxos.collection.initializeUnorderedBulkOp()
    assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'

    self.get_need_to_fix_transactions_by_blocks(first_block, last_block, function (err, transactions_data) {
      if (err) return self.fix_blocks(err)
      console.log('Fixing blocks ' + first_block + '-' + last_block + ' (' + transactions_data.length + ' txs).')
        // var tx_to_fix_count = transactions_data.length
      // logger.debug('fixing '+tx_to_fix_count+' txs.')
      if (!transactions_data || !transactions_data.length) {
        // logger.debug('no need to fix')
        if (debug) console.time('vin_insert_bulks')
        return close_blocks(null, true)
      }
      async.each(transactions_data, function (transaction_data, cb) {
        // logger.debug('fixing txid:', transaction_data.txid)
        self.parse_vin(transaction_data, transaction_data.blockheight, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_utxos_bulk, function (err, coinbase, all_fixed) {
          if (err) return cb(err)
          raw_transaction_bulk.find({txid: transaction_data.txid}).updateOne({
            $set: {
              iosparsed: all_fixed,
              vin: transaction_data.toObject().vin,
              tries: transaction_data.tries || 0
            }
          })
          // transaction_data.save(cb)
          if (!transaction_data.colored && all_fixed) {
            emits.push(['newtransaction', transaction_data])
            // self.emit('newtransaction', transaction_data)
          }
          cb()
        })
      },
      function (err) {
        if (err) return callback(err)
        if (debug) console.time('vin_insert_bulks')
        execute_bulks_parallel([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, raw_transaction_bulk, assets_utxos_bulk], close_blocks)
      })
    })
  })
}

Scanner.prototype.parse_cc = function (err, callback) {
  var self = this
  if (err) {
    console.error(err)
    return self.parse_cc()
  }
  var emits = []
  callback = callback || function (err) {
    self.parse_cc(err)
  }

  self.get_next_block_to_cc_parse(500, function (err, raw_block_datas) {
    if (err) return self.parse_cc(err)
    if (!raw_block_datas || !raw_block_datas.length) {
      return setTimeout(function () {
        callback()
      }, 500)
    }
    var first_block = raw_block_datas[0].height
    var num_of_blocks = raw_block_datas.length
    var last_block = raw_block_datas[num_of_blocks - 1].height
    // if (last_block - first_block + 1 !== num_of_blocks) {
    //   logger.debug('!!'+JSON.stringify(raw_block_datas[num_of_blocks-1]))
    //   return callback('num_of_blocks: '+num_of_blocks+' first_block: '+first_block+' last_block: '+last_block)
    // }
    // var next_block = raw_block_data.height

    var close_blocks = function (err, empty) {
      // logger.debug('closing')
      if (debug) console.timeEnd('parse_cc_bulks')
      if (err) return self.parse_cc(err)
      emits.forEach(function (emit) {
        self.emit(emit[0], emit[1])
      })
      if (empty) {
        var bulk = Blocks.collection.initializeUnorderedBulkOp()
        raw_block_datas.forEach(function (raw_block_data) {
          if (raw_block_data.txsparsed) {
            self.emit('newblock', raw_block_data)
            self.set_last_fully_parsed_block(raw_block_data.height)
            // var conditions = {
            //   hash: raw_block_data.hash
            //   // txsparsed: true
            // }
            bulk.find({hash: raw_block_data.hash}).updateOne({
              $set: {
                ccparsed: true
              }
            })
          }
        })
        if (bulk.length) {
          bulk.execute(callback)
        } else {
          callback(err)
        }
      } else {
        callback(err)
      }
    }

    var utxo_bulk = Utxo.collection.initializeUnorderedBulkOp()
    utxo_bulk.bulk_name = 'utxo_bulk'
    var raw_transaction_bulk = RawTransactions.collection.initializeUnorderedBulkOp()
    raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
    var assets_transactions_bulk = AssetsTransactions.collection.initializeUnorderedBulkOp()
    assets_transactions_bulk.bulk_name = 'assets_transactions_bulk'
    var assets_utxos_bulk = AssetsUtxos.collection.initializeUnorderedBulkOp()
    assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'
    var assets_addresses_bulk = AssetsAddresses.collection.initializeUnorderedBulkOp()
    assets_addresses_bulk.bulk_name = 'assets_addresses_bulk'

    self.get_need_to_cc_parse_transactions_by_blocks(first_block, last_block, function (err, transactions_data) {
      if (err) return self.parse_cc(err)
      console.log('Parsing cc for blocks ' + first_block + '-' + last_block + ' (' + transactions_data.length + ' txs).')
        // var tx_to_fix_count = transactions_data.length
      // logger.debug('fixing '+tx_to_fix_count+' txs.')
      if (!transactions_data || !transactions_data.length) {
        // logger.debug('no need to fix')
        if (debug) console.time('parse_cc_bulks')
        return close_blocks(null, true)
      }
      transactions_data.forEach(function (transaction_data) {
        transaction_data = transaction_data.toObject()
        // var raw_block_data = raw_block_datas[transaction_data.blockheight - first_block]
        self.parse_cc_tx(transaction_data, utxo_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk)

        if (transaction_data.iosparsed) {
          var conditions = {
            iosparsed: true,
            txid: transaction_data.txid
          }
          raw_transaction_bulk.find(conditions).updateOne({
            $set: {
              vout: transaction_data.vout,
              ccparsed: true,
              overflow: transaction_data.overflow || false
            }
          })
          emits.push(['newcctransaction', transaction_data])
          // self.emit('newcctransaction', transaction_data)
        }
      })
      // logger.debug('executing vins bulks')
      if (debug) console.time('parse_cc_bulks')
      execute_bulks_parallel([utxo_bulk, raw_transaction_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk], close_blocks)
    })
  })
}

Scanner.prototype.parse_cc_tx = function (transaction_data, utxo_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk) {
  // logger.debug('parsing cc: '+transaction_data.txid)
  if (transaction_data.iosparsed && transaction_data.ccdata && transaction_data.ccdata.length) {
    var assets = get_assets_outputs(transaction_data)
    assets.forEach(function (asset, out_index) {
      // logger.debug('found cc asset '+JSON.stringify(asset)+' in tx: '+transaction_data.txid)
      if (asset) {
        transaction_data.vout[out_index].assets = asset
        var conditions = {
          txid: transaction_data.txid,
          index: out_index
        }
        utxo_bulk.find(conditions).update({$set: {
          assets: asset
        }})
        asset.forEach(function (one_asset) {
          var type = null
          if (transaction_data.ccdata && transaction_data.ccdata.length && transaction_data.ccdata[0].type) {
            type = transaction_data.ccdata[0].type
          }
          var txids_conditions = {
            assetId: one_asset.assetId,
            txid: transaction_data.txid,
            type: type
          }
          assets_transactions_bulk.find(txids_conditions).upsert().updateOne(txids_conditions)
          var utxos_conditions = {
            assetId: one_asset.assetId,
            utxo: transaction_data.txid + ':' + out_index
          }
          assets_utxos_bulk.find(utxos_conditions).upsert().updateOne(utxos_conditions)
          if (one_asset.amount && transaction_data.vout[out_index].scriptPubKey && transaction_data.vout[out_index].scriptPubKey.addresses) {
            transaction_data.vout[out_index].scriptPubKey.addresses.forEach(function (address) {
              var addresses_conditions = {
                assetId: one_asset.assetId,
                address: address
              }
              assets_addresses_bulk.find(addresses_conditions).upsert().updateOne(addresses_conditions)
            })
          }
        })
      }
    })
  }
}

Scanner.prototype.get_need_to_cc_parse_transactions_by_blocks = function (first_block, last_block, callback) {
  // var self = this

  var conditions = {
    // iosparsed: true,
    colored: true,
    ccparsed: false,
    // 'vin.coinbase': null,
    blockheight: {$gte: first_block, $lte: last_block}
  }

  RawTransactions.find(conditions).sort('blockheight').limit(1000).exec(callback)
}

Scanner.prototype.get_need_to_fix_transactions_by_blocks = function (first_block, last_block, callback) {
  // var self = this
  // logger.debug('get_need_to_fix_transactions_by_blocks: '+first_block+'-'+last_block)
  var conditions = {
    iosparsed: false,
    blockheight: {$gte: first_block, $lte: last_block}
  }
  var sort = {
    blockheight: 1,
    tries: 1
  }
  RawTransactions.find(conditions).sort(sort).limit(200).exec(callback)
}

Scanner.prototype.set_next_hash = function (next_hash) {
  // var self = this
  properties.next_hash = next_hash
  // process.send({last_hash: last_hash})
}

Scanner.prototype.set_last_hash = function (last_hash) {
  // var self = this
  properties.last_hash = last_hash
  // process.send({last_hash: last_hash})
}

Scanner.prototype.set_last_block = function (last_block) {
  // var self = this
  properties.last_block = last_block
  process.send({last_block: last_block})
}

Scanner.prototype.set_last_fully_parsed_block = function (last_fully_parsed_block) {
  // var self = this
  properties.last_fully_parsed_block = last_fully_parsed_block
  process.send({last_fully_parsed_block: last_fully_parsed_block})
}

Scanner.prototype.get_next_block_to_cc_parse = function (limit, callback) {
  // var self = this
  var conditions = {
    ccparsed: false
  }
  var projection = {
    height: 1,
    hash: 1,
    time: 1,
    size: 1,
    totalsent: 1,
    fees: 1,
    confirmations: 1,
    txlength: 1,
    txsparsed: 1
  }
  Blocks.find(conditions, projection)
  .sort('height')
  .limit(limit)
  .exec(callback)
}

Scanner.prototype.get_next_block_to_fix = function (limit, callback) {
  // var self = this
  var conditions = {
    txinserted: true, txsparsed: false
  }
  var projection = {
    height: 1,
    hash: 1
    // time: 1,
    // size: 1,
    // totalsent: 1,
    // fees: 1,
    // confirmations: 1,
    // txlength: 1
  }
  Blocks.find(conditions, projection)
  .sort('height')
  .limit(limit)
  .exec(callback)
}

Scanner.prototype.get_next_new_block = function (callback) {
  var self = this
  if (properties.last_block && properties.last_hash) {
    return callback(null, properties.last_block + 1, properties.last_hash)
  }
  Blocks.findOne({txinserted: true})
    .sort('-height')
    .exec(function (err, block_data) {
      if (err) return callback(err)
      if (block_data) {
        self.set_last_block(block_data.height)
        self.set_last_hash(block_data.hash)
        callback(null, block_data.height + 1, block_data.hash)
      } else {
        callback(null, 0)
      }
    })
}

Scanner.prototype.get_raw_block = function (block_height, callback) {
  var self = this
  bitcoin_rpc.cmd('getblockhash', [block_height], function (err, hash) {
    if (err) {
      if ('code' in err && err.code === -8) {
        // logger.debug('CODE -8!!!')
        bitcoin_rpc.cmd('getblockcount', [], function (err, block_count) {
          if (err) return callback(err)
          if (block_count < block_height) {
            return self.get_raw_block(block_count, callback)
          } else {
            return callback()
          }
        })
      } else {
        callback(err)
      }
    } else if (hash) {
      bitcoin_rpc.cmd('getblock', [hash], callback)
    } else {
      bitcoin_rpc.cmd('getblockcount', [], function (err, block_count) {
        if (err) return callback(err)
        if (block_count < block_height) {
          return self.get_raw_block(block_count, callback)
        } else {
          return callback()
        }
      })
    }
  })
}

Scanner.prototype.parse_new_block = function (raw_block_data, callback) {
  var self = this
  raw_block_data.time = raw_block_data.time * 1000
  raw_block_data.txsparsed = false
  raw_block_data.txinserted = false
  raw_block_data.ccparsed = false
  raw_block_data.reward = calc_block_reward(raw_block_data.height)
  raw_block_data.totalsent = 0
  raw_block_data.fees = 0
  raw_block_data.txlength = raw_block_data.tx.length
  var conditions = {
    hash: raw_block_data.hash
  }
  console.log('parsing new block ' + raw_block_data.height)

  var command_arr = []
  raw_block_data.tx.forEach(function (txhash) {
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1]})
  })

  // console.log(raw_block_data.tx)

  var utxo_bulk = Utxo.collection.initializeUnorderedBulkOp()
  utxo_bulk.bulk_name = 'utxo_bulk'
  var addresses_transactions_bulk = AddressesTransactions.collection.initializeUnorderedBulkOp()
  addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
  var addresses_utxos_bulk = AddressesUtxos.collection.initializeUnorderedBulkOp()
  addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
  var raw_transaction_bulk = RawTransactions.collection.initializeUnorderedBulkOp()
  raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
  // var addresses_assets = {}

  bitcoin_rpc.cmd(command_arr, function (raw_transaction_data, cb) {
    raw_transaction_data = to_discrete(raw_transaction_data)
    var out = self.parse_new_transaction(raw_transaction_data, raw_block_data.height, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk)
    if (out) {
      raw_block_data.totalsent += out
      if (is_coinbase(raw_transaction_data)) {
        raw_block_data.fees = out || raw_block_data.reward
        raw_block_data.fees -= raw_block_data.reward
      }
    }
    cb()
  },
  function (err) {
    if (err) {
      if ('code' in err && err.code === -5) {
        console.error('Can\'t find tx.')
      } else {
        console.error('parse_new_block_err: ' + err)
        // console.error(command_arr)
        return callback(err)
      }
    }
    if (debug) console.time('vout_parse_bulks')
    execute_bulks_parallel([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, raw_transaction_bulk], function (err) {
      if (debug) console.timeEnd('vout_parse_bulks')
      if (err) return callback(err)
      raw_block_data.txinserted = true
      // self.emit('newblock', raw_block_data) //TODO: add total sent && emit when finised
      var blocks_bulk = Blocks.collection.initializeOrderedBulkOp()
      if (!properties.next_hash) {
        blocks_bulk.find({hash: raw_block_data.previousblockhash}).updateOne({$set: {nextblockhash: raw_block_data.hash}})
      }
      blocks_bulk.find(conditions).upsert().updateOne(raw_block_data)

      blocks_bulk.execute(function (err) {
        if (err) return callback(err)
        self.set_last_hash(raw_block_data.hash)
        self.set_last_block(raw_block_data.height)
        self.set_next_hash(raw_block_data.nextblockhash)
        callback()
      })
    })
  })
}

var is_coinbase = function (tx_data) {
  return (tx_data && tx_data.vin && tx_data.vin.length === 1 && tx_data.vin[0].coinbase)
}

Scanner.prototype.get_raw_transaction = function (tx_hash, callback) {
  // var self = this
  bitcoin_rpc.cmd('getrawtransaction', [tx_hash, 1], function (err, raw_transaction_data) {
    if (err) return callback(err)
    callback(null, to_discrete(raw_transaction_data))
  })
}

var to_discrete = function (raw_transaction_data) {
  if (!raw_transaction_data.vout) return raw_transaction_data

  raw_transaction_data.vout.forEach(function (vout) {
    if (vout.value) {
      vout.value *= 100000000
    }
    if (vout.fee) {
      vout.fee *= 100000000
    }
  })
  return raw_transaction_data
}

Scanner.prototype.parse_vin = function (raw_transaction_data, block_height, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_utxos_bulk, callback) {
  // var self = this
  var coinbase = false
  var utxos

  if (!raw_transaction_data.vin) {
    return callback()
  }

  var vins = {}
  async.waterfall([
    function (cb) {
      if (!raw_transaction_data.colored) return cb(null, [])

      var conditions = []
      raw_transaction_data.vin.forEach(function (vin) {
        if (vin.coinbase) {
          coinbase = true
          vin.fixed = true
        } else {
          // var colored = true
          if (vin.txid && 'vout' in vin) {
            conditions.push({
              txid: vin.txid,
              $or: [
                {
                  colored: false
                },
                {
                  iosparsed: true,
                  colored: true,
                  ccparsed: true
                }
              ]
            })
            vins[vin.txid + ':' + vin.vout] = vin
          }
        }
      })
      if (!coinbase) {
        RawTransactions.find({'$or': conditions}).exec(cb)
      } else {
        cb(null, [])
      }
    },
    function (in_transactions, cb) {
      if (coinbase) {
        return cb(null, [])
      }
      var conditions = []
      if (raw_transaction_data.colored) {
        in_transactions.forEach(function (in_transaction) {
          in_transaction.vout.forEach(function (vout) {
            if (in_transaction.txid + ':' + vout.n in vins) {
              conditions.push({
                txid: in_transaction.txid,
                index: vout.n
              })
            }
          })
        })
      } else {
        raw_transaction_data.vin.forEach(function (vin) {
          // colored = true
          if (vin.txid && 'vout' in vin) {
            conditions.push({
              txid: vin.txid,
              index: vin.vout
            })
            // logger.debug('inserting: '+vin.txid+':'+vin.vout)
            vins[vin.txid + ':' + vin.vout] = vin
          }
        })
      }
      if (conditions.length) {
        Utxo.find({'$or': conditions}).exec(cb)
      } else {
        return cb(null, [])
      }
    },
    function (l_utxos, cb) {
      utxos = l_utxos
      if (coinbase) return cb()
      cb()
    }
  ],
  function (err) {
    if (err) return callback(err)
    add_insert_update_to_bulk(raw_transaction_data, vins, utxos)
    add_remove_to_bulk(utxos, utxo_bulk, block_height, raw_transaction_data.txid)
    var all_fixed = (Object.keys(vins).length === 0)
    callback(null, coinbase, all_fixed)
  })
}

var add_insert_update_to_bulk = function (raw_transaction_data, vins, utxos) {
  utxos.forEach(function (utxo) {
    // logger.debug('searching: '+utxo.txid+':'+utxo.index)
    var vin = vins[utxo.txid + ':' + utxo.index]
    // var conditions = {
    //   txid: utxo.txid,
    //   index: utxo.index
    // }

    utxo = utxo.toObject()
    vin.previousOutput = utxo.scriptPubKey
    vin.assets = utxo.assets || []
    vin.value = utxo.value || null
    vin.fixed = true
    delete vins[utxo.txid + ':' + utxo.index]
  })

  // var temp = 0
  // for (var vinkey in vins) {
  //   vin = vins[vinkey]
  //   // console.error('!!!! NOT FOUND '+vin.txid+':'+vin.vout+' !!!!')
  // }
  if (Object.keys(vins).length) {
    raw_transaction_data.tries = raw_transaction_data.tries || 0
    raw_transaction_data.tries++
    // logger.debug('another try: '+raw_transaction_data.tries++)
    // logger.debug('Object.keys(vins).length', Object.keys(vins).length)
  }
}

var add_remove_to_bulk = function (utxos, utxos_bulk, block_height, txid) {
  var is_utxos_bulk = false
  utxos.forEach(function (utxo) {
    utxos_bulk.find({ _id: utxo._id}).updateOne({
      $set: {
        used: true,
        usedBlockheight: block_height,
        usedTxid: txid
      }
    })
    is_utxos_bulk = true
  })
  return is_utxos_bulk
}

var get_assets_outputs = function (raw_transaction) {
  var transaction_data = JSON.parse(JSON.stringify(raw_transaction))
  var ccdata = transaction_data.ccdata[0]
  var assets = []
  // var issuedassetid = null
  /*var encodeItem = { assetid: 0, iputindex: 0, amountleft: 0 }*/
  // is it an issuece
  if (ccdata.type === 'issuance') {
    // logger.debug('issuance!')
    var opts = {
      'cc_data': [{
        type: 'issuance',
        lockStatus: ccdata.lockStatus,
        divisibility: ccdata.divisibility
      }],
      'vin': [{'txid': transaction_data.vin[0].txid, 'vout': transaction_data.vin[0].vout }]
    }
    if (!opts.cc_data[0].lockStatus) {
      opts.vin[0].address = transaction_data.vin[0].previousOutput.addresses[0]
    }
    transaction_data.vin[0].assets = transaction_data.vin[0].assets || []
    transaction_data.vin[0].assets.unshift({
      assetId: assetIdencoder(opts),
      amount: ccdata.amount,
      issueTxid: transaction_data.txid,
      divisibility: ccdata.divisibility,
      lockStatus: ccdata.lockStatus && ccdata.noRules
    })
    // console.log(transaction_data.vin[0].assets)
  }

  var paymentIndex = 0
  transaction_data.vin.forEach(function (prevOutput, currentIndex) {
    var overflow = 0
    prevOutput.assets = prevOutput.assets || []
    prevOutput.assets.forEach(function (asset, assetIndex) {
      var currentAmount = 0
      var currentPayment = {}

      // console.log(paymentIndex + " < " + ccdata.payments.length + " "  )
      for (var i = paymentIndex;
        i < ccdata.payments.length
        && (currentAmount <= asset.amount)
        && ccdata.payments[i].input === currentIndex; paymentIndex++, i++) {
        currentPayment = ccdata.payments[i]
        var actualAmount = overflow ? overflow : currentPayment.amountOfUnits
        overflow = 0
        // console.log("checking for asset with amount at: " + currentPayment.output + "  " + currentPayment.amountOfUnits)
        if (isPaymentSimple(currentPayment)) {
          // console.log("paymet is simple")
          if (isInOutputsScope(currentPayment.output, transaction_data.vout)) {
            // console.log("output in scope")
            if (!assets[currentPayment.output]) assets[currentPayment.output] = []
            // console.log("found and asset with amount at: " + currentPayment.output + "  " +actualAmount)

            // console.log(ccdata.payments)
            assets[currentPayment.output].push({
              assetId: asset.assetId,
              amount: (actualAmount + currentAmount > asset.amount) ? (asset.amount - currentAmount) : actualAmount,
              issueTxid: asset.issueTxid,
              divisibility: asset.divisibility,
              lockStatus: asset.lockStatus
            })
            currentAmount += actualAmount
          }
        } else if (isPaymentRange(currentPayment)) {
          // currentPayment.output
        }
      }
      // check leftovers and throw them to the last aoutput
      if (currentAmount < asset.amount) {
        // console.log("found change ")
        if (isPaymentSimple(currentPayment)) {
           // if we already encoded the last output then just set the correct amount
          var prev = assets[transaction_data.vout.length - 1] ? assets[transaction_data.vout.length - 1].length - 1 : false
          if (prev && assets[transaction_data.vout.length - 1][prev].assetId === asset.assetId) {
            assets[transaction_data.vout.length - 1][prev].amount = asset.amount
          } else if (!assets[transaction_data.vout.length - 1]) { // put chnage in last output
            assets[transaction_data.vout.length - 1] = []
          }
          assets[transaction_data.vout.length - 1].push({
            assetId: asset.assetId,
            amount: asset.amount - currentAmount,
            issueTxid: asset.issueTxid,
            divisibility: asset.divisibility,
            lockStatus: asset.lockStatus
          })
        }
      }
   // set overflow so we take the next asset if we need to
      if (currentAmount > asset.amount) {
        overflow = currentAmount - asset.amount
        paymentIndex--
      }
      else overflow = 0
    })
    if (overflow) {
      raw_transaction.overflow = true
    }
  }) // prev_outputs.forEach
  assets = assets.map(function (output_assets) {
    return output_assets.filter(function (asset) { return asset.amount > 0 })
  })
  return assets
}

function isPaymentSimple (payment) {
  return (!payment.range && !payment.precent)
}

function isPaymentRange (payment) {
  return payment.range
}

function isPaymentByPrecentage (payment) {
  return payment.range
}

function isInOutputsScope (i, vout) {
  return i <= vout.length - 1
}

Scanner.prototype.parse_vout = function (raw_transaction_data, block_height, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk) {
  // var self = this
  var out = 0
  // var first = null
  if (!raw_transaction_data.vout) return 0
  // var assets
  raw_transaction_data.colored = false
  raw_transaction_data.ccparsed = false
  var addresses_transactions_in_bulks = []
  var addresses_utxos_in_bulks = []
  raw_transaction_data.vout.forEach(function (vout) {
    if (vout.scriptPubKey.hex.length > 2000) {
      vout.scriptPubKey.hex = null
      vout.scriptPubKey.asm = 'TOBIG'
    }
    if (vout.scriptPubKey && vout.scriptPubKey.type === 'nulldata') {
      // logger.debug('found OP_RETURN')

      var hex = get_opreturn_data(vout.scriptPubKey.hex) // remove op_return (0x6a) and data length?
      // logger.debug('hex', hex)
      if (check_version(hex)) {
        try {
          var cc = CCTransaction.fromHex(hex).toJson()
        } catch (e) {
          console.log('Invalid CC transaction.')
        }
        if (cc) {
          // logger.debug('colored!')
          raw_transaction_data.ccdata = raw_transaction_data.ccdata || []
          raw_transaction_data.ccdata.push(cc)
          raw_transaction_data.colored = true
          raw_transaction_data.ccparsed = false
        }
      }
    }
    out += vout.value
    var utxo = {
      txid: raw_transaction_data.txid,
      index: vout.n,
      value: vout.value,
      used: false,
      blockheight: block_height,
      blocktime: raw_transaction_data.blocktime
    }
    if ('scriptPubKey' in vout && 'addresses' in vout.scriptPubKey) {
      utxo.scriptPubKey = vout.scriptPubKey
      var tx_pushed = false
      utxo.scriptPubKey.addresses.forEach(function (address) {
        if (!tx_pushed) {
          var address_tx = {
            address: address,
            txid: raw_transaction_data.txid
          }
          if (addresses_transactions_in_bulks.indexOf(address_tx) === -1) {
            addresses_transactions_bulk.find(address_tx).upsert().updateOne(address_tx)
            addresses_transactions_in_bulks.push(address_tx)
          }
          tx_pushed = true
        }
        var address_utxo = {
          address: address,
          utxo: utxo.txid + ':' + utxo.index
        }
        if (addresses_utxos_in_bulks.indexOf(address_utxo) === -1) {
          addresses_utxos_bulk.find(address_utxo).upsert().updateOne(address_utxo)
          addresses_utxos_in_bulks.push(address_utxo)
        }
      })
    }
    var conditions = {
      txid: utxo.txid,
      index: utxo.index
    }
    utxo_bulk.find(conditions).upsert().updateOne(utxo)
  })
  return out
}

var get_opreturn_data = function (hex) {
  return hex.substring(4)
}

var check_version = function (hex) {
  var version = hex.toString('hex').substring(0, 4)
  return (version.toLowerCase() === '4343')
}

Scanner.prototype.parse_new_transaction = function (raw_transaction_data, block_height, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk) {
  var self = this
  if (raw_transaction_data.time) {
    raw_transaction_data.time = raw_transaction_data.time * 1000
  }
  if (raw_transaction_data.blocktime) {
    raw_transaction_data.blocktime = raw_transaction_data.blocktime * 1000
    raw_transaction_data.blockheight = block_height
  } else {
    raw_transaction_data.blockheight = -1
    raw_transaction_data.blocktime = Date.now()
  }
  raw_transaction_data.tries = 0

  var conditions = {
    txid: raw_transaction_data.txid
  }

  raw_transaction_data.iosparsed = false
  var out = self.parse_vout(raw_transaction_data, block_height, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk)
  raw_transaction_bulk.find(conditions).upsert().updateOne(raw_transaction_data)
  return out
}

var calc_block_reward = function (block_height) {
  var reward = 50 * 100000000
  var divistions = Math.floor(block_height / 210000)
  while (divistions) {
    reward /= 2
    divistions--
  }
  return reward
}

Scanner.prototype.parse_new_mempool_transaction = function (raw_transaction_data, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk, callback) {
  var self = this
  var transaction_data
  var did_work = false
  var parsing_vin = false
  var emit
  var conditions = {
    txid: raw_transaction_data.txid
  }
  async.waterfall([
    function (cb) {
      if (raw_transaction_data.from_db) return cb(null, raw_transaction_data)
      RawTransactions.findOne(conditions, cb)
    },
    function (l_transaction_data, cb) {
      transaction_data = l_transaction_data
      if (transaction_data) {
        raw_transaction_data = transaction_data.toObject()
        cb(null, 0)
      } else {
        // logger.debug('parsing new tx: '+raw_transaction_data.txid)
        did_work = true
        var out = self.parse_new_transaction(raw_transaction_data, -1, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk)
        cb(null, out)
      }
    },
    function (out, cb) {
      if (raw_transaction_data.iosparsed) {
        cb(null, null, true)
      } else {
        // logger.debug('parsing tx vin: '+raw_transaction_data.txid)
        parsing_vin = true
        self.parse_vin(raw_transaction_data, -1, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_utxos_bulk, cb)
      }
    },
    function (coinbase, all_fixed, cb) {
      if (raw_transaction_data.ccparsed) {
        cb(null, null)
      } else {
        raw_transaction_data.iosparsed = parsing_vin && all_fixed
        did_work = did_work && parsing_vin && all_fixed
        if (all_fixed && raw_transaction_data.colored && !raw_transaction_data.ccparsed) {
          self.parse_cc_tx(raw_transaction_data, utxo_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk)
          raw_transaction_data.ccparsed = true
          did_work = true
        }
        if (did_work && (all_fixed || raw_transaction_data.ccparsed)) {
          // logger.debug('emiting tx:',raw_transaction_data.txid)
          emit = ['newtransaction', raw_transaction_data]
          // self.emit('newtransaction', raw_transaction_data)
          if (raw_transaction_data.colored) {
            // logger.debug('emiting cc tx:',raw_transaction_data.txid)
            emit = ['newcctransaction', raw_transaction_data]
            // self.emit('newcctransaction', raw_transaction_data)
          }
        }
        if (did_work) {
          raw_transaction_bulk.find(conditions).upsert().updateOne(raw_transaction_data)
        }
        cb()
      }
    }
  ],
  function (err) {
    if (err) return callback(err)
    if (emit) self.emit(emit[0], emit[1])
    callback()
  })
}

Scanner.prototype.parse_new_mempool = function (callback) {
  var self = this

  self.emit('mempool')
  bitcoin_rpc.cmd('getrawmempool', [], function (err, whole_txids) {
    console.log('parsing mempool txs (' + whole_txids.length + ')')
    if (err) return callback(err)
    if (!whole_txids || !whole_txids.length) return callback()

    var i = 0
    async.whilst(function () { return i < whole_txids.length },
      function (cb) {
        var utxo_bulk = Utxo.collection.initializeUnorderedBulkOp()
        utxo_bulk.bulk_name = 'utxo_bulk'
        var raw_transaction_bulk = RawTransactions.collection.initializeUnorderedBulkOp()
        raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
        var addresses_transactions_bulk = AddressesTransactions.collection.initializeUnorderedBulkOp()
        addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
        var addresses_utxos_bulk = AddressesUtxos.collection.initializeUnorderedBulkOp()
        addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
        var assets_utxos_bulk = AssetsUtxos.collection.initializeUnorderedBulkOp()
        assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'
        var assets_transactions_bulk = AssetsTransactions.collection.initializeUnorderedBulkOp()
        assets_transactions_bulk.bulk_name = 'assets_transactions_bulk'
        var assets_addresses_bulk = AssetsAddresses.collection.initializeUnorderedBulkOp()
        assets_addresses_bulk.bulk_name = 'assets_addresses_bulk'

        var n_batch = 5000
        var txids = whole_txids.slice(i, i + n_batch)
        console.log('parsing mempool txs (' + i + '-' + (i + txids.length) + ',' + whole_txids.length + ')')
        i += n_batch
        // var addresses_assets = {}
        var parsed_tx = []
        // console.log('before find db tx')
        var conditions = {
          txid: {$in: txids},
          iosparsed: true,
          $where: 'this.colored == this.ccparsed'
        }
        var projection = {
          txid: 1
        }
        RawTransactions.find(conditions, projection, function (err, transactions) {
          // console.log('after find db tx')
          if (err) return cb(err)
          transactions.forEach(function (transaction) {
            parsed_tx.push(transaction.txid)
          })
          // var txids_intersection = _.intersection(txids, db_txids)
          var txids_intersection = _.intersection(txids, parsed_tx)
          txids = _.xor(txids_intersection, txids)
          var command_arr = []
          txids.forEach(function (txhash) {
            command_arr.push({ method: 'getrawtransaction', params: [txhash, 1]})
          })
          // console.log('before find and parse rpc tx ('+txids.length+')')
          bitcoin_rpc.cmd(command_arr, function (raw_transaction_data, cb2) {
            raw_transaction_data = to_discrete(raw_transaction_data)
            self.parse_new_mempool_transaction(raw_transaction_data, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk, cb2)
          },
          function (err) {
            // console.log('after find and parse rpc tx')
            if (err) {
              if ('code' in err && err.code === -5) {
                console.error('Can\'t find tx.')
              } else {
                console.error('parse_new_block_err: ' + err)
                // console.error(command_arr)
                return cb(err)
              }
            }
            // console.log('before parse db tx')
            // async.each(transactions, function (transaction, cb) {
            //   transaction.from_db = true
            //   self.parse_new_mempool_transaction(transaction, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk, cb)
            // },
            // function (err) {
            //   if (err) return cb(err)
            //   console.log('after parse db tx')
            execute_bulks_parallel([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_utxos_bulk, assets_transactions_bulk, assets_addresses_bulk, raw_transaction_bulk], cb)
            // })
          })
        })
      },
      callback
    )
  })
}

Scanner.prototype.priority_parse = function (txid, callback) {
  var self = this
  // var ins = []
  var PARSED = 'PARSED'
  var transaction
  var utxo_bulk = Utxo.collection.initializeUnorderedBulkOp()
  utxo_bulk.bulk_name = 'utxo_bulk'
  var raw_transaction_bulk = RawTransactions.collection.initializeUnorderedBulkOp()
  raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
  var addresses_transactions_bulk = AddressesTransactions.collection.initializeUnorderedBulkOp()
  addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
  var addresses_utxos_bulk = AddressesUtxos.collection.initializeUnorderedBulkOp()
  addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
  var assets_utxos_bulk = AssetsUtxos.collection.initializeUnorderedBulkOp()
  assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'
  var assets_transactions_bulk = AssetsTransactions.collection.initializeUnorderedBulkOp()
  assets_transactions_bulk.bulk_name = 'assets_transactions_bulk'
  var assets_addresses_bulk = AssetsAddresses.collection.initializeUnorderedBulkOp()
  assets_addresses_bulk.bulk_name = 'assets_addresses_bulk'
  async.waterfall([
    function (cb) {
      var conditions = {
        txid: txid,
        iosparsed: true,
        $where: 'this.colored == this.ccparsed'
      }
      var projection = {
        txid: 1
      }
      RawTransactions.findOne(conditions, projection).exec(cb)
    },
    function (tx, cb) {
      if (tx) return cb(PARSED)
      bitcoin_rpc.cmd('getrawtransaction', [txid, 1], function (err, raw_transaction_data) {
        if (err && err.code === -5) return cb(['tx ' + txid + ' not found.', 204])
        cb(err, raw_transaction_data)
      })
    },
    function (raw_transaction_data, cb) {
      transaction = raw_transaction_data
      transaction = to_discrete(transaction)
      if (!transaction || !transaction.vin) return callback('txid ' + txid + ' not found')
      async.each(transaction.vin, function (vin, cb2) {
        self.priority_parse(vin.txid, cb2)
      },
      cb)
    },
    function (cb) {
      self.parse_new_mempool_transaction(transaction, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk, cb)
    }
  ],
  function (err) {
    if (err) {
      if (err === PARSED) return callback()
      return callback(err)
    }
    execute_bulks_parallel([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_utxos_bulk, assets_transactions_bulk, assets_addresses_bulk, raw_transaction_bulk], callback)
  })
}

Scanner.prototype.get_info = function (callback) {
  bitcoin_rpc.cmd('getinfo', [], callback)
}

var execute_bulks = function (bulks, callback) {
  var bulk_index = 0
  async.eachSeries(bulks, function (bulk, cb) {
    // if (process.env.ROLE === 'scanner') logger.debug('Executing bulk with size '+bulk.length)
    var channel = 'bulk_' + bulk_index + '_length_' + bulk.length
    if (debug) console.time(channel)
    if (bulk.length) {
      bulk.execute(function (err) {
        if (debug) console.timeEnd(channel)
        bulk_index++
        cb(err)
      })
    } else {
      // logger.debug('empty bulk')
      if (debug) console.timeEnd(channel)
      bulk_index++
      cb()
    }
  },
  callback)
}

var execute_bulks_parallel = function (bulks, callback) {
  async.each(bulks, function (bulk, cb) {
    // if (process.env.ROLE === 'scanner') logger.debug('Executing bulk with size '+bulk.length)
    var channel = 'bulk_' + bulk.bulk_name + '_length_' + bulk.length
    if (debug) console.time(channel)
    if (bulk.length) {
      bulk.execute(function (err) {
        if (debug) console.timeEnd(channel)
        cb(err)
      })
    } else {
      // logger.debug('empty bulk')
      if (debug) console.timeEnd(channel)
      cb()
    }
  },
  callback)
}

Scanner.prototype.transmit = function (txHex, callback) {
  var self = this
  if (typeof txHex !== 'string') {
    txHex = txHex.toHex()
  }
  bitcoin_rpc.cmd('sendrawtransaction', [txHex], function (err, txid) {
    if (err) return callback(err)
    self.priority_parse(txid, function (err) {
      if (err) return callback(err)
      return callback(null, {txid: txid})
    })
  })
}

module.exports = Scanner
