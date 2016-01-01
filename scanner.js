var async = require('async')
var util = require('util')
var events = require('events')
var _ = require('lodash')
var CCTransaction = require('cc-transaction')
var bitcoin = require('bitcoin-async')
var get_assets_outputs = require('cc-get-assets-outputs')

var properties
var bitcoin_rpc
var debug

function Scanner (settings, db) {
  // console.log('Mongoose', db)
  var self = this

  debug = settings.debug
  self.to_revert = []
  self.priority_parse_list = []
  properties = settings.properties
  self.next_hash = settings.next_hash
  self.last_hash = settings.last_hash
  self.last_block = settings.last_block
  self.last_fully_parsed_block = settings.last_fully_parsed_block
  bitcoin_rpc = new bitcoin.Client(settings.rpc_settings)

  self.Blocks = db.model('blocks', require(__dirname + '/models/blocks')(db, properties))
  self.RawTransactions = db.model('rawtransactions', require(__dirname + '/models/rawtransactions')(db, properties))
  self.Utxo = db.model('utxo', require(__dirname + '/models/utxo')(db))
  self.AddressesTransactions = db.model('addressestransactions', require(__dirname + '/models/addressestransactions')(db))
  self.AddressesUtxos = db.model('addressesutxos', require(__dirname + '/models/addressesutxos')(db))
  self.AssetsTransactions = db.model('assetstransactions', require(__dirname + '/models/assetstransactions')(db))
  self.AssetsUtxos = db.model('assetsutxos', require(__dirname + '/models/assetsutxos')(db))
  self.AssetsAddresses = db.model('assetsaddresses', require(__dirname + '/models/assetsaddresses')(db))

  if (process.env.ROLE === properties.roles.SCANNER) {
    self.on('newblock', function (newblock) {
      process.send({to: properties.roles.API, newblock: newblock})
    })
    self.on('newtransaction', function (newtransaction) {
      process.send({to: properties.roles.API, newtransaction: newtransaction})
    })
    self.on('newcctransaction', function (newcctransaction) {
      process.send({to: properties.roles.API, newcctransaction: newcctransaction})
    })
    self.on('revertedblock', function (revertedblock) {
      process.send({to: properties.roles.API, revertedblock: revertedblock})
    })
    self.on('revertedtransaction', function (revertedtransaction) {
      process.send({to: properties.roles.API, revertedtransaction: revertedtransaction})
    })
    self.on('revertedcctransaction', function (revertedcctransaction) {
      process.send({to: properties.roles.API, revertedcctransaction: revertedcctransaction})
    })
    self.on('mempool', function () {
      process.send({to: properties.roles.API, mempool: true})
    })
  }

  self.mempool_cargo = async.cargo(function (tasks, callback) {
    self.parse_mempool_cargo(tasks, callback)
  }, 500)
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
        raw_block_data = null
      }
      if (!raw_block_data || (raw_block_data.height === next_block - 1 && raw_block_data.hash === last_hash)) {
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
  ], function (err) {
    if (debug && job) console.timeEnd(job)
    if (err) self.to_revert = []
    self.scan_blocks(err)
  })
}

Scanner.prototype.revert_block = function (block_height, callback) {
  var self = this
  console.log('Reverting block: ' + block_height)
  var conditions = {
    height: block_height
  }
  self.Blocks.findOne(conditions).exec(function (err, block_data) {
    if (err) return callback(err)
    if (!block_data || !block_data.tx) return callback()
    var block_id = {
      height: block_data.height,
      hash: block_data.hash
    }
    var utxo_bulk = self.Utxo.collection.initializeOrderedBulkOp()
    // var holders_bulk = Holders.collection.initializeOrderedBulkOp()
    var addresses_transactions_bulk = self.AddressesTransactions.collection.initializeOrderedBulkOp()
    var addresses_utxos_bulk = self.AddressesUtxos.collection.initializeOrderedBulkOp()
    var assets_transactions_bulk = self.AssetsTransactions.collection.initializeOrderedBulkOp()
    var assets_utxos_bulk = self.AssetsUtxos.collection.initializeOrderedBulkOp()
    var raw_transaction_bulk = self.RawTransactions.collection.initializeOrderedBulkOp()

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
        self.fix_mempool(function (err) {
          // logger.debug('deleting block')
          self.Blocks.remove(conditions).exec(function (err) {
            if (err) return callback(err)
            // logger.debug('setting hashes')
            self.emit('revertedblock', block_id)
            self.set_last_hash(block_data.previousblockhash)
            self.set_last_block(block_data.height - 1)
            self.set_next_hash(null)
            // logger.debug('done reverting')
            callback()
          })
        })
      })
    })
  })
}

Scanner.prototype.fix_mempool = function (callback) {
  this.RawTransactions.update({blockheight: -1}, {iosparsed: false, ccparsed: false}, callback)
}

Scanner.prototype.revert_tx = function (txid, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, raw_transaction_bulk, callback) {
  var self = this
  var conditions = {
    txid: txid
  }
  console.log('reverting tx ' + txid)
  self.RawTransactions.findOne(conditions).exec(function (err, tx) {
    if (err) return callback(err)
    if (!tx) return callback()
    async.waterfall([
      function (cb) {
        // logger.debug('reverting vin')
        self.revert_vin(tx, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, cb)
      },
      function (cb) {
        // logger.debug('reverting vout')
        self.revert_vout(tx.txid, tx.vout, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk)
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

Scanner.prototype.revert_vin = function (tx, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, callback) {
  var txid = tx.txid
  var vins = tx.vin
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
  this.Utxo.find(conditions).exec(function (err, useds) {
    if (err) return callback(err)
    if (!useds || !useds.length) return callback()
    useds.forEach(function (used) {
      if (used.usedTxid === txid) {
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
      }
    })
    callback()
  })
}

Scanner.prototype.revert_vout = function (txid, vouts, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk) {
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
      if (!empty) return callback(err)
      var bulk = self.Blocks.collection.initializeUnorderedBulkOp()
      raw_block_datas.forEach(function (raw_block_data) {
        bulk.find({hash: raw_block_data.hash}).updateOne({
          $set: {
            txsparsed: true,
            ccparsed: false
          }
        })
      })
      if (!bulk.length) return callback(err)
      bulk.execute(callback)
    }

    var utxo_bulk = self.Utxo.collection.initializeUnorderedBulkOp()
    utxo_bulk.bulk_name = 'utxo_bulk'
    var raw_transaction_bulk = self.RawTransactions.collection.initializeUnorderedBulkOp()
    raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'

    self.get_need_to_fix_transactions_by_blocks(first_block, last_block, function (err, transactions_data) {
      if (err) return self.fix_blocks(err)
      console.log('Fixing blocks ' + first_block + '-' + last_block + ' (' + transactions_data.length + ' txs).')
        // var tx_to_fix_count = transactions_data.length
      // logger.debug('fixing '+tx_to_fix_count+' txs.')
      if (!transactions_data) return callback('can\'t get transactions from db')
      if (!transactions_data.length) {
        // logger.debug('no need to fix')
        if (debug) console.time('vin_insert_bulks')
        return close_blocks(null, true)
      }
      async.each(transactions_data, function (transaction_data, cb) {
        // logger.debug('fixing txid:', transaction_data.txid)
        self.parse_vin(transaction_data, transaction_data.blockheight, utxo_bulk, function (err, coinbase, all_fixed) {
          if (err) return cb(err)
          raw_transaction_bulk.find({txid: transaction_data.txid}).updateOne({
            $set: {
              iosparsed: all_fixed,
              vin: transaction_data.toObject().vin,
              tries: transaction_data.tries || 0,
              fee: transaction_data.fee,
              totalsent: transaction_data.totalsent

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
        execute_bulks_parallel([utxo_bulk, raw_transaction_bulk], close_blocks)
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
    var did_work = false
    var close_blocks = function (err, empty) {
      // logger.debug('closing')
      if (debug) console.timeEnd('parse_cc_bulks')
      if (err) return self.parse_cc(err)
      emits.forEach(function (emit) {
        self.emit(emit[0], emit[1])
      })
      if (!empty) {
        if (did_work) {
          return callback(err)
        } else {
          return setTimeout(function () {
            callback()
          }, 500)
        }
      }
      var bulk = self.Blocks.collection.initializeUnorderedBulkOp()
      raw_block_datas.forEach(function (raw_block_data) {
        if (raw_block_data.txsparsed) {
          self.emit('newblock', raw_block_data)
          self.set_last_fully_parsed_block(raw_block_data.height)
          bulk.find({hash: raw_block_data.hash}).updateOne({
            $set: {
              ccparsed: true
            }
          })
        }
      })
      if (!bulk.length) {
        return setTimeout(function () {
          callback()
        }, 500)
      }
      bulk.execute(callback)
    }

    var utxo_bulk = self.Utxo.collection.initializeUnorderedBulkOp()
    utxo_bulk.bulk_name = 'utxo_bulk'
    var raw_transaction_bulk = self.RawTransactions.collection.initializeUnorderedBulkOp()
    raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
    var assets_transactions_bulk = self.AssetsTransactions.collection.initializeUnorderedBulkOp()
    assets_transactions_bulk.bulk_name = 'assets_transactions_bulk'
    var assets_utxos_bulk = self.AssetsUtxos.collection.initializeUnorderedBulkOp()
    assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'
    var assets_addresses_bulk = self.AssetsAddresses.collection.initializeUnorderedBulkOp()
    assets_addresses_bulk.bulk_name = 'assets_addresses_bulk'

    self.get_need_to_cc_parse_transactions_by_blocks(first_block, last_block, function (err, transactions_data) {
      if (err) return self.parse_cc(err)
      console.log('Parsing cc for blocks ' + first_block + '-' + last_block + ' (' + transactions_data.length + ' txs).')
        // var tx_to_fix_count = transactions_data.length
      // logger.debug('fixing '+tx_to_fix_count+' txs.')
      if (!transactions_data) return callback('can\'t get transactions from db')
      if (!transactions_data.length) {
        // logger.debug('no need to fix')
        if (debug) console.time('parse_cc_bulks')
        return close_blocks(null, true)
      }
      transactions_data.forEach(function (transaction_data) {
        transaction_data = transaction_data.toObject()
        // var raw_block_data = raw_block_datas[transaction_data.blockheight - first_block]
        self.parse_cc_tx(transaction_data, utxo_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk)

        if (transaction_data.iosparsed) {
          did_work = true
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
          emits.push(['newtransaction', transaction_data])
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

  this.RawTransactions.find(conditions).sort('blockheight').limit(1000).exec(callback)
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
  this.RawTransactions.find(conditions).sort(sort).limit(200).exec(callback)
}

Scanner.prototype.set_next_hash = function (next_hash) {
  // var self = this
  properties.next_hash = next_hash
}

Scanner.prototype.set_last_hash = function (last_hash) {
  // var self = this
  properties.last_hash = last_hash
}

Scanner.prototype.set_last_block = function (last_block) {
  // var self = this
  properties.last_block = last_block
  process.send({to: properties.roles.API, last_block: last_block})
}

Scanner.prototype.set_last_fully_parsed_block = function (last_fully_parsed_block) {
  // var self = this
  properties.last_fully_parsed_block = last_fully_parsed_block
  process.send({to: properties.roles.API, last_fully_parsed_block: last_fully_parsed_block})
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
  this.Blocks.find(conditions, projection)
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
  this.Blocks.find(conditions, projection)
  .sort('height')
  .limit(limit)
  .exec(callback)
}

Scanner.prototype.get_next_new_block = function (callback) {
  var self = this
  if (properties.last_block && properties.last_hash) {
    return callback(null, properties.last_block + 1, properties.last_hash)
  }
  console.log('scanner.Blocks', !!self.Blocks)
  self.Blocks.findOne({txinserted: true})
    .sort('-height')
    .exec(function (err, block_data) {
      if (err) return callback(err)
      if (block_data) {
        self.set_last_block(block_data.height)
        self.set_last_hash(block_data.hash)
        callback(null, block_data.height + 1, block_data.hash)
      } else {
        callback(null, 0, null)
      }
    })
}

Scanner.prototype.get_raw_block = function (block_height, callback) {
  var self = this
  console.log('get_raw_block')
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
    if (~self.to_revert.indexOf(txhash)) {
      self.to_revert = []
    }
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1]})
  })

  // console.log(raw_block_data.tx)

  var utxo_bulk = self.Utxo.collection.initializeUnorderedBulkOp()
  utxo_bulk.bulk_name = 'utxo_bulk'
  var addresses_transactions_bulk = self.AddressesTransactions.collection.initializeUnorderedBulkOp()
  addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
  var addresses_utxos_bulk = self.AddressesUtxos.collection.initializeUnorderedBulkOp()
  addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
  var raw_transaction_bulk = self.RawTransactions.collection.initializeUnorderedBulkOp()
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
      // self.emit('newblock', raw_block_data) //TODO: add total sent
      var blocks_bulk = self.Blocks.collection.initializeOrderedBulkOp()
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
  if (!raw_transaction_data || !raw_transaction_data.vout) return raw_transaction_data

  raw_transaction_data.vout.forEach(function (vout) {
    if (vout.value) {
      vout.value *= 100000000
    }
  })
  return raw_transaction_data
}

var calc_fee = function (raw_transaction_data) {
    var fee = 0
    var totalsent = 0
    var coinbase = false
    if ('vin' in raw_transaction_data && raw_transaction_data.vin) {
      raw_transaction_data.vin.forEach(function (vin) {
        if ('coinbase' in vin && vin.coinbase) {
          coinbase = true
        }
        if (vin.value) {
          fee += vin.value
        }
      })
    }
    if (raw_transaction_data.vout) {
      raw_transaction_data.vout.forEach(function (vout) {
        if ('value' in vout && vout.value) {
          fee -= vout.value
          totalsent += vout.value
        }
      })
    }
    raw_transaction_data.totalsent = totalsent
    raw_transaction_data.fee = coinbase ? 0 : fee
  }

Scanner.prototype.parse_vin = function (raw_transaction_data, block_height, utxo_bulk, callback) {
  var self = this
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
        self.RawTransactions.find({'$or': conditions}).exec(cb)
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
        self.Utxo.find({'$or': conditions}).exec(cb)
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
    if (all_fixed) {
      calc_fee(raw_transaction_data)
    }
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
    if (raw_transaction_data.tries > 1000) {
      // var send_obj = {
      //   to: properties.roles.SCANNER,
      //   parse_priority: raw_transaction_data.txid
      // }
      // console.log('add_insert_update_to_bulk: send_obj', send_obj)
      // process.send(send_obj)
      console.warn('transaction', raw_transaction_data.txid, 'has un parsed inputs (', Object.keys(vins), ') for over then 1000 tries.')
    }
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

Scanner.prototype.parse_vout = function (raw_transaction_data, block_height, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk) {
  // var self = this
  var out = 0
  // var first = null
  if (!raw_transaction_data.vout) return 0
  // var assets
  raw_transaction_data.colored = false
  // raw_transaction_data.ccparsed = false
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
          // raw_transaction_data.ccparsed = false
        }
      }
    }
    out += vout.value
    var utxo = {
      blocktime: raw_transaction_data.blocktime,
      blockheight: block_height
    }
    var new_utxo = {
      txid: raw_transaction_data.txid,
      index: vout.n,
      value: vout.value,
      used: false
    }
    if ('scriptPubKey' in vout && 'addresses' in vout.scriptPubKey) {
      new_utxo.scriptPubKey = vout.scriptPubKey
      var tx_pushed = false
      new_utxo.scriptPubKey.addresses.forEach(function (address) {
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
          utxo: new_utxo.txid + ':' + new_utxo.index
        }
        if (addresses_utxos_in_bulks.indexOf(address_utxo) === -1) {
          addresses_utxos_bulk.find(address_utxo).upsert().updateOne(address_utxo)
          addresses_utxos_in_bulks.push(address_utxo)
        }
      })
    }
    var conditions = {
      txid: new_utxo.txid,
      index: new_utxo.index
    }
    utxo_bulk.find(conditions).upsert().updateOne({
      $set: utxo,
      $setOnInsert: new_utxo
    })
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
  } else {
    raw_transaction_data.blocktime = Date.now()
    if (block_height !== -1) console.log('yoorika!')
  }
  raw_transaction_data.blockheight = block_height
  // raw_transaction_data.tries = 0

  var conditions = {
    txid: raw_transaction_data.txid
  }

  var out = self.parse_vout(raw_transaction_data, block_height, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk)
  raw_transaction_data.iosparsed = false
  raw_transaction_data.ccparsed = false
  var update = {
    blocktime: raw_transaction_data.blocktime,
    blockheight: raw_transaction_data.blockheight,
    blockhash: raw_transaction_data.blockhash,
    time: raw_transaction_data.time
  }
  delete raw_transaction_data.blocktime
  delete raw_transaction_data.blockheight
  delete raw_transaction_data.blockhash
  delete raw_transaction_data.time
  raw_transaction_bulk.find(conditions).upsert().updateOne({
    $setOnInsert: raw_transaction_data,
    $set: update
  })

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

var get_block_height = function (blockhash, callback) {
  bitcoin_rpc.cmd('getblock', [blockhash], function (err, block) {
    if (err) return callback(err)
    callback(null, block.height)
  })
}

Scanner.prototype.parse_new_mempool_transaction = function (raw_transaction_data, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk, callback) {
  var self = this
  var transaction_data
  var did_work = false
  var blockheight = -1
  // var parsing_vin = false
  var emits = []
  var conditions = {
    txid: raw_transaction_data.txid
  }
  async.waterfall([
    function (cb) {
      // if (raw_transaction_data.from_db) return cb(null, raw_transaction_data)
      self.RawTransactions.findOne(conditions, cb)
    },
    function (l_transaction_data, cb) {
      transaction_data = l_transaction_data
      if (transaction_data) {
        raw_transaction_data = transaction_data.toObject()
        blockheight = raw_transaction_data.blockheight || -1
        cb(null, blockheight)
      } else {
        // logger.debug('parsing new tx: '+raw_transaction_data.txid)
        did_work = true
        if (blockheight === -1 && raw_transaction_data.blockhash) {
          get_block_height(raw_transaction_data.blockhash, cb)
        }
        else {
          cb(null, blockheight)
        }
      }
    },
    function (l_blockheight, cb) {
      blockheight = l_blockheight
      if (transaction_data) return cb()
      if (raw_transaction_data.time) {
        raw_transaction_data.time = raw_transaction_data.time * 1000
      }
      if (raw_transaction_data.blocktime) {
        raw_transaction_data.blocktime = raw_transaction_data.blocktime * 1000
      } else {
        raw_transaction_data.blocktime = Date.now()
      }
      raw_transaction_data.blockheight = blockheight
      // raw_transaction_data.tries = 0

      var out = self.parse_vout(raw_transaction_data, blockheight, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk)
      raw_transaction_data.iosparsed = false
      raw_transaction_data.ccparsed = false
      cb()
    },
    function (cb) {
      if (raw_transaction_data.iosparsed) {
        cb(null, null, true)
      } else {
        // logger.debug('parsing tx vin: '+raw_transaction_data.txid)
        did_work = true
        self.parse_vin(raw_transaction_data, blockheight, utxo_bulk, cb)
      }
    },
    function (coinbase, all_fixed, cb) {
      if (raw_transaction_data.ccparsed) {
        cb(null, null)
      } else {
        raw_transaction_data.iosparsed = all_fixed
        // did_work = did_work || (parsing_vin && all_fixed)
        if (all_fixed && raw_transaction_data.colored && !raw_transaction_data.ccparsed) {
          self.parse_cc_tx(raw_transaction_data, utxo_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk)
          raw_transaction_data.ccparsed = true
          did_work = true
        }
        if (did_work && all_fixed) {
          emits.push(['newtransaction', raw_transaction_data])
          // self.emit('newtransaction', raw_transaction_data)
          if (raw_transaction_data.colored) {
            emits.push(['newcctransaction', raw_transaction_data])
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
    emits.forEach(function (emit) {
      self.emit(emit[0], emit[1])
    })
    callback()
  })
}

Scanner.prototype.parse_mempool_cargo = function (txids, callback) {
  var self = this

  var utxo_bulk = self.Utxo.collection.initializeUnorderedBulkOp()
  utxo_bulk.bulk_name = 'utxo_bulk'
  var raw_transaction_bulk = self.RawTransactions.collection.initializeUnorderedBulkOp()
  raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
  var addresses_transactions_bulk = self.AddressesTransactions.collection.initializeUnorderedBulkOp()
  addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
  var addresses_utxos_bulk = self.AddressesUtxos.collection.initializeUnorderedBulkOp()
  addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
  var assets_utxos_bulk = self.AssetsUtxos.collection.initializeUnorderedBulkOp()
  assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'
  var assets_transactions_bulk = self.AssetsTransactions.collection.initializeUnorderedBulkOp()
  assets_transactions_bulk.bulk_name = 'assets_transactions_bulk'
  var assets_addresses_bulk = self.AssetsAddresses.collection.initializeUnorderedBulkOp()
  assets_addresses_bulk.bulk_name = 'assets_addresses_bulk'

  var command_arr = []
  txids = _.uniq(txids)
  var ph_index = txids.indexOf('PH')
  if (ph_index !== -1) {
    txids.splice(ph_index, 1)
  }
  console.log('parsing mempool cargo (' + txids.length + ')')

  txids.forEach(function (txhash) {
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1]})
  })
  var tx_count = 0
  var tx_parsed_count = 0
  bitcoin_rpc.cmd(command_arr, function (raw_transaction_data, cb) {
    if (!raw_transaction_data) {
      console.log('Null transaction')
      return cb()
    }
    raw_transaction_data = to_discrete(raw_transaction_data)
    self.parse_new_mempool_transaction(raw_transaction_data, raw_transaction_bulk, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, assets_addresses_bulk, function (err) {
      cb(err)
    })
  },
  function (err) {
    if (err) {
      if ('code' in err && err.code === -5) {
        console.error('Can\'t find tx.')
      } else {
        console.error('parse_mempool_cargo: ' + err)
        return callback(err)
      }
    }
    console.log('parsing mempool bulks')
    execute_bulks_parallel([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_utxos_bulk, assets_transactions_bulk, assets_addresses_bulk, raw_transaction_bulk], function (err) {
      if (err) {
        self.to_revert = []
        console.log('killing in the name of!')
        console.error('execute cargo bulk error ', err)
        self.mempool_cargo.kill()
        self.emit('kill')
      }
      callback()
    })
  })
}

Scanner.prototype.revert_txids = function (callback) {
  var self = this

  self.to_revert = _.uniq(self.to_revert)
  if (!self.to_revert.length) return callback()
  console.log('need to revert ' + self.to_revert.length + ' txs from mempool.')
  var n_batch = 100
  // async.whilst(function () { return self.to_revert.length },
    // function (cb) {
      var utxo_bulk = self.Utxo.collection.initializeUnorderedBulkOp()
      utxo_bulk.bulk_name = 'utxo_bulk'
      var raw_transaction_bulk = self.RawTransactions.collection.initializeUnorderedBulkOp()
      raw_transaction_bulk.bulk_name = 'raw_transaction_bulk'
      var addresses_transactions_bulk = self.AddressesTransactions.collection.initializeUnorderedBulkOp()
      addresses_transactions_bulk.bulk_name = 'addresses_transactions_bulk'
      var addresses_utxos_bulk = self.AddressesUtxos.collection.initializeUnorderedBulkOp()
      addresses_utxos_bulk.bulk_name = 'addresses_utxos_bulk'
      var assets_utxos_bulk = self.AssetsUtxos.collection.initializeUnorderedBulkOp()
      assets_utxos_bulk.bulk_name = 'assets_utxos_bulk'
      var assets_transactions_bulk = self.AssetsTransactions.collection.initializeUnorderedBulkOp()
      assets_transactions_bulk.bulk_name = 'assets_transactions_bulk'

      var txids = self.to_revert.slice(0, n_batch)
      console.log('reverting txs (' + txids.length + ',' + self.to_revert.length + ')')

      // logger.debug('reverting '+block_data.tx.length+' txs.')
      var regular_txids = []
      var colored_txids = []

      async.each(txids, function (txid, cb) {
        bitcoin_rpc.cmd('getrawtransaction', [txid], function (err, raw_transaction_data) {
          if (err || !raw_transaction_data || !raw_transaction_data.confirmations) {
            regular_txids.push(txid)
            self.revert_tx(txid, utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, raw_transaction_bulk, function (err, colored) {
              if (err) return cb(err)
              if (colored) {
                colored_txids.push(txid)
              }
              cb()
            })
          } else {
            console.log('found tx that do not need to revert', txid)
            if (~self.to_revert.indexOf(txid)) {
              self.to_revert.splice(self.to_revert.indexOf(txid), 1)
            }
            // No need for now....
            // if blockhash
            // get blockheight
            // parse tx (first parse)
            // if not iosfixed - set block as not fixed
            // if colored and not cc_parsed - set block as not cc_parsed
            cb()
          }
        })
      },
      function (err) {
        if (err) return cb(err)
        // logger.debug('executing bulks')
        execute_bulks_parallel([utxo_bulk, addresses_transactions_bulk, addresses_utxos_bulk, assets_transactions_bulk, assets_utxos_bulk, raw_transaction_bulk], function (err) {
          if (err) return cb(err)
          regular_txids.forEach(function (txid) {
            self.emit('revertedtransaction', {txid: txid})
          })
          colored_txids.forEach(function (txid) {
            self.emit('revertedcctransaction', {txid: txid})
          })
          self.to_revert = []
          callback()
        })
      })
  //   },
  //   callback
  // )
}

Scanner.prototype.parse_new_mempool = function (callback) {
  var self = this

  var db_parsed_txids = []
  var db_unparsed_txids = []
  var new_txids
  var cargo_size
  var limit = 100000
  var has_next = true
  var skip = 0

  if (properties.scanner.mempool !== 'true') return callback()
  console.log('start reverting (if needed)')
  async.waterfall([
    function (cb) {
      self.revert_txids(cb)
    },
    function (cb) {
      console.log('end reverting (if needed)')
      self.emit('mempool')
      var conditions = {
        blockheight: -1
      }
      var projection = {
        txid: 1,
        iosparsed: 1,
        colored: 1,
        ccparsed: 1
      }
      console.log('start find mempool db txs')
      async.whilst(function () { return has_next },
        function (cb) {
          self.RawTransactions.find(conditions, projection, {limit: limit, skip: skip}, function (err, transactions) {
            if (err) return cb(err)
            transactions.forEach(function (transaction) {
              if (transaction.iosparsed && transaction.colored === transaction.ccparsed) {
                db_parsed_txids.push(transaction.txid)
              } else {
                db_unparsed_txids.push(transaction.txid)
              }
            })
            if (transactions.length === limit) {
              console.log('got txs', skip + 1, '-', skip + limit)
              skip += limit
            } else {
              has_next = false
            }
            cb()
          })
        },
      cb)
    },
    function (cb) {
      console.log('end find mempool db txs')
      console.log('start find mempool bitcoind txs')
      bitcoin_rpc.cmd('getrawmempool', [], cb)
    },
    function (whole_txids, cb) {
      whole_txids = whole_txids || []
      console.log('end find mempool bitcoind txs')
      console.log('parsing mempool txs (' + whole_txids.length + ')')
      console.log('start xoring')
      var txids_intersection = _.intersection(db_parsed_txids, whole_txids) // txids that allready parsed in db
      new_txids = _.xor(txids_intersection, whole_txids) // txids that not parsed in db
      db_parsed_txids = _.xor(txids_intersection, db_parsed_txids) // the rest of the txids in the db (not yet found in mempool)
      txids_intersection = _.intersection(db_unparsed_txids, whole_txids) // txids that in mempool and db but not fully parsed
      db_unparsed_txids = _.xor(txids_intersection, db_unparsed_txids) // the rest of the txids in the db (not yet found in mempool, not fully parsed)
      console.log('end xoring')
      new_txids.push('PH')
      console.log('parsing new mempool txs (' + (new_txids.length - 1) + ')')
      cargo_size = new_txids.length
      var ended = 0
      var end_func = function () {
        if (!ended++) {
          self.removeListener('kill', end_func)
          console.log('mempool cargo ended.')
          cb()
        }
      }
      self.once('kill', end_func)
      self.mempool_cargo.push(new_txids, function () {
        if (!--cargo_size) {
          var db_txids = db_parsed_txids.concat(db_unparsed_txids)
          self.to_revert = self.to_revert.concat(db_txids)
          end_func()
        }
      })
    }
  ], callback)
}

Scanner.prototype.wait_for_parse = function (txid, callback) {
  var self = this

  var sent = 0
  var end = function (err) {
    if (!sent++) {
      callback(err)
    }
  }

  var listener = function (transaction) {
    if (transaction.txid === txid) {
      self.removeListener('newtransaction', listener)
      end()
    }
  }
  self.on('newtransaction', listener)

  var conditions = {
    txid: txid,
    iosparsed: true,
    $where: 'this.colored == this.ccparsed'
  }
  var projection = {
    txid: 1
  }
  self.RawTransactions.findOne(conditions, projection).exec(function (err, transaction) {
    if (err) return end(err)
    if (transaction) end()
  })
}

Scanner.prototype.priority_parse = function (txid, callback) {
  var self = this
  var PARSED = 'PARSED'
  var PROCESSING = 'PROCESSING'
  var transaction
  console.time('priority_parse: '+ txid)
  var end = function (err) {
    if (~self.priority_parse_list.indexOf(txid)) {
      self.priority_parse_list.splice(self.priority_parse_list.indexOf(txid), 1)
    }
    console.timeEnd('priority_parse: '+ txid)
    callback(err)
  }
  async.waterfall([
    function (cb) {
      if (~self.priority_parse_list.indexOf(txid)) {
        return self.wait_for_parse(txid, function (err) {
          if (err) return cb(err)
          cb(PARSED)
        })
      }
      console.time('priority_parse: find in db '+ txid)
      self.priority_parse_list.push(txid)
      var conditions = {
        txid: txid,
        iosparsed: true,
        $where: 'this.colored == this.ccparsed'
      }
      var projection = {
        txid: 1
      }
      self.RawTransactions.findOne(conditions, projection).exec(cb)
    },
    function (tx, cb) {
      console.timeEnd('priority_parse: find in db '+ txid)
      if (tx) return cb(PARSED)
      console.time('priority_parse: get_from_bitcoind '+ txid)
      bitcoin_rpc.cmd('getrawtransaction', [txid, 1], function (err, raw_transaction_data) {
        if (err && err.code === -5) return cb(['tx ' + txid + ' not found.', 204])
        cb(err, raw_transaction_data)
      })
    },
    function (raw_transaction_data, cb) {
      console.timeEnd('priority_parse: get_from_bitcoind '+ txid)
      console.time('priority_parse: parse inputs '+ txid)
      transaction = raw_transaction_data
      transaction = to_discrete(transaction)
      if (!transaction || !transaction.vin) return cb(['tx ' + txid + ' not found.', 204])
      async.each(transaction.vin, function (vin, cb2) {
        self.priority_parse(vin.txid, cb2)
      },
      cb)
    },
    function (cb) {
      console.timeEnd('priority_parse: parse inputs '+ txid)
      console.time('priority_parse: parse '+ txid)
      self.mempool_cargo.unshift(txid, cb)
    }
  ],
  function (err) {
    if (err) {
      if (err === PARSED) {
        process.send({to: properties.roles.API, priority_parsed: txid})
        return end()
      }
      process.send({
        to: properties.roles.API,
        priority_parsed: txid,
        err: err
      })
      if (err === PROCESSING) {
        return callback('In the middle of processing.')
      } else {
        return end(err)
      }
    }
    console.timeEnd('priority_parse: parse '+ txid)
    process.send({to: properties.roles.API, priority_parsed: txid})
    end()
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
