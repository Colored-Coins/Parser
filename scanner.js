var async = require('async')
var util = require('util')
var events = require('events')
var _ = require('lodash')
var CCTransaction = require('cc-transaction')
var bitcoin = require('bitcoin-async')
var get_assets_outputs = require('cc-get-assets-outputs')
var squel = require('squel').useFlavour('postgres')
squel.cls.DefaultQueryBuilderOptions.autoQuoteFieldNames = true
squel.cls.DefaultQueryBuilderOptions.nameQuoteCharacter = '"'

var properties
var bitcoin_rpc
var debug

function Scanner (settings, db) {
  var self = this

  debug = settings.debug
  self.to_revert = []
  self.priority_parse_list = []
  properties = settings.properties
  bitcoin_rpc = new bitcoin.Client(settings.rpc_settings)

  self.sequelize = db.sequelize
  self.Sequelize = db.Sequelize
  self.Outputs = db.outputs
  self.Inputs = db.inputs
  self.Blocks = db.blocks
  self.Blocks.properties = properties
  self.Transactions = db.transactions
  self.Transactions.properties = properties
  self.AddressesOutputs = db.addressesoutputs
  self.AddressesTransactions = db.addressestransactions
  self.AssetsOutputs = db.assetsoutputs
  self.AssetsTransactions = db.assetstransactions
  self.AssetsAddresses = db.assetsaddresses
  self.Assets = db.assets
  // self.Blocks = db.model('blocks', require(__dirname + '/models/blocks')(db, properties))
  // self.Transactions = db.model('Transactions', require(__dirname + '/models/Transactions')(db, properties))
  // self.Utxo = db.model('utxo', require(__dirname + '/models/utxo')(db))
  // self.AddressesTransactions = db.model('addressestransactions', require(__dirname + '/models/addressestransactions')(db))
  // self.AddressesUtxos = db.model('addressesutxos', require(__dirname + '/models/addressesutxos')(db))
  // self.AssetsTransactions = db.model('assetstransactions', require(__dirname + '/models/assetstransactions')(db))
  // self.AssetsUtxos = db.model('assetsutxos', require(__dirname + '/models/assetsutxos')(db))
  // self.AssetsAddresses = db.model('assetsaddresses', require(__dirname + '/models/assetsaddresses')(db))

  // if (process.env.ROLE !== properties.roles.API) {
  //   self.on('newblock', function (newblock) {
  //     process.send({to: properties.roles.API, newblock: newblock})
  //   })
  //   self.on('newtransaction', function (newtransaction) {
  //     process.send({to: properties.roles.API, newtransaction: newtransaction})
  //   })
  //   self.on('newcctransaction', function (newcctransaction) {
  //     process.send({to: properties.roles.API, newcctransaction: newcctransaction})
  //   })
  //   self.on('revertedblock', function (revertedblock) {
  //     process.send({to: properties.roles.API, revertedblock: revertedblock})
  //   })
  //   self.on('revertedtransaction', function (revertedtransaction) {
  //     process.send({to: properties.roles.API, revertedtransaction: revertedtransaction})
  //   })
  //   self.on('revertedcctransaction', function (revertedcctransaction) {
  //     process.send({to: properties.roles.API, revertedcctransaction: revertedcctransaction})
  //   })
  //   self.on('mempool', function () {
  //     process.send({to: properties.roles.API, mempool: true})
  //   })
  // }

  // self.mempool_cargo = async.cargo(function (tasks, callback) {
  //   self.parse_mempool_cargo(tasks, callback)
  // }, 500)
}

util.inherits(Scanner, events.EventEmitter)

Scanner.prototype.scan_blocks = function (err) {
  var self = this
  if (err) {
    console.error('scan_blocks: err = ', err)
    return self.scan_blocks()
  }
  var job
  var next_block
  var last_hash

  async.waterfall([
    function (cb) {
      console.log('scan_blocks #1')
      self.get_next_new_block(cb)
    },
    function (l_next_block, l_last_hash, cb) {
      console.log('scan_blocks #2, l_next_block = ', l_next_block, ' l_last_hash = ', l_last_hash)
      last_hash = l_last_hash
      next_block = l_next_block || 0
      self.get_raw_block(next_block, cb)
    },
    function (raw_block_data, cb) {
      console.log('scan_blocks #3, raw_block_data.height = ', raw_block_data.height)
      if (!cb) {
        cb = raw_block_data
        raw_block_data = null
      }
      if (!raw_block_data || (raw_block_data.height === next_block - 1 && raw_block_data.hash === last_hash)) {
        // TODO oded
        // setTimeout(function () {
        //   if (debug) {
        //     job = 'mempool_scan'
        //     console.time(job)
        //   }
        //   self.parse_new_mempool(cb)
        // }, 500)
        cb()
      } else if (!raw_block_data.previousblockhash || raw_block_data.previousblockhash === last_hash) {
        // logger.debug('parsing block')
        if (debug) {
          job = 'parse_new_block'
          console.time(job)
        }
        self.parse_new_block(raw_block_data, cb)
      } else {
        // TODO oded
        // if (debug) {
        //   job = 'reverting_block'
        //   console.time(job)
        // }
        // self.revert_block(next_block - 1, cb)
      }
    }
  ], function (err) {
    if (debug && job) console.timeEnd(job)
    if (err) {
      self.to_revert = []
      self.mempool_txs = null
      console.log('scan_blocks - err = ', err)
    }
    console.log(' --- ended scan_blocks ----')
    self.scan_blocks(err)
  })
}

Scanner.prototype.get_next_block_to_fix = function (limit, callback) {
  var conditions = {
    txsinserted: true, txsparsed: false
  }
  this.Blocks.findAll(
  {
    where: conditions,
    attributes: ['height', 'hash'],
    limit: limit,
    order: [['height', 'ASC']],
    logging: (debug && console.log),
    raw: true
  }).then(function (blocks) { 
    console.log('get_next_block_to_fix - found ' + blocks.length + ' length')
    callback(null, blocks) 
  }).catch(function (e) {
    console.log('get_next_block_to_fix - e = ', e)
    callback(e)
  })
}

Scanner.prototype.get_next_new_block = function (callback) {
  var self = this
  console.log('get_next_new_block')
  if (properties.last_block && properties.last_hash) {
    console.log('properties.last_block = ', properties.last_block)
    return callback(null, properties.last_block + 1, properties.last_hash)
  }
  self.Blocks.findOne({
    where: { txsinserted: true},
    attributes: ['height', 'hash'],
    order: [['height', 'DESC']],
    logging: (debug && console.log)
  }).then(function (block) {
    if (block) {
      set_last_block(block.height)
      set_last_hash(block.hash)
      callback(null, block.height + 1, block.hash)
    } else {
      callback(null, 0, null)
    }
  }).catch(callback)
}

Scanner.prototype.get_raw_block = function (block_height, callback) {
  var self = this
  console.log('get_raw_block(' + block_height + ')')
  bitcoin_rpc.cmd('getblockhash', [block_height], function (err, hash) {
    console.log('getblockhash - err = ', err)
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
  raw_block_data.txsinserted = false
  raw_block_data.ccparsed = false
  raw_block_data.reward = calc_block_reward(raw_block_data.height)
  raw_block_data.totalsent = 0
  raw_block_data.fees = 0
  raw_block_data.txlength = raw_block_data.tx.length
  console.log('parsing new block ' + raw_block_data.height)

  var command_arr = []

  raw_block_data.tx.forEach(function (txhash) {
    if (~self.to_revert.indexOf(txhash)) {
      self.to_revert = []
    }
    if (self.mempool_txs) {
      var mempool_tx_index = -1
      self.mempool_txs.forEach(function (mempool_tx, i) {
        if (!~mempool_tx_index && mempool_tx.txid === txhash) {
          mempool_tx_index = i
        }
      })
      if (~mempool_tx_index) {
        self.mempool_txs.splice(mempool_tx_index, 1)
      }
    }
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1]})
  })

  var index_in_block = 0
  bitcoin_rpc.cmd(command_arr, function (raw_transaction_data, cb) {
    var sql_query = []
    raw_transaction_data = to_discrete(raw_transaction_data)
    raw_transaction_data.index_in_block = index_in_block
    index_in_block++
    var out = self.parse_new_transaction(raw_transaction_data, raw_block_data.height, sql_query)
    if (out) {
      raw_block_data.totalsent += out
      if (is_coinbase(raw_transaction_data)) {
        raw_block_data.fees = out || raw_block_data.reward
        raw_block_data.fees -= raw_block_data.reward
      }
    }
    if (!sql_query.length) return cb()
    sql_query = sql_query.join(';\n')
    self.sequelize.transaction(function (sql_transaction) {
      return self.sequelize.query(sql_query, {transaction: sql_transaction, logging: (debug && console.log) })
        .then(function () { cb() })
        .catch(cb)
    })
  },
  function (err) {
    var sql_query = ''
    if (err) {
      if ('code' in err && err.code === -5) {
        console.error('Can\'t find tx.')
      } else {
        console.error('parse_new_block_err: ', err)
        return callback(err)
      }
    }
    if (debug) console.time('vout_parse_bulks')
    raw_block_data.txsinserted = true

    sql_query += squel.insert()
      .into('blocks')
      .setFields(to_sql_fields(raw_block_data, {exclude: ['tx', 'confirmations']}))
      .toString() + ';\n' // TODO oded

    if (!properties.next_hash && raw_block_data.previousblockhash) {
      sql_query += squel.update()
        .table('blocks')
        .set('nextblockhash', raw_block_data.hash)
        .where('hash = \'' + raw_block_data.previousblockhash + '\'')
        .toString()
    }

    var close_block = function () {
      set_last_hash(raw_block_data.hash)
      set_last_block(raw_block_data.height)
      set_next_hash(raw_block_data.nextblockhash)
      callback()
    }

    self.sequelize.query(sql_query, {logging: (debug && console.log)})
      .then(close_block)
      .catch(callback)
  })
}

var get_opreturn_data = function (hex) {
  return hex.substring(4)
}

var check_version = function (hex) {
  var version = hex.toString('hex').substring(0, 4)
  return (version.toLowerCase() === '4343')
}

Scanner.prototype.parse_new_transaction = function (raw_transaction_data, block_height, sql_query) {
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

  self.parse_vin(raw_transaction_data, block_height, sql_query)
  var out = self.parse_vout(raw_transaction_data, block_height, sql_query)

  raw_transaction_data.iosparsed = false
  raw_transaction_data.ccparsed = false
  var update = {
    blocktime: raw_transaction_data.blocktime,
    blockheight: raw_transaction_data.blockheight,
    blockhash: raw_transaction_data.blockhash,
    time: raw_transaction_data.time
  }

  sql_query.unshift(squel.insert()
    .into('transactions')
    .setFields(to_sql_fields(raw_transaction_data, {exclude: ['vin', 'vout', 'confirmations']}))
    .toString() + ' on conflict (txid) do update set ' + to_sql_update_string(update))

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

Scanner.prototype.parse_vin = function (raw_transaction_data, block_height, sql_query) {
  var self = this
  if (!raw_transaction_data.vin) return
  raw_transaction_data.vin.forEach(function (vin, index) {
    vin.input_txid = raw_transaction_data.txid
    vin.input_index = index
    sql_query.push(squel.insert()
      .into('inputs')
      .setFields(to_sql_fields(vin))
      .toString() + ' on conflict (input_txid, input_index) do nothing')
  })
}

Scanner.prototype.parse_vout = function (raw_transaction_data, block_height, sql_query) {
  var self = this
  var out = 0
  var addresses = []
  if (!raw_transaction_data.vout) return 0
  raw_transaction_data.ccdata = raw_transaction_data.ccdata || []
  raw_transaction_data.vout.forEach(function (vout) {
    if (vout.scriptPubKey.hex.length > 2000) {
      vout.scriptPubKey.hex = null
      vout.scriptPubKey.asm = 'TOBIG'
    }
    if (vout.scriptPubKey && vout.scriptPubKey.type === 'nulldata') {

      var hex = get_opreturn_data(vout.scriptPubKey.hex) // remove op_return (0x6a) and data length?
      if (check_version(hex)) {
        try {
          var cc = CCTransaction.fromHex(hex).toJson()
        } catch (e) {
          console.log('Invalid CC transaction.')
        }
        if (cc) {
          raw_transaction_data.ccdata.push(cc)
          raw_transaction_data.colored = true
        }
      }
    }
    out += vout.value

    var new_utxo = {
      txid: raw_transaction_data.txid,
      n: vout.n,
      value: vout.value
    }
    if (vout.scriptPubKey) {
      new_utxo.scriptPubKey = vout.scriptPubKey
    }

    sql_query.push(squel.insert()
      .into('outputs')
      .setFields(to_sql_fields(new_utxo))
      .toString() + ' on conflict (txid, n) do nothing')

    if (vout.scriptPubKey && vout.scriptPubKey.addresses) {
      vout.scriptPubKey.addresses.forEach(function (address) {
        addresses.push(address)
        sql_query.push(squel.insert()
          .into('addressesoutputs')
          .set('address', address)
          .set('output_id',
            squel.select().field('id').from('outputs').where('txid = \'' +  new_utxo.txid + '\' AND n = ' + new_utxo.n))
          .toString() + ' on conflict (address, "output_id") do nothing')
      })
    }
  })

  addresses = _.uniq(addresses)
  addresses.forEach(function (address) {
    sql_query.push(squel.insert()
      .into('addressestransactions')
      .set('address', address)
      .set('txid', raw_transaction_data.txid)
      .toString() + ' on conflict (address, txid) do nothing')    
  })

  return out
}

Scanner.prototype.fix_blocks = function (err, callback) {
  var self = this
  if (err) {
    console.error('fix_blocks: err = ', err)
    return self.fix_blocks(null, callback)
  }
  var emits = []
  callback = callback || function (err) {
    self.fix_blocks(err)
  }
  self.get_next_block_to_fix(50, function (err, raw_block_datas) {
    if (err) return callback(err)
    console.log('fix_blocks - get_next_block_to_fix: ' + raw_block_datas.length + ' blocks')
    if (!raw_block_datas || !raw_block_datas.length) {
      return setTimeout(function () {
        callback()
      }, 500)
    }
    var first_block = raw_block_datas[0].height
    var num_of_blocks = raw_block_datas.length
    var last_block = raw_block_datas[num_of_blocks - 1].height

    var close_blocks = function (err, empty) {
      console.log('fix_blocks - close_blocks: err = ', err, ', empty = ', empty)
      if (err) return callback(err)
      emits.forEach(function (emit) {
        self.emit(emit[0], emit[1])
      })
      var blocks_heights = _.map(raw_block_datas, 'height')
      if (!blocks_heights.length) {
        return setTimeout(function () {
          callback()
        }, 500)
      }
      var update = {
        txsparsed: true,
        ccparsed: false
      }
      var conditions = {
        height: {$in: blocks_heights}
      }
      self.Blocks.update(
        update,
        {
          where: conditions,
          logging: (debug && console.log)
        }
      ).then(function (res) {
        console.log('fix_blocks - close_blocks - success')
        callback()
      }).catch(function (e) {
        console.log('fix_blocks - close_blocks - e = ', e)
        callback(e)
      })
    }

    self.get_need_to_fix_transactions_by_blocks(first_block, last_block, function (err, transactions_datas) {
      if (err) return callback(err)
      console.log('Fixing blocks ' + first_block + '-' + last_block + ' (' + transactions_datas.length + ' txs).')
      if (!transactions_datas) return callback('can\'t get transactions from db')
      if (!transactions_datas.length) {
        return close_blocks(null, true)
      }
      async.each(transactions_datas, function (transaction_data, cb) {
        var sql_query = []
        transaction_data = transaction_data.toJSON()
        self.fix_transaction(transaction_data, sql_query)
        sql_query = sql_query.join(';\n')
        self.sequelize.transaction(function (sql_transaction) {
          return self.sequelize.query(sql_query, {transaction: sql_transaction, logging: (debug && console.log)})
            .then(function () {
              if (!transaction_data.colored) {
                emits.push(['newtransaction', transaction_data])
              }
              console.log('get_need_to_fix_transactions_by_blocks() - then')
              cb()
            })
            .catch(function (err) {
              console.log('get_need_to_fix_transactions_by_blocks() - err = ', err)
              cb(err)
            })
        })
      }, close_blocks)
    })
  })
}

Scanner.prototype.parse_cc = function (err, callback) {
  var self = this
  if (err) {
    console.error('parse_cc: err = ', err)
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

    var did_work = false
    var close_blocks = function (err, empty) {
      if (debug) console.timeEnd('parse_cc_bulks')
      if (err) return callback(err)
      emits.forEach(function (emit) {
        self.emit(emit[0], emit[1])
      })
      if (!empty) {
        if (did_work) {
          return callback()
        } else {
          return setTimeout(function () {
            callback()
          }, 500)
        }
      }

      raw_block_datas.forEach(function (raw_block_data) {
        self.emit('newblock', raw_block_data)
        set_last_fully_parsed_block(raw_block_data.height)
      })

      var blocks_heights = _(raw_block_datas).filter('txsparsed').map('height').value()
      if (!blocks_heights.length) {
        return setTimeout(function () {
          callback()
        }, 500)
      }
      console.log('parse_cc: close_blocks ' + blocks_heights[0] + '-' + blocks_heights[blocks_heights.length - 1])
      var update = {
        ccparsed: true
      }
      var conditions = {
        height: {$in: blocks_heights}
      }
      self.Blocks.update(
        update,
        {
          where: conditions,
          logging: (debug && console.log)
        }
      ).then(function (res) {
        console.log('parse_cc - close_blocks - success')
        callback()
      }).catch(function (e) {
        console.log('parse_cc - close_blocks - e = ', e)
        callback(e)
      })
    }

    self.get_need_to_cc_parse_transactions_by_blocks(first_block, last_block, function (err, transactions_data) {
      if (err) return self.parse_cc(err)
      console.log('Parsing cc for blocks ' + first_block + '-' + last_block + ' (' + transactions_data.length + ' txs).')
      if (!transactions_data) return callback('can\'t get transactions from db')
      if (!transactions_data.length) {
        if (debug) console.time('parse_cc_bulks')
        return close_blocks(null, true)
      }

      async.each(transactions_data, function (transaction_data, cb) {
        var sql_query = []
        transaction_data = transaction_data.toJSON()
        self.parse_cc_tx(transaction_data, sql_query)
        if (!transaction_data.iosparsed) {
          return cb()
        }
        did_work = true
        sql_query = sql_query.join(';\n')
        emits.push(['newcctransaction', transaction_data])
        emits.push(['newtransaction', transaction_data])
        self.sequelize.transaction(function (sql_transaction) {
          return self.sequelize.query(sql_query, {transaction: sql_transaction, logging: (debug && console.log)})
            .then(function () { cb() })
            .catch(cb)
        })
      }, close_blocks)
    })
  })
}

Scanner.prototype.parse_cc_tx = function (transaction_data, sql_query) {
  if (!transaction_data.iosparsed || !transaction_data.ccdata || !transaction_data.ccdata.length) {
    return
  }

  var assetsArrays = get_assets_outputs(transaction_data)
  var index = 0
  assetsArrays.forEach(function (assetArray, out_index) {
    index = out_index
    if (!assetArray) {
      return
    }
    transaction_data.vout[out_index].assets = asset
    var conditions = {
      txid: transaction_data.txid,
      index: out_index
    }

    assetArray.forEach(function (asset, index_in_output) {
      var type = null
      if (transaction_data.ccdata && transaction_data.ccdata.length && transaction_data.ccdata[0].type) {
        type = transaction_data.ccdata[0].type
      }
      if (type === 'issuance') {
        sql_query.push(squel.insert()
          .into('assets')
          .set('assetId', asset.assetId)
          .set('issueTxid', asset.issueTxid)
          .set('divisibility', asset.divisibility)
          .set('aggregationPolicy', asset.aggregationPolicy)
          .toString() + ' on conflict ("assetId") do nothing')  
      }
      sql_query.push(squel.insert()
        .into('assetstransactions')
        .set('assetId', asset.assetId)
        .set('txid', transaction_data.txid)
        .set('type', type)
        .toString() + ' on conflict ("assetId") do nothing')
      sql_query.push(squel.insert()
        .into('assetsoutputs')
        .set('assetId', asset.assetId)
        .set('output_id', transaction_data.vout[out_index].id)
        .set('index_in_output', index_in_output)
        .toString() + ' on conflict ("assetId", "output_id") do nothing')
      if (asset.amount && transaction_data.vout[out_index].scriptPubKey && transaction_data.vout[out_index].scriptPubKey.addresses) {
        transaction_data.vout[out_index].scriptPubKey.addresses.forEach(function (address) {
          sql_query.push(squel.insert()
            .into('assetsaddresses')
            .set('assetId', asset.assetId)
            .set('address', address)
            .toString() + ' on conflict ("assetId", address) do nothing')
        })
      }
      sql_query.push(squel.update()
        .into('transactions')
        .set('ccparsed', true)
        .set('overflow', transaction_data.overflow || false)
        .where('txid = \'' + transaction_data.txid + '\'')
        .toString())
    })
  })
}

Scanner.prototype.get_need_to_cc_parse_transactions_by_blocks = function (first_block, last_block, callback) {
  var conditions = {
    colored: true,
    ccparsed: false,
    blockheight: {$between: [first_block, last_block]}
  }
  this.Transactions.findAll({
    where: conditions,
    limit: 1000,
    include: [
      { model: this.Inputs, as: 'vin' },
      { model: this.Outputs, as: 'vout', include: [
        { model: this.AssetsOutputs, include: [
          { model: this.Assets, as: 'asset' }
        ]}
      ]}
    ],
    order: [['blockheight', 'ASC']],
    logging: (debug && console.log)
  }).then(function (transactions) {
    console.log('get_need_to_cc_parse_transactions_by_blocks - transactions.length = ', transactions.length)
    callback(null, transactions)
  }).catch(function (e) {
    console.log('get_need_to_cc_parse_transactions_by_blocks - e = ', e)
    callback(e)
  })
}

Scanner.prototype.get_need_to_fix_transactions_by_blocks = function (first_block, last_block, callback) {
  var conditions = {
    iosparsed: false,
    blockheight: {$between: [first_block, last_block]}
  }
  this.Transactions.findAll({
    where: conditions,
    limit: 200,
    include: [
      { model: this.Inputs, as: 'vin' },
      { model: this.Outputs, as: 'vout' }
    ],
    order: [['blockheight', 'ASC']/*, ['tries', 'ASC']*/],
    logging: (debug && console.log)
  }).then(function (transactions) {
    console.log('get_need_to_fix_transactions_by_blocks - transactions.length = ', transactions.length)
    callback(null, transactions)
  }).catch(function (e) {
    console.log('get_need_to_fix_transactions_by_blocks - e = ', e)
    callback(e)
  })
}

Scanner.prototype.fix_transaction = function (raw_transaction_data, sql_query, callback) {
  raw_transaction_data.vin.filter(function (input) { return !input.coinbase }).forEach(function (input) {
    fix_vin(input, sql_query)
    fix_used_vout(input.txid, input.vout, raw_transaction_data.txid, raw_transaction_data.blockheight, sql_query)
  })

  sql_query.push(squel.update()
    .table('transactions')
    .set('iosparsed', true)
    // .set('fee', raw_transaction_data.fee)
    // .set('totalsent', raw_transaction_data.totalsent)
    .where('txid = \'' + raw_transaction_data.txid + '\'')
    .toString())
}

var fix_vin = function (input, sql_query) {
  sql_query.push(squel.update()
    .table('inputs')
    .set('output_id',
      squel.select().field('id').from('outputs').where('txid = \'' +  input.txid + '\' AND n = ' + input.vout))
    .where('input_txid = \'' + input.input_txid + '\' AND input_index = ' + input.input_index)
    .toString())
}

var fix_used_vout = function (txid, vout, usedTxid, usedBlockheight, sql_query) {
  sql_query.push(squel.update()
    .table('outputs')
    .set('used', true)
    .set('usedTxid', usedTxid)
    .set('usedBlockheight', usedBlockheight)
    .where('txid = \'' + txid + '\' AND n = ' + vout)
    .toString())
}

var set_next_hash = function (next_hash) {
  properties.next_hash = next_hash
}

var set_last_hash = function (last_hash) {
  properties.last_hash = last_hash
}

var set_last_block = function (last_block) {
  properties.last_block = last_block
  process.send({to: properties.roles.API, last_block: last_block})
}

var set_last_fully_parsed_block = function (last_fully_parsed_block) {
  properties.last_fully_parsed_block = last_fully_parsed_block
  process.send({to: properties.roles.API, last_fully_parsed_block: last_fully_parsed_block})
}

Scanner.prototype.get_next_block_to_cc_parse = function (limit, callback) {
  var conditions = {
    ccparsed: false
  }
  var attributes = [
    'height',
    'hash',
    'txsparsed'
  ]
  this.Blocks.findAll({where: conditions, attributes: attributes, order: [['height', 'ASC']], limit: limit, logging: (debug && console.log), raw: true})
    .then(function (blocks) { callback(null, blocks) })
    .catch(function (e) {
      console.error('get_next_block_to_cc_parse: e = ', e)
      callback(e)
    })
}

var is_coinbase = function (tx_data) {
  return (tx_data && tx_data.vin && tx_data.vin.length === 1 && tx_data.vin[0].coinbase)
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

var to_sql_fields = function (obj, options) {
  var copyObj = _.cloneDeep(obj)
  var exclude = (options && options.exclude) || []
  Object.keys(copyObj).forEach(function (key) {
    if (exclude.indexOf(key) > -1) {
      delete copyObj[key]
      return
    }
    if (typeof copyObj[key] === 'object') {
      copyObj[key] = JSON.stringify(copyObj[key])
    }
  })
  return copyObj
}

var to_sql_update_string = function (obj) {
  var res = '('
  // keys
  Object.keys(obj).forEach(function (key, i, keys) {
    res += key
    if (i < keys.length - 1) {
      res += ', '
    }
  })
  res += ') = ('
  // values
  Object.keys(obj).forEach(function (key, i, keys) {
    res += (typeof obj[key] === 'string') ? ('\'' + obj[key] + '\'') : obj[key]
    if (i < keys.length - 1) {
      res += ', '
    }
  })
  res += ')'
  return res
}

module.exports = Scanner