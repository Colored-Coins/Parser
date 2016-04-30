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

  self.mempool_cargo = async.cargo(function (tasks, callback) {
    console.log('async.cargo() - parse_mempool_cargo')
    self.parse_mempool_cargo(tasks, callback)
  }, /*500*/ 2)
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
    raw: true
  }).then(function (blocks) { 
    console.log('get_next_block_to_fix - found ' + blocks.length + ' blocks')
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
    order: [['height', 'DESC']]
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
      return self.sequelize.query(sql_query, {transaction: sql_transaction})
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

    self.sequelize.transaction(function (sql_transaction) {
      return self.sequelize.query(sql_query, {transaction: sql_transaction})
        .then(close_block)
        .catch(callback)
    })
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

  // put this query first because of outputs and inputs foreign key constraints, validate trasnaction in DB
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

var get_block_height = function (blockhash, callback) {
  bitcoin_rpc.cmd('getblock', [blockhash], function (err, block) {
    if (err) return callback(err)
    callback(null, block.height)
  })
}

Scanner.prototype.parse_vin = function (raw_transaction_data, block_height, sql_query) {
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
          .toString() + ' on conflict (address, output_id) do nothing')
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
      if (!empty) return callback()
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
          where: conditions
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
        self.fix_transaction(transaction_data, sql_query, function (err, all_fixed) {
          if (err) return cb(err)
          sql_query = sql_query.join(';\n')
          self.sequelize.transaction(function (sql_transaction) {
            return self.sequelize.query(sql_query, {transaction: sql_transaction})
              .then(function () {
                if (!transaction_data.colored && all_fixed) {
                  emits.push(['newtransaction', transaction_data])
                }
                cb()
              })
              .catch(function (err) {
                console.log('get_need_to_fix_transactions_by_blocks() - err = ', err)
                cb(err)
              })
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

      var blocks_heights = []
      raw_block_datas.forEach(function (raw_block_data) {
        if (raw_block_data.txsparsed) {
          self.emit('newblock', raw_block_data)
          set_last_fully_parsed_block(raw_block_data.height)
          blocks_heights.push(raw_block_data.height)
        }
      })

      console.log('blocks_heights.length = ', blocks_heights.length)
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
          where: conditions
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
        sql_query.push(squel.update()
          .into('transactions')
          .set('ccparsed', true)
          .set('overflow', transaction_data.overflow || false)
          .where('iosparsed AND txid = \'' + transaction_data.txid + '\'')
          .toString())
        sql_query = sql_query.join(';\n')
        emits.push(['newcctransaction', transaction_data])
        emits.push(['newtransaction', transaction_data])
        self.sequelize.transaction(function (sql_transaction) {
          return self.sequelize.query(sql_query, {transaction: sql_transaction})
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
        .toString() + ' on conflict ("assetId", output_id) do nothing')
      if (asset.amount && transaction_data.vout[out_index].scriptPubKey && transaction_data.vout[out_index].scriptPubKey.addresses) {
        transaction_data.vout[out_index].scriptPubKey.addresses.forEach(function (address) {
          sql_query.push(squel.insert()
            .into('assetsaddresses')
            .set('assetId', asset.assetId)
            .set('address', address)
            .toString() + ' on conflict ("assetId", address) do nothing')
        })
      }
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
    order: [['blockheight', 'ASC']]
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
    attributes: ['txid', 'blockheight', 'tries', 'colored'],
    include: [
      { model: this.Inputs, as: 'vin', attributes: {exclude: ['scriptSig']} },
      { model: this.Outputs, as: 'vout', attributes: {exclude: ['scriptPubKey']} }
    ],
    order: [
      ['blockheight', 'ASC'], 
      ['tries', 'ASC'],
      [{model: this.Inputs, as: 'vin'}, 'input_index', 'ASC'],
      [{model: this.Outputs, as: 'vout'}, 'n', 'ASC']
    ],
    raw: true,
    nest: true
  }).then(function (transactions) {
    transactions = _(transactions)
      .groupBy('txid')
      .transform(function (result, txs, txid) {
        result.push({
          txid: txid, 
          blockheight: txs[0].blockheight,
          tries: txs[0].tries,
          colored: txs[0].colored,
          vin: _.map(txs, 'vin'),
          vout: _.map(txs, 'vout')
        })
        return result
      }, [])
      .value()
    console.log('get_need_to_fix_transactions_by_blocks - transactions.length = ', transactions.length)
    callback(null, transactions)
  }).catch(function (e) {
    console.log('get_need_to_fix_transactions_by_blocks - e = ', e)
    callback(e)
  })
}

Scanner.prototype.fix_transaction = function (raw_transaction_data, sql_query, callback) {
  this.fix_vin(raw_transaction_data, raw_transaction_data.blockheight, sql_query, function (err, all_fixed) {
    if (err) return callback(err)
    sql_query.push(squel.update()
      .table('transactions')
      .set('iosparsed', all_fixed)
      .set('tries', raw_transaction_data.tries || 0)
      .set('fee', raw_transaction_data.fee)
      .set('totalsent', raw_transaction_data.totalsent)
      .where('txid = \'' + raw_transaction_data.txid + '\'')
      .toString())
    callback(null, all_fixed)
  })
}

Scanner.prototype.fix_vin = function (raw_transaction_data, blockheight, sql_query, callback) {
  // fixing a transaction - we need to fulfill the condition where a transaction is iosparsed if and only if
  // all of its inputs are fixed. 
  // An input is fixed when it is assigned with the output_id of its associated output, and the output is marked as used.
  // If the transaction is colored - this should happen only after this output's transaction is both iosparsed AND ccparsed.
  // Otherwise, it is enough for it to be in DB.

  var self = this
  var coinbase = false
  var inputsToFix = {}
  var conditions = []

  if (!raw_transaction_data.vin) {
    return callback('transaction ' + raw_transaction_data.txid + ' does not have vin.')
  }

  var end = function (in_transactions) {
    var inputsToFixNow = []
    in_transactions.forEach(function (in_transaction) {
      in_transaction.vout.forEach(function (vout) {
        var input
        if (in_transaction.txid + ':' + vout.n in inputsToFix) {
          input = inputsToFix[in_transaction.txid + ':' + vout.n]
          input.value = vout.value
          input.output_id = vout.id
          inputsToFixNow.push(input)
        }
      })
    })

    inputsToFixNow.forEach(function (input) {
      self.fix_input(input, sql_query)
      self.fix_used_output(input.txid, input.vout, raw_transaction_data.txid, blockheight, sql_query)
    })
    var all_fixed = (inputsToFixNow.length ===  Object.keys(inputsToFix).length)
    if (all_fixed) {
      calc_fee(raw_transaction_data)
    } else {
      raw_transaction_data.tries = raw_transaction_data.tries || 0
      raw_transaction_data.tries++
      if (raw_transaction_data.tries > 1000) {
        console.warn('transaction', raw_transaction_data.txid, 'has un parsed inputs (', Object.keys(inputsToFix), ') for over than 1000 tries.')
      }
    }
    callback(null, all_fixed)  
  }

  raw_transaction_data.vin.forEach(function (vin) {
    if (vin.coinbase) {
      coinbase = true
    } else if (!vin.fixed) {
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
      inputsToFix[vin.txid + ':' + vin.vout] = vin
    }
  })

  if (coinbase) {
    return end([])
  }
  self.Transactions.findAll({
    where: {$or: conditions},
    attributes: [],
    include: [
      { model: self.Outputs, as: 'vout', attributes: ['id', 'txid', 'n', 'value'] }
    ],
    raw: true,
    nest: true
  })
  .then(function (transactions) {
    transactions = _(transactions)
      .groupBy('vout.txid')
      .transform(function (result, txs, txid) {
        result.push({txid: txid, vout: _.map(txs, 'vout') })
        return result
      }, [])
      .value()
    end(transactions)
  })
  .catch(callback)
}

Scanner.prototype.fix_input = function (input, sql_query) {
  sql_query.push(squel.update()
    .table('inputs')
    .set('output_id', input.output_id)
    .set('value', input.value)
    .set('fixed', true)
    .where('input_txid = \'' + input.input_txid + '\' AND input_index = ' + input.input_index)
    .toString())
}

Scanner.prototype.fix_used_output = function (txid, vout, usedTxid, usedBlockheight, sql_query) {
  sql_query.push(squel.update()
    .table('outputs')
    .set('used', true)
    .set('usedTxid', usedTxid)
    .set('usedBlockheight', usedBlockheight)
    .where('txid = \'' + txid + '\' AND n = ' + vout)
    .toString())
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
  this.Blocks.findAll({where: conditions, attributes: attributes, order: [['height', 'ASC']], limit: limit, raw: true})
    .then(function (blocks) { callback(null, blocks) })
    .catch(function (e) {
      console.error('get_next_block_to_cc_parse: e = ', e)
      callback(e)
    })
}

Scanner.prototype.parse_new_mempool_transaction = function (raw_transaction_data, sql_query, emits, callback) {
  console.log('parse_new_mempool_transaction - txid = ', raw_transaction_data.txid)
  var self = this
  var transaction_data
  var did_work = false
  var iosparsed
  var ccparsed
  var blockheight = -1
  var emits = []
  var conditions = {
    txid: raw_transaction_data.txid
  }
  async.waterfall([
    function (cb) {
      self.Transactions.findOne({
        where: conditions, 
        attributes: {exclude: ['index_in_block']},
        include: [
          { model: self.Inputs, as: 'vin', attributes: {exclude: ['output_id', 'input_txid', 'input_index']}, include: [
            { model: self.Outputs, as: 'previousOutput', attributes: {exclude: ['id', 'txid']} }
          ]},
          { model: self.Outputs, as: 'vout', attributes: {exclude: ['id', 'txid']} }
        ],
        order: [
          [{model: self.Inputs, as: 'vin'}, 'input_index', 'ASC'],
          [{model: self.Outputs, as: 'vout'}, 'n', 'ASC']
        ]
      }).then(function (tx) { cb(null, tx) }).catch(cb)
    },
    function (l_transaction_data, cb) {
      transaction_data = l_transaction_data
      if (transaction_data) {
        raw_transaction_data = transaction_data
        blockheight = raw_transaction_data.blockheight || -1
        cb(null, blockheight)
      } else {
        logger.debug('parse_new_mempool_transaction: did not find in DB, parsing new tx: ' + raw_transaction_data.txid)
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
      logger.debug('parse_new_mempool_transaction: l_blockheight = ' + l_blockheight)
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

      self.parse_vin(raw_transaction_data, blockheight, sql_query)
      self.parse_vout(raw_transaction_data, blockheight, sql_query)
      raw_transaction_data.iosparsed = false
      raw_transaction_data.ccparsed = false
      cb()
    },
    function (cb) {
      if (raw_transaction_data.iosparsed) {
        cb(null, null, true)
      } else {
        did_work = true
        self.fix_vin(raw_transaction_data, blockheight, sql_query, cb)
      }
    },
    function (all_fixed, cb) {
      if (raw_transaction_data.ccparsed) {
        cb(null, null)
      } else {
        raw_transaction_data.iosparsed = all_fixed
        if (all_fixed && raw_transaction_data.colored && !raw_transaction_data.ccparsed) {
          self.parse_cc_tx(raw_transaction_data, sql_query)
          raw_transaction_data.ccparsed = true
          did_work = true
        }
        if (did_work && all_fixed) {
          emits.push(['newtransaction', raw_transaction_data])
          if (raw_transaction_data.colored) {
            emits.push(['newcctransaction', raw_transaction_data])
          }
        }
        if (did_work) {
          // put this query first because of outputs and inputs foreign key constraints, validate trasnaction in DB
          sql_query.unshift(squel.insert()
            .into('transactions')
            .setFields(to_sql_fields(raw_transaction_data, {exclude: ['vin', 'vout', 'confirmations']}))
            .toString() + ' on conflict (txid) do update set ' + to_sql_update_string({iosparsed: raw_transaction_data.iosparsed, ccparsed: raw_transaction_data.ccparsed}))
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
    callback(null, did_work, raw_transaction_data.iosparsed, raw_transaction_data.ccparsed)
  })
}

Scanner.prototype.parse_mempool_cargo = function (txids, callback) {
  console.log('parse_mempool_cargo - txids = ', txids)
  var self = this

  var new_mempool_txs = []
  var command_arr = []
  var emits = []
  txids = _.uniq(txids)
  var ph_index = txids.indexOf('PH')
  if (ph_index !== -1) {
    txids.splice(ph_index, 1)
  }
  console.log('parsing mempool cargo (' + txids.length + ')')

  txids.forEach(function (txhash) {
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1]})
  })
  
  var handleError = function (err) {
    self.to_revert = []
    self.mempool_txs = null
    console.log('killing in the name of!')
    console.error('execute cargo bulk error ', err)
    self.mempool_cargo.kill()
    self.emit('kill')
    return callback(err)
  }

  bitcoin_rpc.cmd(command_arr, function (raw_transaction_data, cb) {
    console.log('received result from bitcoind, raw_transaction_data.txid = ', raw_transaction_data.txid)
    var sql_query = []
    if (!raw_transaction_data) {
      console.log('Null transaction')
      return cb()
    }
    raw_transaction_data = to_discrete(raw_transaction_data)
    self.parse_new_mempool_transaction(raw_transaction_data, sql_query, emits, function (err, did_work, iosparsed, ccparsed) {
      logger.info('parse_new_mempool: parse_new_mempool_transaction ended - did_work = ' + did_work + ', iosparsed = ' + iosparsed + ', ccparsed = ', ccparsed)
      if (err) return cb(err)
      if (!did_work) {
        return cb()
      }
      new_mempool_txs.push({
        txid: raw_transaction_data.txid,
        iosparsed: iosparsed,
        colored: raw_transaction_data.colored,
        ccparsed: ccparsed
      })
      if (!sql_query.length) return cb()
      sql_query = sql_query.join(';\n')
      self.sequelize.transaction(function (sql_transaction) {
        return self.sequelize.query(sql_query, {transaction: sql_transaction, logging: console.log, benchmark: true})
          .then(function () { cb() })
          .catch(cb)
      })
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
    if (self.mempool_txs) {
      new_mempool_txs.forEach(function (mempool_tx) {
        var found = false
        self.mempool_txs.forEach(function (self_mempool_tx) {
          if (!found && mempool_tx.txid === self_mempool_tx.txid) {
            found = true
            self_mempool_tx.iosparsed = mempool_tx.iosparsed
            self_mempool_tx.colored = mempool_tx.colored
            self_mempool_tx.ccparsed = mempool_tx.ccparsed
          }
        })
        if (!found) {
          self.mempool_txs.push({
            txid: mempool_tx.txid,
            iosparsed: mempool_tx.iosparsed,
            colored: mempool_tx.colored,
            ccparsed: mempool_tx.ccparsed
          })
        }
      })
    }
    emits.forEach(function (emit) {
      self.emit(emit[0], emit[1])
    })
    callback()
  })
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
    ccparsed: {$col: 'colored'}
  }
  var attributes = ['txid']
  self.Transactions.findOne({ where: conditions, attributes: attributes, raw: true })
  .then(function (transaction) {
    if (transaction) end()
  })
  .catch(end)
}

Scanner.prototype.priority_parse = function (txid, callback) {
  var self = this
  var PARSED = 'PARSED'
  var transaction
  console.log('start priority_parse: '+ txid)
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
        ccparsed: {$col: 'colored'}
      }
      var attributes = ['txid']
      self.Transactions.findOne({ where: conditions, attributes: attributes, logging: console.log, benchmark: true, raw: true })
      .then(function (tx) { cb(null, tx) })
      .catch(cb)
    },
    function (tx, cb) {
      console.timeEnd('priority_parse: find in db ' + txid)
      if (tx) return cb(PARSED)
      console.time('priority_parse: get_from_bitcoind ' + txid)
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
        return end()
      }
      end(err)
    }
    console.timeEnd('priority_parse: parse '+ txid)
    end()
  })
}

Scanner.prototype.get_info = function (callback) {
  bitcoin_rpc.cmd('getinfo', [], callback)
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