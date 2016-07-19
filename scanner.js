var async = require('async')
var util = require('util')
var events = require('events')
var _ = require('lodash')
var CCTransaction = require('cc-transaction')
var bitcoin = require('bitcoin-async')
var get_assets_outputs = require('cc-get-assets-outputs')
var squel = require('squel')
squel.cls.DefaultQueryBuilderOptions.autoQuoteFieldNames = true
squel.cls.DefaultQueryBuilderOptions.nameQuoteCharacter = '"'
squel.cls.DefaultQueryBuilderOptions.separator = '\n'
var sql_builder = require('nodejs-sql')(squel)

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

  if (process.env.ROLE !== properties.roles.API) {
    self.on('newblock', function (newblock) {
      process.send({to: properties.roles.API, newblock: newblock})
    })
    self.on('newtransaction', function (newtransaction) {
      process.send({to: properties.roles.API, newtransaction: newtransaction})
    })
    self.on('newcctransaction', function (newcctransaction) {
      console.log('scanner.js: ' + process.env.ROLE + ' newcctransaction ', newcctransaction)
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
    console.log('async.cargo() - parse_mempool_cargo')
    self.parse_mempool_cargo(tasks, callback)
  }, 100)
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
        setTimeout(function () {
          if (debug) {
            job = 'mempool_scan'
            console.time(job)
          }
          self.parse_new_mempool(cb)
        }, 500)
      } else if (!raw_block_data.previousblockhash || raw_block_data.previousblockhash === last_hash) {
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
    if (err) {
      self.to_revert = []
      self.mempool_txs = null
      console.log('scan_blocks - err = ', err)
    }
    console.log(' --- ended scan_blocks ----')
    self.scan_blocks(err)
  })
}

Scanner.prototype.revert_block = function (block_height, callback) {
  var self = this
  console.log('Reverting block: ' + block_height)
  var find_block_query = '' +
    'SELECT\n' +
    '  hash,\n' +
    '  previousblockhash,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      transactions.txid\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '          txid\n' +
    '        FROM\n' +
    '          transactions\n' +
    '        WHERE\n' +
    '          transactions.blockheight = blocks.height\n' +
    '        ORDER BY\n' +
    '          transactions.index_in_block) AS transactions)) AS tx\n' +
    'FROM\n' +
    '  blocks\n' +
    'WHERE\n' +
    '  blocks.height = :height'

  self.sequelize.query(find_block_query, {type: self.sequelize.QueryTypes.SELECT, replacements: {height: block_height}})
    .then(function (block_data) {
      if (!block_data || !block_data.length) return callback('no block for height ' + block_height)
      block_data = block_data[0]
      if (!block_data.tx) return callback('no transactions in block for height ' + block_height)
      var block_id = {
        height: block_height,
        hash: block_data.hash
      }

      // logger.debug('reverting '+block_data.tx.length+' txs.')
      var txids = []
      var colored_txids = []
      var sql_query = []
      async.mapSeries(block_data.tx.reverse(), function (txid, cb) {
        txids.push(txid)
        self.revert_tx(txid, sql_query, function (err, colored, revert_flags_txids) {
          if (err) return cb(err)
          if (colored) {
            colored_txids.push(txid)
          }
          cb(null, revert_flags_txids)
        })
      },
      function (err, revert_flags_txids) {
        if (err) return callback(err)
        revert_flags_txids = [].concat.apply([], revert_flags_txids)
        revert_flags_txids = _(revert_flags_txids).uniq().filter(function (txid) { return txid }).value()
        console.log('revert flags txids:', revert_flags_txids)
        if (revert_flags_txids.length) {
          sql_query.push(squel.update()
            .table('transactions')
            .set('iosparsed', false)
            .set('ccparsed', false)
            .where('txid IN ?', revert_flags_txids)
            .toString())
          sql_query.push(squel.update()
            .table('inputs')
            .set('output_id', null)
            .where('input_txid IN ?', revert_flags_txids)
            .toString())
        }
        // logger.debug('executing bulks')
        sql_query = sql_query.join(';\n')
        self.sequelize.query(sql_query)
          .then(function () {
            txids.forEach(function (txid) {
              self.emit('revertedtransaction', {txid: txid})
            })
            colored_txids.forEach(function (txid) {
              self.emit('revertedcctransaction', {txid: txid})
            })
            self.fix_mempool(function (err) {
              // logger.debug('deleting block')
              if (err) return callback(err)
              var delete_blocks_query = squel.delete()
                .from('blocks')
                .where('height = ?', block_height)
                .toString()
              self.sequelize.query(delete_blocks_query)
                .then(function () {
                  self.emit('revertedblock', block_id)
                  set_last_hash(block_data.previousblockhash)
                  set_last_block(block_data.height - 1)
                  set_next_hash(null)
                  callback()
                }).catch(callback)
            })
          }).catch(callback)
      })
    }).catch(callback)
}

Scanner.prototype.fix_mempool = function (callback) {
  var fix_mempool_query = squel.update()
    .table('transactions')
    .set('iosparsed', false)
    .set('ccparsed', false)
    .where('blockheight = -1')
    .toString()
  this.sequelize.query(fix_mempool_query).then(function () { callback() }).catch(callback)
}

Scanner.prototype.revert_tx = function (txid, sql_query, callback) {
  var self = this
  // console.log('reverting tx ' + txid)
  var find_transaction_query = '' +
    'SELECT\n' +
    '  transactions.colored,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vin\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        inputs.input_index,\n' +
    '        inputs.txid,\n' +
    '        inputs.vout\n' +
    '      FROM\n' +
    '        inputs\n' +
    '      WHERE\n' +
    '        inputs.input_txid = transactions.txid\n' +
    '      ORDER BY input_index) AS vin)) AS vin,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vout\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        outputs.id,\n' +
    '        outputs."usedTxid"\n' +
    '      FROM\n' +
    '        outputs\n' +
    '      WHERE outputs.txid = transactions.txid\n' +
    '      ORDER BY n) AS vout)) AS vout\n' +
    'FROM\n' +
    '  transactions\n' +
    'WHERE\n' +
    '  txid = :txid'
  self.sequelize.query(find_transaction_query, {replacements: {txid: txid}, type: self.sequelize.QueryTypes.SELECT})
    .then(function (transactions) {
      if (!transactions || !transactions.length) return callback()
      var next_txids = []
      var transaction = transactions[0]
      self.revert_vin(txid, transaction.vin, sql_query)
      self.revert_vout(transaction.vout, sql_query)
      sql_query.push(squel.delete()
        .from('addressestransactions')
        .where('txid = ?', txid)
        .toString())
      sql_query.push(squel.delete()
        .from('assetstransactions')
        .where('txid = ?', txid)
        .toString())
      sql_query.push(squel.delete()
        .from('transactions')
        .where('txid = ?', txid)
        .toString())

      transaction.vout.forEach(function (output) {
        if (output.usedTxid && next_txids.indexOf(output.usedTxid) === -1) {
          next_txids.push(output.usedTxid)
        }
      })
      callback(null, transaction.colored, next_txids)
    })
    .catch(callback)
}

Scanner.prototype.revert_vin = function (txid, vin, sql_query) {
  if (!vin || !vin.length || vin[0].coinbase) return
  vin.forEach(function (input) {
    if (input.txid && input.vout) {
      sql_query.push(squel.update()
        .table('outputs')
        .set('used', false)
        .set('usedTxid', null)
        .set('usedBlockheight', null)
        .where('txid = ? AND n = ? AND "usedTxid" = ?', input.txid, input.vout, txid)
        .toString())
    }
    sql_query.push(squel.delete()
      .from('inputs')
      .where('input_txid = ? AND input_index = ?', txid, input.input_index)
      .toString())
  })
}

Scanner.prototype.revert_vout = function (vout, sql_query) {
  if (!vout || !vout.length) return
  vout.forEach(function (output) {
    sql_query.push(squel.delete()
      .from('addressesoutputs')
      .where('output_id = ?', output.id)
      .toString())
    sql_query.push(squel.delete()
      .from('assetsoutputs')
      .where('output_id = ?', output.id)
      .toString())
    sql_query.push(squel.delete()
      .from('outputs')
      .where('id = ?', output.id)
      .toString())
  })
}

Scanner.prototype.get_next_block_to_fix = function (limit, callback) {
  var conditions = {
    txsinserted: true, txsparsed: false
  }
  this.Blocks.findAll({
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
    return callback(null, properties.last_block + 1, properties.last_hash)
  }
  self.Blocks.findOne({
    where: {txsinserted: true},
    attributes: ['height', 'hash'],
    order: [['height', 'DESC']],
    raw: true
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

  self.to_revert = []
  raw_block_data.tx.forEach(function (txhash) {
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
    command_arr.push({method: 'getrawtransaction', params: [txhash, 1]})
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
      .toString() + ' ON CONFLICT (hash) DO UPDATE SET txsinserted = TRUE;'

    if (!properties.next_hash && raw_block_data.previousblockhash) {
      sql_query += squel.update()
        .table('blocks')
        .set('nextblockhash', raw_block_data.hash)
        .where('hash = ?', raw_block_data.previousblockhash)
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

var get_opreturn_data = function (asm) {
  return asm.substring('OP_RETURN '.length)
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
    time: raw_transaction_data.time,
    index_in_block: raw_transaction_data.index_in_block
  }

  // put this query first because of outputs and inputs foreign key constraints, validate transaction in DB
  sql_query.unshift(squel.insert()
    .into('transactions')
    .setFields(to_sql_fields(raw_transaction_data, {exclude: ['vin', 'vout', 'confirmations']}))
    .toString() + ' ON CONFLICT (txid) DO UPDATE SET ' + sql_builder.to_update_string(update))

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
      .toString() + ' ON CONFLICT (input_txid, input_index) DO NOTHING')
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
    } else if (vout.scriptPubKey && vout.scriptPubKey.type === 'nulldata') {
      var hex = get_opreturn_data(vout.scriptPubKey.asm)
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
      .toString() + ' ON CONFLICT (txid, n) DO NOTHING')

    if (vout.scriptPubKey && vout.scriptPubKey.addresses) {
      vout.scriptPubKey.addresses.forEach(function (address) {
        addresses.push(address)
        sql_query.push(squel.insert()
          .into('addressesoutputs')
          .set('address', address)
          .set('output_id',
            squel.select().field('id').from('outputs').where('txid = ? AND n = ?', new_utxo.txid, new_utxo.n))
          .toString() + ' ON CONFLICT (address, output_id) DO NOTHING')
      })
    }
  })

  addresses = _.uniq(addresses)
  addresses.forEach(function (address) {
    sql_query.push(squel.insert()
      .into('addressestransactions')
      .set('address', address)
      .set('txid', raw_transaction_data.txid)
      .toString() + ' ON CONFLICT (address, txid) DO NOTHING')
  })

  return out
}

Scanner.prototype.scan_mempol_only = function (err) {
  var self = this
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
        height: {$between: [first_block, last_block]}
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
      console.time('fix_transactions')
      console.time('fix_transactions - each')
      var bulk_outputs_ids = []
      var bulk_inputs = []
      async.each(transactions_datas, function (transaction_data, cb) {
        self.fix_vin(transaction_data, transaction_data.blockheight, bulk_outputs_ids, bulk_inputs, function (err) {
          if (err) return cb(err)
          // console.time('fix_transactions - fix bulk')
          if (!transaction_data.colored && transaction_data.iosparsed) {
            emits.push(['newtransaction', transaction_data])
          }
          cb()
        })
      },
      function (err) {
        if (err) return callback(err)
        console.timeEnd('fix_transactions - each')
        console.log('fix_transactions - outputs.length = ', bulk_outputs_ids.length)
        console.log('fix_transactions - inputs.length = ', bulk_inputs.length)
        console.log('fix_transactions - transactions.length = ', transactions_datas.length)
        var queries = get_fix_transactions_update_query(bulk_outputs_ids, bulk_inputs, transactions_datas)
        var outputs_query = queries.outputs_query
        var inputs_query = queries.inputs_query
        var transactions_query = queries.transactions_query

        console.time('fix_transactions - parallel')
        async.parallel([
          function (cb) {
            if (!bulk_outputs_ids.length) return cb()
            console.time('fix_transactions - outputs')
            self.sequelize.query(outputs_query).then(function () {
              console.timeEnd('fix_transactions - outputs')
              cb()
            }).catch(cb)
          },
          function (cb) {
            if (!bulk_inputs.length) return cb()
            console.time('fix_transactions - inputs')
            self.sequelize.query(inputs_query).then(function () {
              console.timeEnd('fix_transactions - inputs')
              cb()
            }).catch(cb)
          }
        ],
        function (err) {
          if (err) {
            console.log('fix_transactions - err = ', JSON.stringify(err))
            return callback(err)
          }
          console.time('fix_transactions - transactions')
          self.sequelize.query(transactions_query).then(function () {
            console.timeEnd('fix_transactions - transactions')
            console.timeEnd('fix_transactions - parallel')
            console.timeEnd('fix_transactions')
            close_blocks()
          }).catch(callback)
        })
      })
    })
  })
}

var get_fix_transactions_update_query = function (bulk_outputs_ids, bulk_inputs, transactions) {
  var ans = {}

  if (bulk_outputs_ids.length) {
    var outputs_conditions = 'outputs.id IN ' + sql_builder.to_values(bulk_outputs_ids)

    ans.outputs_query = '' +
      'UPDATE\n' +
      '  outputs\n' +
      'SET\n' +
      '  "usedTxid" = inputs.input_txid,\n' +
      '  "usedBlockheight" = transactions.blockheight,\n' +
      '  used = TRUE\n' +
      'FROM\n' +
      '  inputs\n' +
      'JOIN transactions ON transactions.txid = inputs.input_txid\n' + 
      'WHERE\n' +
      '  (inputs.txid = outputs.txid AND inputs.vout = outputs.n) AND (' + outputs_conditions + ')'
  }

  if (bulk_inputs.length) {
    bulk_inputs.push({txid: 'ffff', vout: -1}) // ugly hack - postgres mistakenly uses seq scan when all vout are the same
    var inputs_conditions = bulk_inputs.map(function (input) {
      return '(inputs.txid = ' + sql_builder.to_value(input.txid) + ' AND inputs.vout = ' + input.vout + ')'
    }).join(' OR ')

    ans.inputs_query = '' +
      'UPDATE\n' +
      '  inputs\n' +
      'SET\n' +
      '  output_id = outputs.id,\n' +
      '  value = outputs.value,\n' +
      '  fixed = TRUE\n' +
      'FROM\n' +
      '  outputs\n' +
      'WHERE\n' +
      '  (inputs.txid = outputs.txid AND inputs.vout = outputs.n) AND (' + inputs_conditions + ')'
  }

  if (!transactions || !transactions.length) {
    return ans
  }

  var transactions_updates = transactions.map(function (transaction) {
    var set = {}
    // Oded: TODO - fix the issue where same fields should be updated
    set.tries = transaction.tries || 0
    set.fee = transaction.fee || 0
    set.totalsent = transaction.totalsent || 0
    set.iosparsed = transaction.iosparsed || false
    return {
      set: set,
      where: {txid: transaction.txid}
    }
  })
  ans.transactions_query = sql_builder.to_multi_condition_update({table_name: 'transactions', updates: transactions_updates})

  return ans
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
        self.parse_cc_tx(transaction_data, sql_query)
        if (!transaction_data.iosparsed) {
          return cb()
        }
        did_work = true
        sql_query.push(squel.update()
          .table('transactions')
          .set('ccparsed', true)
          .set('overflow', transaction_data.overflow || false)
          .where('iosparsed AND txid = ?', transaction_data.txid)
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
    // console.warn('parse_cc_tx, problem: ', JSON.stringify(transaction_data))
    return
  }

  var assetsArrays = get_assets_outputs(transaction_data)
  assetsArrays.forEach(function (assetsArray, out_index) {
    assetsArray = assetsArray || []
    transaction_data.vout[out_index].assets = assetsArray
    assetsArray.forEach(function (asset, index_in_output) {
      var type = null
      if (transaction_data.ccdata && transaction_data.ccdata.length && transaction_data.ccdata[0].type) {
        type = transaction_data.ccdata[0].type
      }
      if (type === 'issuance') {
        sql_query.push(squel.insert()
          .into('assets')
          .set('assetId', asset.assetId)
          .set('lockStatus', asset.lockStatus)
          .set('divisibility', asset.divisibility)
          .set('aggregationPolicy', asset.aggregationPolicy)
          .toString() + ' ON CONFLICT ("assetId") DO NOTHING')
      }
      sql_query.push(squel.insert()
        .into('assetstransactions')
        .set('assetId', asset.assetId)
        .set('txid', transaction_data.txid)
        .set('type', type)
        .toString() + ' ON CONFLICT ("assetId", txid) DO NOTHING')
      sql_query.push(squel.insert()
        .into('assetsoutputs')
        .set('assetId', asset.assetId)
        .set('issueTxid', asset.issueTxid)
        .set('amount', asset.amount)
        .set('output_id', transaction_data.vout[out_index].id ||
          squel.select().field('id').from('outputs').where('txid = ? AND n = ?', transaction_data.txid, out_index))  // if no id (such as in the mempool case), go and fetch it
        .set('index_in_output', index_in_output)
        .toString() + ' ON CONFLICT ("assetId", output_id, index_in_output) DO NOTHING')
      if (asset.amount && transaction_data.vout[out_index].scriptPubKey && transaction_data.vout[out_index].scriptPubKey.addresses) {
        transaction_data.vout[out_index].scriptPubKey.addresses.forEach(function (address) {
          sql_query.push(squel.insert()
            .into('assetsaddresses')
            .set('assetId', asset.assetId)
            .set('address', address)
            .toString() + ' ON CONFLICT ("assetId", address) DO NOTHING')
        })
      }
    })
  })
}

Scanner.prototype.get_need_to_cc_parse_transactions_by_blocks = function (first_block, last_block, callback) {
  console.log('get_need_to_cc_parse_transactions_by_blocks for blocks ' + first_block + '-' + last_block)
  var query = get_find_transaction_query(this,
    'WHERE\n' +
    '  ccparsed = FALSE AND\n' +
    '  colored = TRUE AND\n' +
    '  blockheight BETWEEN ' + first_block + ' AND ' + last_block + '\n' +
    'ORDER BY\n' +
    '  blockheight ASC,\n' +
    '  index_in_block ASC\n' +
    'LIMIT 1000;')
  this.sequelize.query(query, {type: this.sequelize.QueryTypes.SELECT})
    .then(function (transactions) {
      console.log('get_need_to_cc_parse_transactions_by_blocks - transactions.length = ', transactions.length)
      callback(null, transactions)
    })
    .catch(function (e) {
      console.log('get_need_to_cc_parse_transactions_by_blocks - e = ', e)
      callback(e)
    })
}

Scanner.prototype.get_need_to_fix_transactions_by_blocks = function (first_block, last_block, callback) {
  var conditions = {
    iosparsed: false,
    blockheight: {$between: [first_block, last_block]}
  }
  console.time('get_need_to_fix_transactions_by_blocks')
  var query = get_find_transaction_query(this,
    'WHERE\n' +
    '  transactions.iosparsed = FALSE AND\n' +
    '  transactions.blockheight BETWEEN ' + first_block + ' AND ' + last_block + '\n' +
    'ORDER BY\n' +
    '  transactions.blockheight ASC,\n' +
    '  transactions.index_in_block ASC\n' +
    'LIMIT 200;')
  this.sequelize.query(query, {type: this.sequelize.QueryTypes.SELECT})
    .then(function (transactions) {
      console.timeEnd('get_need_to_fix_transactions_by_blocks')
      console.log('get_need_to_fix_transactions_by_blocks #1 - transactions.length = ', transactions.length)
      callback(null, transactions)
    })
    .catch(function (e) {
      console.log('get_need_to_fix_transactions_by_blocks - e = ', e)
      callback(e)
    })
}

Scanner.prototype.fix_vin = function (raw_transaction_data, blockheight, bulk_outputs_ids, bulk_inputs, callback) {
  // fixing a transaction - we need to fulfill the condition where a transaction is iosparsed if and only if
  // all of its inputs are fixed.
  // An input is fixed when it is assigned with the output_id of its associated output, and the output is marked as used.
  // If the transaction is colored - this should happen only after this output's transaction is both iosparsed AND ccparsed.
  // Otherwise, it is enough for it to be in DB.

  var self = this
  var inputs_to_fix = {}
  var outputs_conditions
  var transactions_conditions
  var find_vin_transactions_query

  if (!raw_transaction_data.vin) {
    return callback('transaction ' + raw_transaction_data.txid + ' does not have vin.')
  }

  var end = function (in_transactions) {
    var inputs_to_fix_now = []
    in_transactions.forEach(function (in_transaction) {
      in_transaction.vout.forEach(function (output) {
        var input
        if (in_transaction.txid + ':' + output.n in inputs_to_fix) {
          input = inputs_to_fix[in_transaction.txid + ':' + output.n]
          input.previousOutput = output.scriptPubKey
          input.value = output.value
          input.output_id = output.id
          input.assets = output.assets
          inputs_to_fix_now.push(input)
          bulk_inputs.push({txid: input.txid, vout: input.vout})
          bulk_outputs_ids.push(input.output_id)
        }
      })
    })

    var all_fixed = (inputs_to_fix_now.length === Object.keys(inputs_to_fix).length)
    if (all_fixed) {
      calc_fee(raw_transaction_data)
      if (raw_transaction_data.fee < 0) {
        console.log('calc_fee < 0 !!!')
        console.log('calc_fee: ' + raw_transaction_data.txid + ', raw_transaction_data.fee = ', raw_transaction_data.fee)
        console.log('calc_fee: ' + raw_transaction_data.txid + ', raw_transaction_data.totalsent = ', raw_transaction_data.totalsent)
        console.log('calc_fee, ' + raw_transaction_data.txid + ', raw_transaction_data = ', raw_transaction_data)
      }
    } else {
      raw_transaction_data.tries = raw_transaction_data.tries || 0
      raw_transaction_data.tries++
      if (raw_transaction_data.tries > 1000) {
        console.warn('transaction', raw_transaction_data.txid, 'has un parsed inputs (', Object.keys(inputs_to_fix), ') for over than 1000 tries.')
      }
    }
    raw_transaction_data.iosparsed = all_fixed
    callback()
  }

  var is_input_fixed = function (input) {
    return input.coinbase || input.output_id
  }

  raw_transaction_data.vin.forEach(function (vin) {
    if (!is_input_fixed(vin)) {
      inputs_to_fix[vin.txid + ':' + vin.vout] = vin
    }
  })

  if (!Object.keys(inputs_to_fix).length) {
    return end([])
  }

  inputs_to_fix['ffff:-1'] = true   // hack to avoid seq scan
  outputs_conditions = '(' + Object.keys(inputs_to_fix).map(function (txid_index) {
    txid_index = txid_index.split(':')
    var txid = txid_index[0]
    var n = txid_index[1]
    return '(outputs.txid = ' + sql_builder.to_value(txid) + ' AND outputs.n = ' + n + ')'
  }).join(' OR ') + ')'
  delete inputs_to_fix['ffff:-1']

  if (raw_transaction_data.colored) {
    find_vin_transactions_query = '' +
      'SELECT\n' +
      '  outputs.*,\n' +
      '  to_json(array(\n' +
      '    SELECT assets FROM\n' +
      '      (SELECT\n' +
      '        assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
      '        assets.*\n' +
      '      FROM\n' +
      '        assetsoutputs\n' +
      '      INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
      '      WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)\n' +
      '    AS assets)) AS assets\n' +
      'FROM outputs\n' +
      'JOIN transactions ON transactions.txid = outputs.txid\n' +
      'WHERE ((transactions.colored = FALSE) OR (transactions.colored = TRUE AND transactions.iosparsed = TRUE AND transactions.ccparsed = TRUE)) AND ' + outputs_conditions + ';'
  } else {
    find_vin_transactions_query = '' +
      'SELECT\n' +
      '  outputs.*\n' +
      'FROM outputs\n' +
      'WHERE ' + outputs_conditions + ';'
  }
  // console.time('find_vin_transactions_query ' + raw_transaction_data.txid)
  self.sequelize.query(find_vin_transactions_query, {type: self.sequelize.QueryTypes.SELECT})
    .then(function (vin_transactions) {
      // console.timeEnd('find_vin_transactions_query ' + raw_transaction_data.txid)
      vin_transactions = _(vin_transactions)
        .groupBy('txid')
        .transform(function (result, vout, txid) {
          result.push({txid: txid, vout: vout})
        }, [])
        .value()
      end(vin_transactions)
    })
    .catch(callback)
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
    'time',
    'size',
    'totalsent',
    'fees',
    'txlength',
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
  var self = this
  var transaction_data
  var did_work = false
  var blockheight = -1
  async.waterfall([
    function (cb) {
      // console.time('parse_new_mempool_transaction - #1 lookup in DB, txid = ' + raw_transaction_data.txid)
      var find_transaction_query = get_find_transaction_query(self, 'WHERE txid = :txid ;')
      self.sequelize.query(find_transaction_query, {replacements: {txid: raw_transaction_data.txid}, type: self.sequelize.QueryTypes.SELECT})
        .then(function (transactions) { cb(null, transactions[0]) })
        .catch(cb)
    },
    function (l_transaction_data, cb) {
      // console.timeEnd('parse_new_mempool_transaction - #1 lookup in DB, txid = ' + raw_transaction_data.txid)
      // console.time('parse_new_mempool_transaction - #2 get_block_height, txid = ' + raw_transaction_data.txid)
      transaction_data = l_transaction_data
      if (transaction_data) {
        raw_transaction_data = transaction_data
        // blockheight = raw_transaction_data.blockheight || -1
        cb(null, blockheight)
      } else {
        // console.log('parse_new_mempool_transaction: did not find in DB, parsing new tx: ' + raw_transaction_data.txid)
        did_work = true
        if (blockheight === -1 && raw_transaction_data.blockhash) {
          get_block_height(raw_transaction_data.blockhash, cb)
        } else {
          cb(null, blockheight)
        }
      }
    },
    function (l_blockheight, cb) {
      blockheight = l_blockheight
      // console.timeEnd('parse_new_mempool_transaction - #2 get_block_height, txid = ' + raw_transaction_data.txid)
      // console.log('parse_new_mempool_transaction - #2 blockheight = ', blockheight)
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
        // console.log('parse_new_mempool_transaction - #3.1 transaction.iosparsed = true, txid = ' + raw_transaction_data.txid)
        cb()
      } else {
        did_work = true
        // console.log('parse_new_mempool_transaction - #3.2 fix_vin')
        // console.time('parse_new_mempool_transaction - fix_vin ' + raw_transaction_data.txid)
        var bulk_outputs_ids = []
        var bulk_inputs = []
        self.fix_vin(raw_transaction_data, blockheight, bulk_outputs_ids, bulk_inputs, function (err) {
          if (err) return cb(err)
          // console.timeEnd('parse_new_mempool_transaction - fix_vin ' + raw_transaction_data.txid)
          var queries = get_fix_transactions_update_query(bulk_outputs_ids, bulk_inputs)
          sql_query.push(queries.outputs_query)
          sql_query.push(queries.inputs_query)
          cb()
        })
      }
    },
    function (cb) {
      if (raw_transaction_data.ccparsed) {
        // console.log('parse_new_mempool_transaction - #4.1 raw_transaction_data.ccparsed = true, txid = ', raw_transaction_data.txid)
        cb(null, null)
      } else {
        if (raw_transaction_data.iosparsed && raw_transaction_data.colored && !raw_transaction_data.ccparsed) {
          // console.log('parse_new_mempool_transaction - #4.2 parse_cc_tx, txid = ', raw_transaction_data.txid)
          self.parse_cc_tx(raw_transaction_data, sql_query)
          raw_transaction_data.ccparsed = true
          did_work = true
        }
        if (did_work && raw_transaction_data.iosparsed) {
          emits.push(['newtransaction', raw_transaction_data])
          if (raw_transaction_data.colored) {
            emits.push(['newcctransaction', raw_transaction_data])
          }
        }
        if (did_work) {
          var update = {
            iosparsed: raw_transaction_data.iosparsed,
            ccparsed: raw_transaction_data.ccparsed,
            tries: raw_transaction_data.tries || 0
          }
          if (raw_transaction_data.fee) update.fee = raw_transaction_data.fee
          if (raw_transaction_data.totalsent) update.totalsent = raw_transaction_data.totalsent
          // put this query first because of outputs and inputs foreign key constraints, validate transaction in DB
          sql_query.unshift(squel.insert()
            .into('transactions')
            .setFields(to_sql_fields(raw_transaction_data, {exclude: ['vin', 'vout', 'confirmations', 'index_in_block']}))
            .toString() + ' ON CONFLICT (txid) DO UPDATE SET ' + sql_builder.to_update_string(update))
        }
        cb()
      }
    }
  ],
  function (err) {
    if (err) return callback(err)
    callback(null, did_work, raw_transaction_data.iosparsed, raw_transaction_data.colored, raw_transaction_data.ccparsed)
  })
}

Scanner.prototype.parse_mempool_cargo = function (txids, callback) {
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
    command_arr.push({ method: 'getrawtransaction', params: [txhash, 1] })
  })

  bitcoin_rpc.cmd(command_arr, function (raw_transaction_data, cb) {
    // console.log('received result from bitcoind, raw_transaction_data.txid = ', raw_transaction_data.txid)
    var sql_query = []
    if (!raw_transaction_data) {
      console.log('Null transaction')
      return cb()
    }
    raw_transaction_data = to_discrete(raw_transaction_data)
    // console.time('parse_new_mempool_transaction time - ' + raw_transaction_data.txid)
    self.parse_new_mempool_transaction(raw_transaction_data, sql_query, emits, function (err, did_work, iosparsed, colored, ccparsed) {
      if (err) return cb(err)
      // work may have been done in priority_parse in context of API
      new_mempool_txs.push({
        txid: raw_transaction_data.txid,
        iosparsed: iosparsed,
        colored: colored,
        ccparsed: ccparsed
      })
      if (!did_work) {
        return cb()
      }
      if (!sql_query.length) return cb()
      sql_query = sql_query.join(';\n')
      self.sequelize.transaction(function (sql_transaction) {
        return self.sequelize.query(sql_query, {transaction: sql_transaction})
          .then(function () {
            // console.timeEnd('parse_new_mempool_transaction time - ' + raw_transaction_data.txid)
            cb()
          })
          .catch(cb)
      })
    })
  },
  function (err) {
    if (err) {
      if ('code' in err && err.code === -5) {
        console.error('Can\'t find tx.')
      } else {
        console.error('parse_mempool_cargo: ', err)
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

Scanner.prototype.revert_txids = function (callback) {
  var self = this

  self.to_revert = _.uniq(self.to_revert)
  if (!self.to_revert.length) return callback()
  console.log('need to revert ' + self.to_revert.length + ' txs from mempool.')
  var n_batch = 100
  // async.whilst(function () { return self.to_revert.length },
    // function (cb) {
      var txids = self.to_revert.slice(0, n_batch)
      console.log('reverting txs (' + txids.length + ',' + self.to_revert.length + ')')

      // logger.debug('reverting '+block_data.tx.length+' txs.')
      var regular_txids = []
      var colored_txids = []

      async.map(txids, function (txid, cb) {
        bitcoin_rpc.cmd('getrawtransaction', [txid], function (err, raw_transaction_data) {
          if (err || !raw_transaction_data || !raw_transaction_data.confirmations) {
            regular_txids.push(txid)
            var sql_query = []
            self.revert_tx(txid, sql_query, function (err, colored, revert_flags_txids) {
              if (err) return cb(err)
              if (colored) {
                colored_txids.push(txid)
              }
              sql_query = sql_query.join(';\n')
              return self.sequelize.query(sql_query)
                .then(function () { cb(null, revert_flags_txids) })
                .catch(cb)
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
            cb(null, [])
          }
        })
      },
      function (err, revert_flags_txids) {
        if (err) return callback(err)
        revert_flags_txids = [].concat.apply([], revert_flags_txids)
        revert_flags_txids = _(revert_flags_txids).uniq().filter(function (txid) { return txid }).value()
        console.log('revert flags txids:', revert_flags_txids)
        if (!revert_flags_txids.length) return callback()

        var sql_query = [
          squel.update()
          .table('transactions')
          .set('iosparsed', false)
          .set('ccparsed', false)
          .where('txid IN ?', revert_flags_txids)
          .toString(),
          squel.update()
          .table('inputs')
          .set('output_id', null)
          .where('input_txid IN ?', revert_flags_txids)
          .toString()
        ]
        sql_query = sql_query.join(';\n')
        self.sequelize.transaction(function (sql_transaction) {
          return self.sequelize.query(sql_query, {transaction: sql_transaction})
          .then(function () {
            regular_txids.forEach(function (txid) {
              self.emit('revertedtransaction', {txid: txid})
            })
            colored_txids.forEach(function (txid) {
              self.emit('revertedcctransaction', {txid: txid})
            })
            self.to_revert = []
            callback()
          })
          .catch(callback)
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

  console.log('parse_new_mempool')
  if (properties.scanner.mempool !== 'true') return callback()
  console.log('start reverting (if needed)')
  async.waterfall([
    function (cb) {
      self.revert_txids(cb)
    },
    function (cb) {
      console.log('end reverting (if needed)')
      if (!self.mempool_txs) {
        self.emit('mempool')
        var conditions = {
          blockheight: -1
        }
        var attributes = ['txid', 'iosparsed', 'colored', 'ccparsed']
        var limit = 10000
        var has_next = true
        var offset = 0
        self.mempool_txs = []
        async.whilst(function () { return has_next },
          function (cb) {
            console.time('find mempool db txs')
            self.Transactions.findAll({
              where: conditions,
              attributes: attributes,
              limit: limit,
              offset: offset,
              raw: true
            }).then(function (transactions) {
              console.timeEnd('find mempool db txs')
              console.time('processing mempool db txs')
              self.mempool_txs = self.mempool_txs.concat(transactions)
              transactions.forEach(function (transaction) {
                if (transaction.iosparsed && transaction.colored === transaction.ccparsed) {
                  db_parsed_txids.push(transaction.txid)
                } else {
                  db_unparsed_txids.push(transaction.txid)
                }
              })
              console.timeEnd('processing mempool db txs')
              if (transactions.length === limit) {
                console.log('getting txs', offset + 1, '-', offset + limit)
                offset += limit
              } else {
                has_next = false
              }
              cb()
            }).catch(cb)
          },
        cb)
      } else {
        console.log('getting mempool from memory cache')
        self.mempool_txs.forEach(function (transaction) {
          if (transaction.iosparsed && transaction.colored === transaction.ccparsed) {
            db_parsed_txids.push(transaction.txid)
          } else {
            db_unparsed_txids.push(transaction.txid)
          }
        })
        cb()
      }
    },
    function (cb) {
      console.log('start find mempool bitcoind txs')
      bitcoin_rpc.cmd('getrawmempool', [], cb)
    },
    function (whole_txids, cb) {
      whole_txids = whole_txids || []
      console.log('end find mempool bitcoind txs')
      console.log('parsing mempool txs (' + whole_txids.length + ')')
      console.log('start xoring')
      // console.log('db_parsed_txids = ', db_parsed_txids.map(function (txid) { return txid }))
      var txids_intersection = _.intersection(db_parsed_txids, whole_txids) // txids that allready parsed in db
      // console.log('txids_intersection = ', txids_intersection.map(function (txid) { return txid }))
      new_txids = _.xor(txids_intersection, whole_txids) // txids that not parsed in db
      // console.log('new_txids = ', new_txids.map(function (txid) { return txid }))
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
          db_txids.forEach(self.remove_from_mempool_cache)
          end_func()
        }
      })
    }
  ], callback)
}

Scanner.prototype.remove_from_mempool_cache = function (txid) {
  var self = this
  if (self.mempool_txs) {
    var mempool_tx_index = -1
    self.mempool_txs.forEach(function (mempool_tx, i) {
      if (!~mempool_tx_index && mempool_tx.txid === txid) {
        mempool_tx_index = i
      }
    })
    if (~mempool_tx_index) {
      self.mempool_txs.splice(mempool_tx_index, 1)
    }
  }
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
  console.log('start priority_parse: ' + txid)
  console.time('priority_parse time: ' + txid)
  var end = function (err) {
    if (~self.priority_parse_list.indexOf(txid)) {
      self.priority_parse_list.splice(self.priority_parse_list.indexOf(txid), 1)
    }
    console.timeEnd('priority_parse time: ' + txid)
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
      console.time('priority_parse: find in db ' + txid)
      self.priority_parse_list.push(txid)
      var conditions = {
        txid: txid,
        iosparsed: true,
        ccparsed: {$col: 'colored'}
      }
      var attributes = ['txid']
      self.Transactions.findOne({ where: conditions, attributes: attributes, raw: true })
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
      console.timeEnd('priority_parse: get_from_bitcoind ' + txid)
      console.time('priority_parse: parse inputs ' + txid)
      transaction = raw_transaction_data
      transaction = to_discrete(transaction)
      if (!transaction || !transaction.vin) return cb(['tx ' + txid + ' not found.', 204])
      async.each(transaction.vin, function (vin, cb2) {
        self.priority_parse(vin.txid, cb2)
      },
      cb)
    },
    function (cb) {
      console.timeEnd('priority_parse: parse inputs ' + txid)
      console.time('priority_parse: parse ' + txid)
      self.mempool_cargo.unshift(txid, cb)
    }
  ],
  function (err) {
    if (err) {
      if (err === PARSED) {
        return end()
      }
      return end(err)
    }
    console.timeEnd('priority_parse: parse ' + txid)
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

var get_find_transaction_query = function (self, query_tail) {
  return '' +
    'SELECT\n' +
    '  ' + sql_builder.to_columns_of_model(self.Transactions, {exclude: ['hex']}) + ',\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vin\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '       inputs.*,\n' +
    '       "previousOutput"."scriptPubKey" AS "previousOutput",\n' +
    '       to_json(array(\n' +
    '          SELECT\n' +
    '            assets\n' +
    '          FROM\n' +
    '            (SELECT\n' +
    '              assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
    '              assets.*\n' +
    '            FROM\n' +
    '              assetsoutputs\n' +
    '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '            WHERE assetsoutputs.output_id = inputs.output_id ORDER BY index_in_output)\n' +
    '        AS assets)) AS assets\n' +
    '      FROM\n' +
    '        inputs\n' +
    '      LEFT OUTER JOIN\n' +
    '        (SELECT outputs.id, outputs."scriptPubKey"\n' +
    '         FROM outputs) AS "previousOutput" ON "previousOutput".id = inputs.output_id\n' +
    '      WHERE\n' +
    '        inputs.input_txid = transactions.txid\n' +
    '      ORDER BY input_index) AS vin)) AS vin,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vout\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        outputs.*,\n' +
    '        to_json(array(\n' +
    '         SELECT assets FROM\n' +
    '           (SELECT\n' +
    '              assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
    '              assets.*\n' +
    '            FROM\n' +
    '              assetsoutputs\n' +
    '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '            WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)\n' +
    '        AS assets)) AS assets\n' +
    '      FROM\n' +
    '        outputs\n' +
    '      WHERE outputs.txid = transactions.txid\n' +
    '      ORDER BY n) AS vout)) AS vout\n' +
    'FROM\n' +
    '  transactions\n' + query_tail
}

/**
 * @param {object} key-value pairs for a table insert
 * @param {object} [options={}]
 * @param {object} [options.exclude] array of keys to exclude from the given object
 * @return {object} key-value pairs for a table insert
*/
var to_sql_fields = function (obj, options) {
  var ans = {}
  Object.keys(obj).forEach(function (key) {
    if (!options || !options.exclude || options.exclude.indexOf(key) === -1) {
      if (typeof obj[key] === 'object') {
        ans[key] = JSON.stringify(obj[key])
        if (ans[key] === 'null') delete ans[key]
      } else {
        ans[key] = obj[key]
      }
    }
  })
  return ans
}

module.exports = Scanner
