'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Transactions = sequelize.define('transactions', {
    blockheight: {
      type: DataTypes.BIGINT
    },
    blockhash: {
      type: bitcoinDataTypes.hashType
    },
    blocktime: {
      type: DataTypes.BIGINT
    },
    index_in_block: {
      type: DataTypes.INTEGER
    },
    txid: {
      type: bitcoinDataTypes.hashType,
      primaryKey: true
    },
    hex: {
      type: DataTypes.TEXT
    },
    version: {
      type: DataTypes.INTEGER
    },
    locktime: {
      type: DataTypes.BIGINT
    },
    time: {
      type: DataTypes.BIGINT
    },
    fee: {
      type: DataTypes.BIGINT
    },
    totalsent: {
      type: DataTypes.BIGINT
    },
    overflow: {
      type: DataTypes.BOOLEAN
    },
    ccdata: {
      type: DataTypes.JSONB
    },
    colored: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    iosparsed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    ccparsed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    tries: {
      type: DataTypes.INTEGER,
      defaultValue: 0
    }
  },
  {
    validate: {
      blockProperties: function () {
        if ((this.blockheight > -1) !== (this.index_in_block !== null)) {
          throw new Error('Require index_in_block when in block')
        }
      }
    },
    classMethods: {
      associate: function (models) {
        Transactions.hasMany(models.outputs, { foreignKey: 'txid', as: 'vout'})
        Transactions.hasMany(models.inputs, { foreignKey: 'input_txid', as: 'vin' }) // inputs in this transaction
      }
    },
    getterMethods: {
      confirmations: function () {
        var properties = Transactions.properties
        return (properties && properties.last_block && this.blockheight > -1) ? (properties.last_block - this.blockheight + 1) : 0
      }
    },
    indexes: [
      {
        fields: ['blockheight']
      },
      {
        fields: ['blockhash']
      },
      {
        fields: ['blocktime']
      },
      {
        fields: ['iosparsed']
      },
      {
        fields: ['overflow']
      },
      {
        fields: ['colored']
      },
      {
        fields: ['ccparsed']
      },
      {
        fields: ['blockheight', 'iosparsed']
      },
      {
        fields: ['blockheight', 'colored']
      },
      {
        fields: ['blockheight', 'colored', 'ccparsed']
      }
    ],
    timestamps: false
  })

  // Transactions.beforeFind(function (options) {
  //   console.log('beforeFind!')
  // })

  return Transactions
}

// module.exports = function (mongoose, properties) {
//   var vin = new mongoose.Schema({
//     sequence: Number,
//     coinbase: {type: String, index: true},
//     txid: {type: String, index: true},
//     vout: {type: Number, index: true},
//     scriptSig: {
//       asm: String,
//       hex: String
//     },
//     previousOutput: {
//       asm: String,
//       hex: String,
//       reqSigs: Number,
//       type: {type: String, index: true},
//       addresses: {type: [String]}
//     },
//     assets: [{
//       assetId: {type: String, index: true},
//       amount: { type: Number, set: function (v) { return Math.round(v) }},
//       issueTxid: String,
//       divisibility: Number,
//       lockStatus: Boolean
//     }],
//     value: { type: Number, set: function (v) { return Math.round(v) }, index: true},
//     fixed: {type: Boolean, index: true, default: false}
//   }, {_id: false })

//   var vout = new mongoose.Schema({
//     value: { type: Number, set: function (v) { return Math.round(v) } },
//     n: Number,
//     scriptPubKey: {
//       asm: String,
//       hex: String,
//       reqSigs: Number,
//       type: {type: String, index: true},
//       addresses: {type: [String]}
//     },
//     assets: [{
//       assetId: {type: String, index: true},
//       amount: { type: Number, set: function (v) { return Math.round(v) }},
//       issueTxid: String,
//       divisibility: Number,
//       lockStatus: Boolean
//     }]
//   }, {_id: false })

//   var payment = new mongoose.Schema({
//     input: Number,
//     output: Number,
//     amount: Number,
//     range: Boolean,
//     percent: Boolean
//   }, {_id: false })

//   var ccdata = new mongoose.Schema({
//     type: String,
//     noRules: Boolean,
//     payments: [payment],
//     protocol: Number,
//     version: Number,
//     divisibility: Number,
//     lockStatus: Boolean,
//     amount: Number,
//     sha2: String,
//     torrentHash: String,
//     multiSig: [{
//       index: {type: Number},
//       hashType: String
//     }]
//   }, {_id: false })

//   // Define our token schema
//   var RawTransactionsSchema = new mongoose.Schema({
//     txid: { type: String, index: { unique: true } },
//     hex: String,
//     version: Number,
//     loctime: Number,
//     fee: { type: Number, set: function (v) { return Math.round(v) } },
//     totalsent: { type: Number, set: function (v) { return Math.round(v) } },
//     blockhash: {type: String, index: true},
//     time: {type: Number, index: true},
//     blocktime: {type: Number, index: true},
//     blockheight: {type: Number, index: true},
//     confirmations: {type: Number, index: true},
//     vin: [vin],
//     vout: [vout],
//     ccdata: [ccdata],
//     iosparsed: {type: Boolean, index: true, default: false},
//     tries: {type: Number, index: true, default: 0},
//     colored: {type: Boolean, index: true, default: false},
//     ccparsed: {type: Boolean, index: true, default: false},
//     overflow: {type: Boolean, index: true, default: false}
//   })

//   RawTransactionsSchema.index({
//     'blockheight': 1,
//     'iosparsed': 1
//   })

//   var round = function (doc) {
//     if (doc.fee) doc.fee = Math.round(doc.fee)
//     if (doc.totalsent) doc.totalsent = Math.round(doc.totalsent)
//     if (doc.vin) {
//       doc.vin.forEach(function (vin) {
//         if (vin.value) vin.value = Math.round(vin.value)
//         if (vin.assets) {
//           vin.assets.forEach(function (asset) {
//             if (asset.amount) asset.amount = Math.round(asset.amount)
//           })
//         }
//       })
//     }
//     if (doc.vout) {
//       doc.vout.forEach(function (vout) {
//         if (vout.value) vout.value = Math.round(vout.value)
//         if (vout.assets) {
//           vout.assets.forEach(function (asset) {
//             if (asset.amount) asset.amount = Math.round(asset.amount)
//           })
//         }
//       })
//     }
//   }

//   RawTransactionsSchema.post('find', function (docs) {
//     if (docs && properties) {
//       docs.forEach(function (doc) {
//         if (properties.last_block && doc.blockheight > -1) {
//           doc.confirmations = properties.last_block - doc.blockheight + 1
//         } else {
//           doc.confirmations = 0
//         }
//         calc_fee(doc)
//         round(doc)
//       })
//     }
//   })

//   RawTransactionsSchema.post('findOne', function (doc) {
//     if (doc && properties) {
//       if (properties.last_block && doc.blockheight > -1) {
//         doc.confirmations = properties.last_block - doc.blockheight + 1
//       } else {
//         doc.confirmations = 0
//       }
//       calc_fee(doc)
//       round(doc)
//     }
//   })

//   var calc_fee = function (doc) {
//     if (doc.totalsent) return
//     var fee = 0
//     var totalsent = 0
//     var coinbase = false
//     if ('vin' in doc && doc.vin) {
//       doc.vin.forEach(function (vin) {
//         if ('coinbase' in vin && vin.coinbase) {
//           coinbase = true
//         }
//         if (vin.value) {
//           fee += vin.value
//         }
//       })
//     }
//     if (doc.vout) {
//       doc.vout.forEach(function (vout) {
//         if ('value' in vout && vout.value) {
//           fee -= vout.value
//           totalsent += vout.value
//         }
//       })
//     }
//     doc.totalsent = totalsent
//     doc.fee = coinbase ? 0 : fee
//   }

//   return RawTransactionsSchema
// }
