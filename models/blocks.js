'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Blocks = sequelize.define('blocks', {
    height: {
      type: DataTypes.INTEGER,
      primaryKey: true
    },
    hash: {
      type: ColoredCoinsDataTypes.HASH,
      unique: true
    },
    previousblockhash: {
      type: ColoredCoinsDataTypes.HASH
    },
    nextblockhash: {
      type: ColoredCoinsDataTypes.HASH
    },
    size: {
      type: DataTypes.INTEGER
    },
    version: {
      type: DataTypes.INTEGER
    },
    merkleroot: {
      type: DataTypes.STRING
    },
    time: {
      type: DataTypes.DOUBLE
    },
    mediantime: {
      type: DataTypes.BIGINT
    },
    nonce: {
      type: DataTypes.BIGINT
    },
    bits: {
      type: DataTypes.STRING
    },
    difficulty: {
      type: DataTypes.DOUBLE
    },
    chainwork: {
      type: DataTypes.STRING
    },
    reward: {
      type: DataTypes.BIGINT
    },
    totalsent: {
      type: DataTypes.BIGINT
    },
    fees: {
      type: DataTypes.BIGINT
    },
    txlength: {
      type: 'SMALLINT'
    },
    txsinserted: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    txsparsed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    ccparsed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    }
  },
  {
    classMethods: {
  		associate: function (models) {
  		  Blocks.hasMany(models.transactions, { foreignKey: 'blockheight', as: 'transactions', constraints: false }) // constraints=false, because block is inserted to DB after a transaction (also, mempol case)
  		}
  	},
    getterMethods: {
      confirmations: function () {
        var properties = Blocks.properties
        return (properties && properties.last_block && this.height > -1) ? (properties.last_block - this.height + 1) : 0
      }
    },
    indexes: [
      {
        fields: ['txsinserted']
      },
      {
        fields: ['txsparsed']
      },
      {
        fields: ['ccparsed']
      },
      {
        fields: [{attribute: 'height', order: 'DESC'}, 'txsinserted']
      },
      {
        fields: ['height', 'txsinserted', 'txsparsed']
      },
      {
        fields: ['height', 'ccparsed']
      }
    ],
    timestamps: false
  })

  return Blocks
}

// module.exports = function (mongoose, properties) {
//   var BlocksSchema = new mongoose.Schema({
//     hash: { type: String, index: { unique: true } },
//     previousblockhash: String,
//     nextblockhash: String,
//     height: { type: Number, index: true },
//     size: Number,
//     version: Number,
//     merkleroot: String,
//     time: Date,
//     nonce: Number,
//     bits: String,
//     difficulty: Number,
//     chainwork: String,
//     txsparsed: { type: Boolean, index: true, default: false},
//     txinserted: { type: Boolean, index: true, default: false},
//     ccparsed: { type: Boolean, index: true, default: false},
//     confirmations: Number,
//     reward: { type: Number, set: function (v) { return Math.round(v)} },
//     totalsent: { type: Number, set: function (v) { return Math.round(v)} },
//     fees: { type: Number, set: function (v) { return Math.round(v)} },
//     tx: { type: [String], default: [] },
//     txlength: Number
//   })

//   BlocksSchema.index({
//     txinserted: 1, txsparsed: 1, height: 1
//   })

//   var round = function (doc) {
//     if (doc.reward) doc.reward = Math.round(doc.reward)
//     if (doc.totalsent) doc.totalsent = Math.round(doc.totalsent)
//     if (doc.fees) doc.fees = Math.round(doc.fees)
//   }

//   BlocksSchema.post('find', function (docs) {
//     if (docs) {
//       docs.forEach(function (doc) {
//         if (properties && properties.last_block) {
//           doc.confirmations = properties.last_block - doc.height + 1
//         }
//         round(doc)
//       })
//     }
//   })

//   BlocksSchema.post('findOne', function (doc) {
//     if (doc && properties && properties.last_block) {
//       doc.confirmations = properties.last_block - doc.height + 1
//       return doc
//     }
//     if (doc) {
//       round(doc)  
//     }
//   })

//   return BlocksSchema
// }
