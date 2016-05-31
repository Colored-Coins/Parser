'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Inputs = sequelize.define('inputs', {
    input_txid: {
      type: ColoredCoinsDataTypes.HASH,
      primaryKey: true
    },
    input_index: {
      type: DataTypes.INTEGER,
      primaryKey: true
    },
    txid: {
      type: ColoredCoinsDataTypes.HASH // might be null (coinbase) ; might be not unique (mempool)
    },
    vout: {
      type: DataTypes.INTEGER // might be null (coinbase) ; might be not unique (mempool)
    },
    output_id: {
      type: DataTypes.BIGINT
    },
    scriptSig: {
      type: DataTypes.JSONB  // to contain hex, asm
    },
    coinbase: {
      type: DataTypes.STRING
    },
    fixed: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    value: {
      type: DataTypes.BIGINT
    },
    sequence: {
      type: DataTypes.BIGINT
    }
  },
  {
    classMethods: {
      associate: function (models) {
        Inputs.belongsTo(models.transactions, { foreignKey: 'input_txid' })
        Inputs.belongsTo(models.outputs, { foreignKey: 'output_id', as: 'previousOutput', constraints: false })  // constraints=false because an input may be inserted to mempool before its corresponding output (orphand)
      }
    },
    indexes: [
      {
        fields: ['input_txid']
      },
      {
        fields: ['txid']
      },
      {
        fields: ['output_id']
      }
    ],
    timestamps: false
  })

  return Inputs
}

// var vin = new mongoose.Schema({
//   sequence: Number,
//   coinbase: {type: String, index: true},
//   txid: {type: String, index: true},
//   vout: {type: Number, index: true},
//   scriptSig: {
//     asm: String,
//     hex: String
//   },
//   previousOutput: {
//     asm: String,
//     hex: String,
//     reqSigs: Number,
//     type: {type: String, index: true},
//     addresses: {type: [String]}
//   },
//   assets: [{
//     assetId: {type: String, index: true},
//     amount: { type: Number, set: function (v) { return Math.round(v) }},
//     issueTxid: String,
//     divisibility: Number,
//     lockStatus: Boolean
//   }],
//   value: { type: Number, set: function (v) { return Math.round(v) }, index: true},
//   fixed: {type: Boolean, index: true, default: false}
// }, {_id: false })