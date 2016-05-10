'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Outputs = sequelize.define('outputs', {
    id: {
      type: DataTypes.BIGINT,
      primaryKey: true,
      autoIncrement: true
    },
    txid: {
      type: bitcoinDataTypes.hashType,
      unique: 'output_id'
    },
    n: {
      type: DataTypes.INTEGER,
      unique: 'output_id'
    },
    used: {
      type: DataTypes.BOOLEAN,
      defaultValue: false
    },
    usedTxid: {
      type: bitcoinDataTypes.hashType
    },
    usedBlockheight: {
      type: DataTypes.INTEGER
    },
    value: {
      type: DataTypes.BIGINT
    },
    scriptPubKey: {
      type: DataTypes.JSONB  // to contain hex, asm, type [, reqSigs, addresses]
    }
  },
  {
    classMethods: {
      associate: function (models) {
        Outputs.belongsTo(models.transactions, { foreignKey: 'txid', as: 'transaction' })
        Outputs.hasOne(models.inputs, { foreignKey: 'output_id', constraints: false }) // constraints = false because an output is not necessarily used
        Outputs.belongsToMany(models.assets, { as: 'assets', through: models.assetsoutputs, foreignKey: 'output_id', otherKey: 'assetId' })
        Outputs.hasMany(models.addressesoutputs, { foreignKey: 'output_id', constraints: false }) // constraints = false because addressoutput is inserted after output
      }
    },
    indexes: [
      {
        fields: ['txid']
      },
      {
        fields: ['n']
      },
      {
        fields: ['txid', 'n', 'used']
      },
      {
        fields: ['used']
      },
      {
        fields: ['usedBlockheight']
      }
      // {
      //   fields: ['scriptPubKey.type']
      // }
    ],
    timestamps: false
  })

  return Outputs
}
