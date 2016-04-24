'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AssetsTransactions = sequelize.define('assetstransactions', {
    assetId: {
      type: bitcoinDataTypes.assetIdType,
      primaryKey: true
    },
    txid: {
      type: bitcoinDataTypes.hashType,
      primaryKey: true
    },
    type: {
      type: DataTypes.ENUM('issuance', 'transfer')
    }
  },
  {
    classMethods: {
      associate: function (models) {
        AssetsTransactions.belongsTo(models.assets, { foreignKey: 'assetId', as: 'asset' })
        AssetsTransactions.belongsTo(models.transactions, { foreignKey: 'txid', as: 'transaction' })
      }
    },
    indexes: [
      {
        fields: ['assetId']
      },
      {
        fields: ['txid']
      },
      {
        fields: ['type']
      },
      {
        fields: ['assetId', 'type']
      }
    ],
    timestamps: false
  })

  return AssetsTransactions
}