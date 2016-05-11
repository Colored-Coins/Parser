'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AssetsTransactions = sequelize.define('assetstransactions', {
    assetId: {
      type: ColoredCoinsDataTypes.ASSETID,
      primaryKey: true
    },
    txid: {
      type: ColoredCoinsDataTypes.HASH,
      primaryKey: true
    },
    type: {
      type: DataTypes.ENUM('issuance', 'transfer'),
      allowNull: false
    }
  },
  {
    // classMethods: {
    //   associate: function (models) {
    //     AssetsTransactions.belongsTo(models.assets, { foreignKey: 'assetId', as: 'asset' })
    //     AssetsTransactions.belongsTo(models.transactions, { foreignKey: 'txid', as: 'transaction' })
    //   }
    // },
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