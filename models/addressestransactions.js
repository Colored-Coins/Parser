'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AddressesTransactions = sequelize.define('addressestransactions', {
    txid: {
      type: ColoredCoinsDataTypes.HASH,
      primaryKey: true
    },
    address: {
      type: ColoredCoinsDataTypes.ADDRESS,
      primaryKey: true
    }
  },
  {
    classMethods: {
      associate: function (models) {
        AddressesTransactions.belongsTo(models.transactions, { foreignKey: 'txid', as: 'transaction' })
      }
    },
    indexes: [
      {
        fields: ['txid']
      },
      {
        fields: ['address']
      }
    ],
    timestamps: false
  })

  return AddressesTransactions
}