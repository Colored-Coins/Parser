'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AddressesTransactions = sequelize.define('addressestransactions', {
    txid: {
      type: DataTypes.STRING,
      primaryKey: true
    },
    address: {
      type: bitcoinDataTypes.addressType,
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