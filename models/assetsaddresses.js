'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AssetsAddresses = sequelize.define('assetsaddresses', {
    assetId: {
      type: bitcoinDataTypes.assetIdType,
      primaryKey: true
    },
    address: {
      type: bitcoinDataTypes.addressType,
      primaryKey: true
    }
  },
  {
    // classMethods: {
    //   associate: function (models) {
    //     AssetsAddresses.belongsTo(models.assets, { foreignKey: 'assetId', as: 'asset' })
    //   },
    // },
    indexes: [
      {
        fields: ['assetId']
      },
      {
        fields: ['address']
      }
    ],
    timestamps: false
  })

  return AssetsAddresses
}