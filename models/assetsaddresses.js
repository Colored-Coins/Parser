'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AssetsAddresses = sequelize.define('assetsaddresses', {
    assetId: {
      type: ColoredCoinsDataTypes.ASSETID,
      primaryKey: true
    },
    address: {
      type: ColoredCoinsDataTypes.ADDRESS,
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