'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Assets = sequelize.define('assets', {
    assetId: {
      type: bitcoinDataTypes.assetIdType,
      primaryKey: true
    },
    divisibility: {
      type: DataTypes.INTEGER
    },
    lockStatus: {
      type: DataTypes.BOOLEAN
    },
    aggregationPolicy: {
      type: DataTypes.ENUM('aggregatable', 'hybrid', 'dispersed')
    }
  },
  {
    classMethods: {
      associate: function (models) {
        Assets.hasMany(models.assetsoutputs, { foreignKey: 'assetId', as: 'assetsoutputs', constraints: false })
        Assets.hasMany(models.assetstransactions, { foreignKey: 'assetId', as: 'assetstransactions', constraints: false })
        Assets.hasMany(models.assetsaddresses, { foreignKey: 'assetId', as: 'assetsaddresses', constraints: false })
      }
    },
    timestamps: false
  })

  return Assets
}