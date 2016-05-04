'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Assets = sequelize.define('assets', {
    assetId: {
      type: bitcoinDataTypes.assetIdType,
      primaryKey: true
    },
    divisibility: {
      type: DataTypes.INTEGER,
      allowNull: false
    },
    lockStatus: {
      type: DataTypes.BOOLEAN,
      allowNull: false
    },
    aggregationPolicy: {
      type: DataTypes.ENUM('aggregatable', 'hybrid', 'dispersed'),
      allowNull: false
    }
  },
  {
    classMethods: {
      associate: function (models) {
        // Assets.hasMany(models.assetsoutputs, { foreignKey: 'assetId', as: 'assetsoutputs', constraints: false })
        // Assets.hasMany(models.assetstransactions, { foreignKey: 'assetId', as: 'assetstransactions', constraints: false })
        // Assets.hasMany(models.assetsaddresses, { foreignKey: 'assetId', as: 'assetsaddresses', constraints: false })
        Assets.belongsToMany(models.outputs, { as: 'outputs', through: models.assetsoutputs, foreignKey: 'assetId', otherKey: 'output_id' })
        Assets.belongsToMany(models.transactions, { as: 'transactions', through: models.assetstransactions, foreignKey: 'assetId', otherKey: 'txid' })
        Assets.belongsToMany(models.assetsaddresses, { as: 'addresses', through: models.assetsaddresses, foreignKey: 'assetId', otherKey: 'address', constraints: false }) // constraints = false because not every asset is neseccarily held by an address
      }
    },
    timestamps: false
  })

  return Assets
}