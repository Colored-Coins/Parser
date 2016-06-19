'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var Assets = sequelize.define('assets', {
    assetId: {
      type: ColoredCoinsDataTypes.ASSETID,
      primaryKey: true
    },
    divisibility: {
      type: 'SMALLINT',
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
    timestamps: false
  })

  return Assets
}