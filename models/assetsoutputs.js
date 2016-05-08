'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AssetsOutputs = sequelize.define('assetsoutputs', {
    assetId: {
      type: bitcoinDataTypes.assetIdType,
      primaryKey: true
    },
    output_id: {
      type: DataTypes.BIGINT,
      primaryKey: true
    },
    index_in_output: {
      type: DataTypes.INTEGER,
      primaryKey: true
    },
    amount: {
      type: DataTypes.BIGINT,
      allowNull: false
    },
    issueTxid: {
      type: bitcoinDataTypes.hashType,
      allowNull: false
    }
  },
  {
    // classMethods: {
    //   associate: function (models) {
    //     AssetsOutputs.belongsTo(models.outputs, { foreignKey: 'output_id', as: 'output' })
    //     AssetsOutputs.belongsTo(models.assets, { foreignKey: 'assetId', as: 'asset' })
    //   }
    // },
    indexes: [
      {
        fields: ['assetId']
      },
      {
        fields: ['output_id']
      },
      {
        fields: ['output_id', 'index_in_output']
      }
    ],
    timestamps: false
  })

  return AssetsOutputs
}