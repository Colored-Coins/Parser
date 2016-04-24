'use strict'

var bitcoinDataTypes = require('./bitcoinDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AddressesOutputs = sequelize.define('addressesoutputs', {
    output_id: {
      type: DataTypes.BIGINT,
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
        AddressesOutputs.belongsTo(models.outputs, { foreignKey: 'output_id', as: 'outputs' })
      }
    },
    indexes: [
      {
        fields: ['output_id']
      },
      {
        fields: ['address']
      }
    ],
    timestamps: false
  })

  return AddressesOutputs
}