'use strict'

var ColoredCoinsDataTypes = require('./coloredCoinsDataTypes')

module.exports = function (sequelize, DataTypes) {
  var AddressesOutputs = sequelize.define('addressesoutputs', {
    output_id: {
      type: DataTypes.BIGINT,
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
        AddressesOutputs.belongsTo(models.outputs, { foreignKey: 'output_id', as: 'output' })
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