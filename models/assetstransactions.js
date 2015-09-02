module.exports = function (mongoose) {
  var AssetsTransactionsSchema = new mongoose.Schema({
    assetId: { type: String, index: true },
    txid: String,
    type: {type: String, index: true},
    updated: {type: Date, index: true}
  })

  AssetsTransactionsSchema.pre('update', function () {
    this.updated = new Date()
  })

  AssetsTransactionsSchema.index({
      assetId: 1,
      txid: 1
    },
    {
      unique: true
    }
  )

  AssetsTransactionsSchema.index({
      assetId: 1,
      type: 1
    }
  )
  return AssetsTransactionsSchema
}
