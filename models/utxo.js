module.exports = function (mongoose) {
  var Utxo = new mongoose.Schema({
    txid: { type: String, index: true},
    index: { type: Number, index: true},
    value: { type: Number, set: function (v) { return Math.round(v) }},
    hex: String,
    ASM: String,
    scriptPubKey: {
      asm: String,
      hex: String,
      reqSigs: Number,
      type: {type: String, index: true},
      addresses: {type: [String]}
    },
    assets: [{
      assetId: {type: String, index: true},
      amount: { type: Number, set: function (v) { return Math.round(v) }},
      issueTxid: String,
      divisibility: Number,
      lockStatus: Boolean
    }],
    used: {type: Boolean, index: true, default: false},
    blockheight: {type: Number, index: true},
    usedBlockheight: {type: Number, index: true},
    usedTxid: String,
    lastUsedTxid: String,
    blocktime: Date
  })

  var round = function (doc) {
    if (doc.value) doc.value = Math.round(doc.value)
    if (doc.assets) {
      doc.assets.forEach(function (asset) {
        if (asset.amount) asset.amount = Math.round(asset.amount)
      })
    }
  }

  Utxo.index(
    {
      txid: 1,
      index: 1,
      used: 1
    }
  )

  Utxo.index(
    {
      txid: 1,
      index: 1
    },
    {
      unique: true
    }
  )

  Utxo.post('find', function (docs) {
    if (docs) {
      docs.forEach(function (doc) {
        round(doc)
      })
    }
  })

  Utxo.post('findOne', function (doc) {
    if (doc) {
      round(doc)
    }
  })

  return Utxo
}
