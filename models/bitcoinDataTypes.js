var Sequelize = require('sequelize')

module.exports = {
	hashType: Sequelize.STRING(64),	// even though this could be of type character (n), there is no performance advantage for this type. In fact, its the slowest char type.
	addressType: Sequelize.STRING(40),
	assetIdType: Sequelize.STRING(40)
}